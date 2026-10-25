using System;
using System.Buffers;
using System.IO;
using System.Threading;
using Microsoft.Extensions.Logging;

namespace QuickFix.Transport;

/// <summary>
/// Coalesces outgoing messages. Send() encodes into a pending buffer and returns;
/// a dedicated thread writes everything pending with a single Stream.Write.
/// Under load this turns one socket write per message into one per batch.
///
/// - Order is preserved (one buffer, one writer).
/// - Backpressure: Send() blocks while more than MaxPendingBytes are waiting,
///   just like a synchronous write blocks when the peer stops reading.
/// - A write failure closes the stream, so the connection's reader sees the error
///   and disconnects the session through its normal error handling.
/// </summary>
internal sealed class BatchingStreamWriter : IDisposable
{
    internal const int MaxPendingBytes = 1024 * 1024;

    private readonly Stream _stream;
    private readonly ILogger _log;
    private readonly object _lock = new();
    private readonly Thread _thread;

    private ArrayBufferWriter<byte> _pending = new(4096);
    private ArrayBufferWriter<byte> _writing = new(4096);
    private bool _closed;

    public BatchingStreamWriter(Stream stream, ILogger log)
    {
        _stream = stream;
        _log = log;
        _thread = new Thread(Run) { IsBackground = true, Name = "QuickFIX/n send" };
        _thread.Start();
    }

    /// <summary>
    /// Queue a message for sending
    /// </summary>
    /// <returns>false if the writer is closed (disconnected or a previous write failed)</returns>
    public bool Send(string data)
    {
        int byteCount = CharEncoding.SelectedEncoding.GetByteCount(data);
        lock (_lock)
        {
            while (!_closed && _pending.WrittenCount > 0 && _pending.WrittenCount + byteCount > MaxPendingBytes)
                Monitor.Wait(_lock);
            if (_closed)
                return false;

            bool wasEmpty = _pending.WrittenCount == 0;
            int written = CharEncoding.SelectedEncoding.GetBytes(data, _pending.GetSpan(byteCount));
            _pending.Advance(written);
            if (wasEmpty)
                Monitor.PulseAll(_lock); // the writer only waits when there is nothing pending
        }
        return true;
    }

    private void Run()
    {
        while (true)
        {
            lock (_lock)
            {
                while (_pending.WrittenCount == 0 && !_closed)
                    Monitor.Wait(_lock);
                if (_pending.WrittenCount == 0)
                    return; // closed and fully flushed

                (_pending, _writing) = (_writing, _pending);
                Monitor.PulseAll(_lock); // senders blocked on backpressure can continue
            }

            try
            {
                _stream.Write(_writing.WrittenSpan);
            }
            catch (Exception e)
            {
                lock (_lock)
                {
                    _closed = true;
                    Monitor.PulseAll(_lock);
                }
                _log.Log(LogLevel.Error, e, "Error writing to socket; closing connection");
                try { _stream.Close(); } catch (Exception) { /* already broken */ }
                return;
            }

            _writing.ResetWrittenCount();
        }
    }

    /// <summary>
    /// Stop accepting messages, and wait (up to the timeout) for everything already queued to be written
    /// </summary>
    public void Close(TimeSpan flushTimeout)
    {
        lock (_lock)
        {
            _closed = true;
            Monitor.PulseAll(_lock);
        }
        if (Thread.CurrentThread != _thread)
            _thread.Join(flushTimeout);
    }

    public void Dispose() => Close(TimeSpan.FromSeconds(2));
}
