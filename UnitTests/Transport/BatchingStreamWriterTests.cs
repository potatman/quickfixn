using System;
using System.IO;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging.Abstractions;
using NUnit.Framework;
using QuickFix;
using QuickFix.Transport;

namespace UnitTests.Transport;

[TestFixture]
public class BatchingStreamWriterTests
{
    [Test]
    public void PreservesOrderAndContent()
    {
        MemoryStream stream = new();
        BatchingStreamWriter writer = new(stream, NullLogger.Instance);
        StringBuilder expected = new();
        for (int i = 0; i < 10_000; i++)
        {
            string msg = $"8=FIX.4.4{Message.SOH}34={i}{Message.SOH}";
            expected.Append(msg);
            Assert.That(writer.Send(msg), Is.True);
        }

        writer.Close(TimeSpan.FromSeconds(10));

        Assert.That(CharEncoding.SelectedEncoding.GetString(stream.ToArray()), Is.EqualTo(expected.ToString()));
    }

    [Test]
    public void CloseFlushesQueuedMessages_ThenRejectsSends()
    {
        GatedStream stream = new();
        BatchingStreamWriter writer = new(stream, NullLogger.Instance);
        writer.Send("first");
        writer.Send("second");

        Task close = Task.Run(() => writer.Close(TimeSpan.FromSeconds(10)));
        stream.Gate.Set();
        Assert.That(close.Wait(TimeSpan.FromSeconds(10)), Is.True);

        Assert.That(stream.Written, Is.EqualTo("firstsecond"));
        Assert.That(writer.Send("too late"), Is.False);
    }

    [Test]
    public void WriteFailure_ClosesStream_AndRejectsSends()
    {
        FailingStream stream = new();
        BatchingStreamWriter writer = new(stream, NullLogger.Instance);
        writer.Send("boom");

        Assert.That(stream.Closed.Wait(TimeSpan.FromSeconds(10)), Is.True);
        Assert.That(SpinWait.SpinUntil(() => !writer.Send("after"), TimeSpan.FromSeconds(10)), Is.True);
    }

    [Test]
    public void SendBlocksWhileTooMuchIsPending()
    {
        GatedStream stream = new();
        BatchingStreamWriter writer = new(stream, NullLogger.Instance);
        string chunk = new('x', BatchingStreamWriter.MaxPendingBytes / 4);

        // The writer takes the first chunk and blocks in Write; the next ones fill the pending buffer
        Task sender = Task.Run(() =>
        {
            for (int i = 0; i < 10; i++)
                writer.Send(chunk);
        });

        Assert.That(sender.Wait(TimeSpan.FromMilliseconds(500)), Is.False, "sender should be held back");
        stream.Gate.Set();
        Assert.That(sender.Wait(TimeSpan.FromSeconds(10)), Is.True);
        writer.Close(TimeSpan.FromSeconds(10));
        Assert.That(stream.Written.Length, Is.EqualTo(10 * chunk.Length));
    }

    /// <summary>
    /// Holds every Write until Gate is set
    /// </summary>
    private sealed class GatedStream : MemoryStream
    {
        public ManualResetEventSlim Gate { get; } = new();
        public string Written
        {
            get { lock (this) return CharEncoding.SelectedEncoding.GetString(ToArray()); }
        }

        public override void Write(ReadOnlySpan<byte> buffer)
        {
            Gate.Wait();
            lock (this) base.Write(buffer);
        }
    }

    private sealed class FailingStream : MemoryStream
    {
        public ManualResetEventSlim Closed { get; } = new();
        public override void Write(ReadOnlySpan<byte> buffer) => throw new IOException("connection reset");
        public override void Close()
        {
            Closed.Set();
            base.Close();
        }
    }
}
