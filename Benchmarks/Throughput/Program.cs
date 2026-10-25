using System.Diagnostics;
using System.Net;
using System.Net.Sockets;
using QuickFix;
using QuickFix.Fields;
using QuickFix.Logger;
using QuickFix.Store;
using QuickFix.Transport;
using MDIR = QuickFix.FIX50SP2.MarketDataIncrementalRefresh;

namespace Throughput;

/// <summary>
/// Measures inbound/outbound FIX message throughput over loopback.
///
///   recv  raw TCP server floods pre-encoded messages into a QuickFIXn initiator (isolates the receive path)
///   e2e   QuickFIXn acceptor sends to a QuickFIXn initiator (send + receive paths)
///
/// Usage: Throughput [recv|e2e] [--messages N] [--store memory|file] [--log null|file] [--validate Y|N]
/// </summary>
public static class Program
{
    private const string ServerCompId = "SERVER";
    private const string ClientCompId = "CLIENT";

    public static int Main(string[] args)
    {
        Options opt = Options.Parse(args);
        Console.WriteLine($"mode={opt.Mode} messages={opt.Messages:N0} store={opt.Store} log={opt.Log} validate={opt.Validate}");

        string workDir = Path.Combine(Path.GetTempPath(), "qfn-throughput-" + Environment.ProcessId);
        Directory.CreateDirectory(workDir);
        try
        {
            if (opt.Mode == "stages")
            {
                RunStages(opt);
                return 0;
            }
            Result result = opt.Mode == "e2e" ? RunEndToEnd(opt, workDir) : RunReceive(opt, workDir);
            result.Print(opt.Messages);
            return 0;
        }
        finally
        {
            try { Directory.Delete(workDir, recursive: true); } catch (IOException) { }
        }
    }

    #region recv mode

    private static Result RunReceive(Options opt, string workDir)
    {
        TcpListener listener = new(IPAddress.Loopback, 0);
        listener.Start();
        int port = ((IPEndPoint)listener.LocalEndpoint).Port;

        CountingApp clientApp = new(opt.Messages);
        SessionSettings clientSettings = ClientSettings(opt, workDir, port);
        using SocketInitiator initiator = new(clientApp, StoreFactory(opt, clientSettings), clientSettings, LogFactory(opt, clientSettings));
        initiator.Start();

        using TcpClient server = listener.AcceptTcpClient();
        server.NoDelay = true;
        NetworkStream stream = server.GetStream();

        // Wait for the client's Logon, then answer it
        Parser parser = new();
        byte[] readBuf = new byte[4096];
        string? logon = null;
        while (logon is null)
        {
            int n = stream.Read(readBuf, 0, readBuf.Length);
            if (n == 0) throw new InvalidOperationException("client closed before logon");
            parser.AddToStream(readBuf.AsSpan(0, n));
            while (logon is null && parser.ReadFixMessage(out string msg))
                if (Message.GetMsgType(msg) == MsgType.LOGON) logon = msg;
        }

        Message logonReply = new();
        logonReply.Header.SetField(new MsgType(MsgType.LOGON));
        logonReply.SetField(new EncryptMethod(0));
        logonReply.SetField(new HeartBtInt(30));
        logonReply.SetField(new ResetSeqNumFlag(true));
        logonReply.SetField(new DefaultApplVerID(ApplVerID.FIX50SP2));
        WriteMessage(stream, logonReply, 1);
        if (!clientApp.LoggedOn.Wait(TimeSpan.FromSeconds(10)))
            throw new TimeoutException("client did not log on");

        // Keep draining whatever the client sends (heartbeats etc.)
        Thread drain = new(() =>
        {
            try { while (stream.Read(readBuf, 0, readBuf.Length) > 0) { } } catch (IOException) { } catch (ObjectDisposedException) { }
        }) { IsBackground = true };
        drain.Start();

        Console.Write("pre-encoding messages... ");
        byte[] payload = EncodeAll(opt.Messages, firstSeqNum: 2);
        Console.WriteLine($"{payload.Length / 1048576.0:N1} MB");

        Measurement m = Measurement.Start();
        for (int off = 0; off < payload.Length; off += 64 * 1024)
            stream.Write(payload, off, Math.Min(64 * 1024, payload.Length - off));
        bool done = clientApp.AllReceived.Wait(TimeSpan.FromMinutes(5));
        Result result = m.Stop(payload.Length, clientApp.Received, done);

        initiator.Stop(force: true);
        listener.Stop();
        return result;
    }

    private static void WriteMessage(Stream stream, Message msg, int seqNum)
    {
        SetHeader(msg, seqNum);
        byte[] bytes = CharEncoding.SelectedEncoding.GetBytes(msg.ConstructString());
        stream.Write(bytes, 0, bytes.Length);
    }

    private static byte[] EncodeAll(int count, int firstSeqNum)
    {
        using MemoryStream ms = new();
        for (int i = 0; i < count; i++)
        {
            MDIR msg = CreateMarketData(i);
            SetHeader(msg, firstSeqNum + i);
            byte[] bytes = CharEncoding.SelectedEncoding.GetBytes(msg.ConstructString());
            ms.Write(bytes, 0, bytes.Length);
        }
        return ms.ToArray();
    }

    private static void SetHeader(Message msg, int seqNum)
    {
        msg.Header.SetField(new BeginString(QuickFix.Values.BeginString_FIXT11));
        msg.Header.SetField(new SenderCompID(ServerCompId));
        msg.Header.SetField(new TargetCompID(ClientCompId));
        msg.Header.SetField(new MsgSeqNum((ulong)seqNum));
        msg.Header.SetField(new SendingTime(DateTime.UtcNow));
    }

    #endregion

    #region stages mode

    /// <summary>
    /// Times each stage of the inbound pipeline in isolation, single-threaded
    /// </summary>
    private static void RunStages(Options opt)
    {
        int n = opt.Messages;
        byte[] payload = EncodeAll(n, firstSeqNum: 2);
        string spec = Path.Combine(AppContext.BaseDirectory, "spec");
        QuickFix.DataDictionary.DataDictionary transportDd = new(Path.Combine(spec, "FIXT11.xml"));
        QuickFix.DataDictionary.DataDictionary appDd = new(Path.Combine(spec, "FIX50SP2.xml"));
        DefaultMessageFactory factory = new();
        ApplVerID applVerId = new(ApplVerID.FIX50SP2);

        string[] msgs = new string[n];
        Stage("frame (Parser)", n, () =>
        {
            Parser parser = new();
            int count = 0;
            for (int off = 0; off < payload.Length; off += 4096)
            {
                parser.AddToStream(payload.AsSpan(off, Math.Min(4096, payload.Length - off)));
                while (parser.ReadFixMessage(out string s))
                    msgs[count++] = s;
            }
        });

        Message[] built = new Message[1024]; // small working set: keeping n messages alive would measure the GC, not the code
        Stage("build (Create+FromString)", n, () =>
        {
            for (int i = 0; i < n; i++)
            {
                Message m = factory.Create(QuickFix.Values.BeginString_FIXT11, applVerId, MsgType.MARKETDATAINCREMENTALREFRESH);
                m.FromString(msgs[i], true, transportDd, appDd, factory);
                built[i & 1023] = m;
            }
        });

        Stage("validate (DataDictionary)", n, () =>
        {
            for (int i = 0; i < n; i++)
                QuickFix.DataDictionary.DataDictionary.Validate(built[i & 1023], transportDd, appDd,
                    QuickFix.Values.BeginString_FIXT11, MsgType.MARKETDATAINCREMENTALREFRESH);
        });

        Stage("serialize (ConstructString)", n, () =>
        {
            for (int i = 0; i < n; i++)
                built[i & 1023].ConstructString();
        });
    }

    private static void Stage(string name, int n, Action action)
    {
        action(); // warm up
        GC.Collect();
        long alloc = GC.GetAllocatedBytesForCurrentThread();
        Stopwatch sw = Stopwatch.StartNew();
        action();
        sw.Stop();
        alloc = GC.GetAllocatedBytesForCurrentThread() - alloc;
        Console.WriteLine($"{name,-30} {sw.Elapsed.TotalNanoseconds / n,8:N0} ns/msg  {alloc / (double)n,8:N0} B/msg");
    }

    #endregion

    #region e2e mode

    private static Result RunEndToEnd(Options opt, string workDir)
    {
        int port = FreePort();

        CountingApp serverApp = new(0);
        SessionSettings serverSettings = ServerSettings(opt, workDir, port);
        using ThreadedSocketAcceptor acceptor = new(serverApp, StoreFactory(opt, serverSettings), serverSettings, LogFactory(opt, serverSettings));
        acceptor.Start();

        CountingApp clientApp = new(opt.Messages);
        SessionSettings clientSettings = ClientSettings(opt, workDir, port);
        using SocketInitiator initiator = new(clientApp, StoreFactory(opt, clientSettings), clientSettings, LogFactory(opt, clientSettings));
        initiator.Start();

        if (!serverApp.LoggedOn.Wait(TimeSpan.FromSeconds(10)) || !clientApp.LoggedOn.Wait(TimeSpan.FromSeconds(10)))
            throw new TimeoutException("sessions did not log on");

        Session session = Session.LookupSession(new SessionID(QuickFix.Values.BeginString_FIXT11, ServerCompId, ClientCompId))!;

        Measurement m = Measurement.Start();
        for (int i = 0; i < opt.Messages; i++)
            session.Send(CreateMarketData(i));
        TimeSpan sendElapsed = m.Elapsed;
        bool done = clientApp.AllReceived.Wait(TimeSpan.FromMinutes(5));
        Result result = m.Stop(0, clientApp.Received, done) with { SendElapsed = sendElapsed };

        initiator.Stop(force: true);
        acceptor.Stop(force: true);
        return result;
    }

    private static int FreePort()
    {
        TcpListener l = new(IPAddress.Loopback, 0);
        l.Start();
        int port = ((IPEndPoint)l.LocalEndpoint).Port;
        l.Stop();
        return port;
    }

    #endregion

    #region settings

    private static SessionSettings ClientSettings(Options opt, string workDir, int port) => Settings(opt, workDir, $"""
        [SESSION]
        ConnectionType=initiator
        SenderCompID={ClientCompId}
        TargetCompID={ServerCompId}
        SocketConnectHost=127.0.0.1
        SocketConnectPort={port}
        ReconnectInterval=1
        """);

    private static SessionSettings ServerSettings(Options opt, string workDir, int port) => Settings(opt, workDir, $"""
        [SESSION]
        ConnectionType=acceptor
        SenderCompID={ServerCompId}
        TargetCompID={ClientCompId}
        SocketAcceptHost=127.0.0.1
        SocketAcceptPort={port}
        """);

    private static SessionSettings Settings(Options opt, string workDir, string session)
    {
        string spec = Path.Combine(AppContext.BaseDirectory, "spec");
        string side = session.Contains("initiator") ? "client" : "server";
        string cfg = $"""
            [DEFAULT]
            BeginString=FIXT.1.1
            DefaultApplVerID=FIX.5.0SP2
            NonStopSession=Y
            HeartBtInt=30
            ResetOnLogon=Y
            UseDataDictionary={opt.Validate}
            TransportDataDictionary={Path.Combine(spec, "FIXT11.xml")}
            AppDataDictionary={Path.Combine(spec, "FIX50SP2.xml")}
            FileStorePath={Path.Combine(workDir, side, "store")}
            FileLogPath={Path.Combine(workDir, side, "log")}

            {session}
            """;
        return new SessionSettings(new StringReader(cfg));
    }

    private static IMessageStoreFactory StoreFactory(Options opt, SessionSettings s) =>
        opt.Store == "file" ? new FileStoreFactory(s) : new MemoryStoreFactory();

    private static ILogFactory LogFactory(Options opt, SessionSettings s) =>
        opt.Log == "file" ? new FileLogFactory(s) : new NullLogFactory();

    #endregion

    /// <summary>
    /// Same shape of message the MKTS-20 fake server sent
    /// </summary>
    private static MDIR CreateMarketData(int i)
    {
        MDIR msg = new();
        msg.SetField(new MDReqID("REQ" + (i % 1000)));

        MDIR.NoMDEntriesGroup group = new()
        {
            MDUpdateAction = new MDUpdateAction(MDUpdateAction.CHANGE),
            MDEntryType = new MDEntryType(MDEntryType.BID),
            MDEntryID = new MDEntryID("US36962G4B75_BID_1"),
            SecurityID = new SecurityID("US36962G4B75"),
            SecurityIDSource = new SecurityIDSource(SecurityIDSource.ISIN_NUMBER),
            MDEntryPx = new MDEntryPx(100.4M + (i % 100) / 100M),
            MDEntrySize = new MDEntrySize(260000),
            MDEntryDate = new MDEntryDate(new DateOnly(2019, 11, 8)),
            MDEntryTime = new MDEntryTime(new TimeOnly(15, 37, 16, 442)),
            MinQty = new MinQty(1000),
            MarketDepth = new MarketDepth(1),
            PriceType = new PriceType(PriceType.PERCENTAGE),
            Spread = new Spread(100.16M),
        };
        msg.AddGroup(group);
        return msg;
    }
}

internal sealed class CountingApp : IApplication
{
    private readonly int _expected;
    private int _received;

    public ManualResetEventSlim LoggedOn { get; } = new();
    public ManualResetEventSlim AllReceived { get; } = new();
    public int Received => Volatile.Read(ref _received);

    public CountingApp(int expected) => _expected = expected;

    public void FromApp(Message message, SessionID sessionId)
    {
        if (Interlocked.Increment(ref _received) == _expected)
            AllReceived.Set();
    }

    public void OnLogon(SessionID sessionId) => LoggedOn.Set();
    public void ToAdmin(Message message, SessionID sessionId) { }
    public void FromAdmin(Message message, SessionID sessionId) { }
    public void ToApp(Message message, SessionID sessionId) { }
    public void OnCreate(SessionID sessionId) { }
    public void OnLogout(SessionID sessionId) { }
}

internal sealed record Options(string Mode, int Messages, string Store, string Log, string Validate)
{
    public static Options Parse(string[] args)
    {
        Options o = new("recv", 200_000, "memory", "null", "Y");
        for (int i = 0; i < args.Length; i++)
        {
            o = args[i] switch
            {
                "recv" or "e2e" or "stages" => o with { Mode = args[i] },
                "--messages" => o with { Messages = int.Parse(args[++i]) },
                "--store" => o with { Store = args[++i] },
                "--log" => o with { Log = args[++i] },
                "--validate" => o with { Validate = args[++i] },
                _ => throw new ArgumentException("unknown argument: " + args[i]),
            };
        }
        return o;
    }
}

internal readonly struct Measurement
{
    private readonly Stopwatch _sw;
    private readonly long _allocated;
    private readonly TimeSpan _cpu;
    private readonly int _gc0, _gc1, _gc2;

    private Measurement(Stopwatch sw)
    {
        _sw = sw;
        _allocated = GC.GetTotalAllocatedBytes(precise: true);
        _cpu = Process.GetCurrentProcess().TotalProcessorTime;
        _gc0 = GC.CollectionCount(0);
        _gc1 = GC.CollectionCount(1);
        _gc2 = GC.CollectionCount(2);
    }

    public static Measurement Start()
    {
        GC.Collect();
        GC.WaitForPendingFinalizers();
        return new Measurement(Stopwatch.StartNew());
    }

    public TimeSpan Elapsed => _sw.Elapsed;

    public Result Stop(long bytes, int received, bool completed) => new(
        _sw.Elapsed,
        null,
        bytes,
        received,
        completed,
        GC.GetTotalAllocatedBytes(precise: true) - _allocated,
        Process.GetCurrentProcess().TotalProcessorTime - _cpu,
        GC.CollectionCount(0) - _gc0,
        GC.CollectionCount(1) - _gc1,
        GC.CollectionCount(2) - _gc2);
}

internal sealed record Result(
    TimeSpan Elapsed, TimeSpan? SendElapsed, long Bytes, int Received, bool Completed,
    long Allocated, TimeSpan Cpu, int Gc0, int Gc1, int Gc2)
{
    public void Print(int expected)
    {
        if (!Completed)
            Console.WriteLine($"!! TIMED OUT: received {Received:N0} of {expected:N0}");
        double sec = Elapsed.TotalSeconds;
        Console.WriteLine($"elapsed      {sec:N2} s" + (SendElapsed is { } s ? $"  (send loop {s.TotalSeconds:N2} s)" : ""));
        Console.WriteLine($"throughput   {Received / sec:N0} msg/s" + (Bytes > 0 ? $"  {Bytes / 1048576.0 / sec:N1} MB/s" : ""));
        Console.WriteLine($"allocated    {Allocated / (double)Received:N0} B/msg  ({Allocated / 1048576.0:N0} MB total)");
        Console.WriteLine($"cpu          {Cpu.TotalMilliseconds * 1000 / Received:N1} us/msg");
        Console.WriteLine($"gc           gen0={Gc0} gen1={Gc1} gen2={Gc2}");
    }
}
