using System.Runtime.InteropServices;
using System.Runtime.Versioning;

namespace NexusMQ.Server;

/// <summary>
/// Runs the broker as a Windows service (<c>--windows-service</c>), talking to the Service Control Manager through advapi32
/// directly, the way ServiceBase does, so no extra package is needed.
/// <list type="bullet">
/// <item>A background thread calls <c>StartServiceCtrlDispatcher</c>, which blocks for the life of the service.</item>
/// <item>The service reports <c>RUNNING</c> once the web host has started, and <c>STOP_PENDING</c> → host shutdown → <c>STOPPED</c>
/// when Windows asks it to stop (Stop-Service, services.msc, or shutting the machine down).</item>
/// </list>
/// Install with <c>setup\install-service.ps1</c>.
/// </summary>
[SupportedOSPlatform("windows")]
internal static class WindowsService
{
    public const string Flag = "--windows-service";

    private const int OwnProcess = 0x10;
    private const int Stopped = 1, StartPending = 2, StopPending = 3, Running = 4;
    private const int ControlStop = 1, ControlInterrogate = 4, ControlShutdown = 5;
    private const int AcceptStop = 0x1, AcceptShutdown = 0x4;

    [StructLayout(LayoutKind.Sequential)]
    private struct ServiceStatus
    {
        public int ServiceType, CurrentState, ControlsAccepted, Win32ExitCode, ServiceSpecificExitCode, CheckPoint, WaitHint;
    }

    [StructLayout(LayoutKind.Sequential, CharSet = CharSet.Unicode)]
    private struct ServiceTableEntry
    {
        public string? ServiceName;
        public IntPtr ServiceProc;
    }

    [UnmanagedFunctionPointer(CallingConvention.Winapi)]
    private delegate void ServiceMainProc(int argc, IntPtr argv);

    [UnmanagedFunctionPointer(CallingConvention.Winapi)]
    private delegate int HandlerProc(int control, int eventType, IntPtr eventData, IntPtr context);

    [DllImport("advapi32.dll", SetLastError = true, CharSet = CharSet.Unicode, EntryPoint = "StartServiceCtrlDispatcherW")]
    private static extern bool StartServiceCtrlDispatcher([In] ServiceTableEntry[] table);

    [DllImport("advapi32.dll", SetLastError = true, CharSet = CharSet.Unicode, EntryPoint = "RegisterServiceCtrlHandlerExW")]
    private static extern IntPtr RegisterServiceCtrlHandlerEx(string serviceName, HandlerProc handler, IntPtr context);

    [DllImport("advapi32.dll", SetLastError = true)]
    private static extern bool SetServiceStatus(IntPtr handle, ref ServiceStatus status);

    // Kept in fields: the native side holds these delegates for the life of the process.
    private static ServiceMainProc? _main;
    private static HandlerProc? _handler;
    private static IntPtr _statusHandle;
    private static int _checkPoint;
    private static readonly ManualResetEventSlim Registered = new();

    /// <summary>
    /// Connect to the Service Control Manager. Returns false (and the broker runs as a normal console app) when the process
    /// was not started by the SCM.
    /// </summary>
    public static bool Attach(WebApplication app, ILogger log)
    {
        var lifetime = app.Lifetime;
        _handler = (control, _, _, _) =>
        {
            switch (control)
            {
                case ControlStop or ControlShutdown:
                    Report(StopPending, waitHint: 30_000);
                    lifetime.StopApplication();
                    return 0;
                case ControlInterrogate:
                    return 0;
                default:
                    return 120; // ERROR_CALL_NOT_IMPLEMENTED
            }
        };
        _main = (_, _) =>
        {
            _statusHandle = RegisterServiceCtrlHandlerEx("NexusMQ", _handler, IntPtr.Zero);
            Report(StartPending, waitHint: 30_000);
            Registered.Set();
            // RUNNING once Kestrel is listening; reported from ApplicationStarted below.
        };

        var connected = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
        var dispatcher = new Thread(() =>
        {
            var table = new[]
            {
                new ServiceTableEntry { ServiceName = "NexusMQ", ServiceProc = Marshal.GetFunctionPointerForDelegate(_main) },
                new ServiceTableEntry { ServiceName = null, ServiceProc = IntPtr.Zero }
            };
            // Blocks until the service has stopped. Fails at once (1063) when not started by the SCM.
            if (!StartServiceCtrlDispatcher(table)) connected.TrySetResult(false);
        }) { IsBackground = true, Name = "NexusMQ service dispatcher" };
        dispatcher.Start();

        // Either ServiceMain runs (started by the SCM) or the dispatcher fails right away.
        var registered = Task.Run(() => Registered.Wait(TimeSpan.FromSeconds(30)));
        Task.WhenAny(registered, connected.Task).Wait();
        if (!Registered.IsSet)
        {
            log.LogWarning("{Flag} was given, but this process was not started by Windows' service manager: running as a console app.", Flag);
            return false;
        }

        lifetime.ApplicationStarted.Register(() => Report(Running));
        lifetime.ApplicationStopping.Register(() => Report(StopPending, waitHint: 30_000));
        return true;
    }

    /// <summary>Tell Windows the service has stopped (after the web host has shut down).</summary>
    public static void ReportStopped(int exitCode = 0) => Report(Stopped, exitCode: exitCode);

    private static void Report(int state, int waitHint = 0, int exitCode = 0)
    {
        if (_statusHandle == IntPtr.Zero) return;
        var status = new ServiceStatus
        {
            ServiceType = OwnProcess,
            CurrentState = state,
            ControlsAccepted = state is Running ? AcceptStop | AcceptShutdown : 0,
            Win32ExitCode = exitCode,
            CheckPoint = state is StartPending or StopPending ? Interlocked.Increment(ref _checkPoint) : 0,
            WaitHint = waitHint
        };
        SetServiceStatus(_statusHandle, ref status);
    }
}
