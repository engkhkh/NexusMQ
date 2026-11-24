using NexusMQ.AspNetCore;

// Standalone NexusMQ broker. All the logic lives in NexusMQ.Core + NexusMQ.AspNetCore,
// so the same three lines can host the broker inside any other ASP.NET Core app.
// No appsettings reload: its file watcher covers the whole content root, which by default contains the data folder,
// so every journal write would wake it (measured: ~30% of a cluster node's CPU). NexusMQ reads its settings once at
// start-up anyway. Pass --hostBuilder:reloadConfigOnChange=true to turn reloading back on.
// --windows-service: run under Windows' service manager (installed by setup\install-service.ps1). Services start in
// C:\Windows\System32, so settings (appsettings.json) are read from the program's own folder instead.
var asService = OperatingSystem.IsWindows() && args.Contains(NexusMQ.Server.WindowsService.Flag);
args = [.. args.Where(a => a != NexusMQ.Server.WindowsService.Flag)];
var builder = WebApplication.CreateBuilder(new WebApplicationOptions
{
    Args = ["--hostBuilder:reloadConfigOnChange=false", .. args],
    ContentRootPath = asService ? AppContext.BaseDirectory : null
});

builder.Services.AddNexusMQBroker(builder.Configuration.GetSection("NexusMQ"));

var app = builder.Build();

app.MapNexusMQ("/api");
app.MapNexusMQDashboard("/", apiPrefix: "/api");
app.MapGet("/health", () => Results.Ok(new { status = "ok" }));

var service = asService && OperatingSystem.IsWindows()
    && NexusMQ.Server.WindowsService.Attach(app, app.Services.GetRequiredService<ILoggerFactory>().CreateLogger("NexusMQ.Server"));
try
{
    app.Run();
}
finally
{
    if (service && OperatingSystem.IsWindows()) NexusMQ.Server.WindowsService.ReportStopped();
}

/// <summary>Exposed for integration tests (WebApplicationFactory).</summary>
public partial class Program;
