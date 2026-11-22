using NexusMQ.AspNetCore;

// Standalone NexusMQ broker. All the logic lives in NexusMQ.Core + NexusMQ.AspNetCore,
// so the same three lines can host the broker inside any other ASP.NET Core app.
// No appsettings reload: its file watcher covers the whole content root, which by default contains the data folder,
// so every journal write would wake it (measured: ~30% of a cluster node's CPU). NexusMQ reads its settings once at
// start-up anyway. Pass --hostBuilder:reloadConfigOnChange=true to turn reloading back on.
var builder = WebApplication.CreateBuilder(["--hostBuilder:reloadConfigOnChange=false", .. args]);

builder.Services.AddNexusMQBroker(builder.Configuration.GetSection("NexusMQ"));

var app = builder.Build();

app.MapNexusMQ("/api");
app.MapNexusMQDashboard("/", apiPrefix: "/api");
app.MapGet("/health", () => Results.Ok(new { status = "ok" }));

app.Run();

/// <summary>Exposed for integration tests (WebApplicationFactory).</summary>
public partial class Program;
