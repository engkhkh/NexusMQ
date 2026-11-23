# Using NexusMQ in your own project

NexusMQ ships as **3 NuGet packages** (already built in the `nupkg` folder):

| Package | Add it to | What it gives you |
|---|---|---|
| `NexusMQ.Client` | Any .NET 10 app that **sends or receives** messages (console, WPF, WinForms, MAUI, API, worker) | `NexusClient`, senders, receivers, processors, sessions, admin. No other dependencies. |
| `NexusMQ.AspNetCore` | ASP.NET Core / Worker Service apps | `AddNexusMQClient`, `AddNexusMQHandler<T>` (background consumers with DI), and optionally `AddNexusMQBroker` + `MapNexusMQ` to **host the broker inside your app**. Pulls in Client and Core. |
| `NexusMQ.Core` | Only if you want the raw engine in-process with no HTTP | `Broker` class. |
| `NexusMQ.Cli` | Your machine (a .NET tool, not a library) | The `nexusmq` command: `dotnet tool install -g NexusMQ.Cli --add-source C:\projects-claude\NexusMQ\nupkg` |

You do **not** copy DLLs by hand. Packages are the clean way: Visual Studio restores them, and an updated version is a single version bump.

---

## Step 1: add the local package feed (one time)

**In Visual Studio:**
1. **Tools → Options → NuGet Package Manager → Package Sources**.
2. Click **+**. Set the Name to `NexusMQ` and the Source to `C:\projects-claude\NexusMQ\nupkg`, then click **Update** and **OK**.

**Or** put a `nuget.config` next to your own solution's `.sln` file:
```xml
<?xml version="1.0" encoding="utf-8"?>
<configuration>
  <packageSources>
    <add key="nuget.org" value="https://api.nuget.org/v3/index.json" />
    <add key="NexusMQ" value="C:\projects-claude\NexusMQ\nupkg" />
  </packageSources>
</configuration>
```

## Step 2: install the package

In Visual Studio, right-click your project → **Manage NuGet Packages** → choose the **NexusMQ** source → **Browse** → install **NexusMQ.AspNetCore** (web or worker apps) or **NexusMQ.Client** (any other app).

Or from the command line:
```powershell
dotnet add package NexusMQ.AspNetCore --source C:\projects-claude\NexusMQ\nupkg
```

---

## Option A: separate broker server (recommended for several services)

Run `NexusMQ.Server` once (F5 in this solution, or `dotnet run --project src/NexusMQ.Server`). Every app connects to it at `http://localhost:5680`.

**ASP.NET Core / Worker app** (`Program.cs`):
```csharp
using NexusMQ.AspNetCore;
using NexusMQ.Client;

builder.Services.AddNexusMQClient("http://localhost:5680");               // inject NexusClient anywhere
builder.Services.AddNexusMQHandler<OrderHandler, OrderPlaced>("orders"); // background consumer

// in a controller / endpoint
app.MapPost("/orders", async (OrderPlaced order, NexusClient nexus) =>
{
    await nexus.CreateSender("orders").SendMessageAsync(NexusMessage.FromJson(order));
    return Results.Accepted();
});

public record OrderPlaced(string OrderId, decimal Total);

public class OrderHandler(MyDbContext db) : INexusMessageHandler<OrderPlaced>   // DI works (scoped per message)
{
    public async Task HandleAsync(OrderPlaced order, ProcessMessageEventArgs args)
    {
        // ... your logic. Return = complete. Throw = retry; after MaxDeliveryCount → dead-letter queue.
    }
}
```

**Console / WPF / WinForms app** (package `NexusMQ.Client` only):
```csharp
await using var client = new NexusClient("http://localhost:5680");
await client.Admin.CreateQueueAsync("orders");                          // idempotent
await client.CreateSender("orders").SendMessageAsync(new NexusMessage("hello"));

var receiver = client.CreateReceiver("orders");
var msg = await receiver.ReceiveMessageAsync(TimeSpan.FromSeconds(10));
if (msg is not null) await receiver.CompleteMessageAsync(msg);
```

## Option B: broker embedded in your app (single service or modular monolith)

No separate server to run. Add these lines to your ASP.NET Core `Program.cs`:
```csharp
builder.Services.AddNexusMQBroker(builder.Configuration.GetSection("NexusMQ")); // storage in ./data by default
builder.Services.AddNexusMQClient("http://localhost:5000");                       // your app's own URL
builder.Services.AddNexusMQHandler<OrderHandler, OrderPlaced>("orders");

var app = builder.Build();

var broker = app.Services.GetRequiredService<NexusMQ.Core.Broker>();     // create the topology at startup
broker.CreateQueue("orders");

app.MapNexusMQ("/api");                        // broker HTTP API
app.MapNexusMQDashboard("/nexusmq", "/api");   // dashboard
```
`samples/Sample.WebApp` is a complete working example of this option. It uses a typed handler, a topic subscription with a SQL filter, and an ordered session handler.

> **Embedded broker and the data folder:** ASP.NET watches your app's content root (to reload appsettings.json), subfolders included.
> If NexusMQ's data folder is inside it (the default `"data"` is), every message write wakes that watcher and costs a lot of CPU, and NexusMQ
> logs a warning at start-up. Fix it with either:
> - an absolute data folder outside the app, e.g. `"NexusMQ": { "DataDirectory": "C:\\NexusMQData" }`, or
> - turning reloading off: `var builder = WebApplication.CreateBuilder(["--hostBuilder:reloadConfigOnChange=false", .. args]);`

## Option C: project reference (while you're still developing NexusMQ itself)

If your app's solution and NexusMQ are both on your machine: right-click your solution → **Add → Existing Project** → `C:\projects-claude\NexusMQ\src\NexusMQ.Client\NexusMQ.Client.csproj` (and/or `NexusMQ.AspNetCore`). Then right-click your project → **Add → Project Reference**. Changes to NexusMQ show up immediately, with no repacking.

## Option D: plain DLL reference (not recommended)

Build NexusMQ in Release. In your project use **Add → Project Reference → Browse** and pick `src\NexusMQ.Client\bin\Release\net10.0\NexusMQ.Client.dll`. This works for `NexusMQ.Client` because it has no dependencies. For `NexusMQ.AspNetCore` you'd also have to reference `NexusMQ.Core.dll` and `NexusMQ.Client.dll` yourself, which is why packages are better.

---

## Updating after you change NexusMQ
1. Bump `<Version>` in `Directory.Build.props` (for example 1.8.0 → 1.9.0).
2. Run `pack.ps1` from the NexusMQ folder. It writes new `.nupkg` files into `nupkg`.
3. In your project, update the package in **Manage NuGet Packages → Updates**.

## Feature cheat sheet
```csharp
// Topic + SQL-filtered subscription (Azure Service Bus style)
await client.Admin.CreateTopicAsync("events");
await client.Admin.CreateSubscriptionAsync("events", "big-eu",
    [new NexusSubscriptionRule { SqlFilter = "region = 'eu' AND amount > 100" }]);

// Exchange routing (RabbitMQ style)
await client.Admin.CreateExchangeAsync("logs", NexusExchangeType.Topic);
await client.Admin.BindAsync("logs", "errors", "*.error");
await client.CreateSender("exchanges/logs").PublishAsync("billing.error", new NexusMessage("boom"));

// Ordered processing per key (sessions)
await client.Admin.CreateQueueAsync("shipments", new NexusQueueOptions { RequiresSession = true });
await client.CreateSender("shipments").SendMessageAsync(new NexusMessage("packed") { SessionId = "order-42" });
var processor = client.CreateSessionProcessor("shipments", new NexusSessionProcessorOptions { MaxConcurrentSessions = 8 });

// Auto-forwarding and a shared failures queue
await client.Admin.CreateQueueAsync("all-failures");
await client.Admin.CreateQueueAsync("inbox", new NexusQueueOptions { ForwardTo = "orders", ForwardDeadLetteredMessagesTo = "all-failures" });

// Rule action: tag the copy that a subscription receives
await client.Admin.CreateSubscriptionAsync("events", "eu",
    [new NexusSubscriptionRule { SqlFilter = "region = 'eu'", SqlAction = "SET priority = 'high'" }]);

// Exactly one copy even when a send is retried (timeouts, failover): duplicate detection on the queue.
// Every NexusMessage gets a MessageId when created; set your own (e.g. the order number) to make re-sent business events idempotent.
await client.Admin.CreateQueueAsync("payments", new NexusQueueOptions { DuplicateDetectionWindow = TimeSpan.FromMinutes(10) });
await client.CreateSender("payments").SendMessageAsync(new NexusMessage(json) { MessageId = $"payment-{orderId}" });

// Delayed delivery, TTL, dead letters
await sender.ScheduleMessageAsync(msg, DateTimeOffset.UtcNow.AddMinutes(5));
msg.TimeToLive = TimeSpan.FromHours(1);
var dlq = client.CreateReceiver("orders/$deadletter");
await client.Admin.ResubmitDeadLettersAsync("orders");
```

## Push vs long-polling
Processors use WebSocket push by default. Tune them with:
```csharp
builder.Services.AddNexusMQHandler<OrderHandler, OrderPlaced>("orders", o =>
{
    o.MaxConcurrentCalls = 8;   // handlers running in parallel
    o.PrefetchCount = 32;       // messages the server may push ahead (more = faster, but they are locked while buffered)
    // o.Transport = NexusTransport.LongPolling;  // if a proxy blocks WebSockets
});
```
Message bodies travel as raw bytes in both directions (`NexusClientOptions.BinaryBodies`, on by default): sends go as a binary batch and
push delivers binary frames. Large messages are several times faster (16 KB bodies: about 3–4× to send, about 5× to consume). It works with
older servers too: a server before 1.12 refuses the binary send once and the client switches to JSON for it.

## Monitoring
- Prometheus: scrape `http://<server>/api/metrics`.
- OpenTelemetry: `builder.Services.AddOpenTelemetry().WithMetrics(m => m.AddMeter("NexusMQ"))` in the app that hosts the broker.

## Security: give each app its own key
By default the API is open, which is fine on your own machine. Before exposing it on a network, add keys to the server's `appsettings.json`
(see the README's "Access keys" section) and give each app only what it needs:
```csharp
// Web API that only publishes orders
builder.Services.AddNexusMQClient("http://nexusmq:5680", builder.Configuration["NexusMQ:SendKey"]);
// Worker that only consumes them
builder.Services.AddNexusMQClient("http://nexusmq:5680", builder.Configuration["NexusMQ:ListenKey"]);
```
Create keys without touching config: `nexusmq keys create orders-api --rights Send --entity orders` (or the dashboard's
**Access keys** panel). To change a secret with no downtime, run `nexusmq keys rotate orders-api --grace 600`, roll the new secret out to the app,
and the old one stops working after 10 minutes.

If a key lacks a right, calls fail with `NexusClientException` code `Forbidden` (HTTP 403). Processors report this through `ProcessErrorAsync`.

## Rate limits and HTTPS in your app
- If you give an app's key a rate limit, the client waits and retries automatically when it's exceeded (default 3 retries).
  Change this with `new NexusClientOptions { MaxRateLimitRetries = 5 }`.
- For HTTPS, just use an `https://` URL. Push switches to `wss://` automatically. See `docs/TLS.md`.
  `NexusClientOptions.AllowUntrustedCertificates = true` is for self-signed **development** certificates only.

## Two servers for high availability
Give your app both server URLs, comma-separated, and it fails over by itself:
```csharp
builder.Services.AddNexusMQClient("http://mq-a:5680,http://mq-b:5680", key);
```
Setup and the important caveats (async replication, at-least-once, split brain) are in `docs/HA.md`.

## Three servers (Raft cluster)
For the strongest guarantees, run 3 servers in cluster mode and give your app all three URLs:
```csharp
builder.Services.AddNexusMQClient("http://mq-1:5680,http://mq-2:5680,http://mq-3:5680", key);
```
A send that returned OK is stored on a majority, so losing any one server loses nothing. See `docs/CLUSTER.md`.

## Running it in Docker
From the NexusMQ folder: `docker compose up -d --build`. Your apps then use `http://localhost:5680` (or `http://nexusmq:5680` from other
containers on the same compose network), and Grafana at http://localhost:3000 shows the NexusMQ dashboard.
