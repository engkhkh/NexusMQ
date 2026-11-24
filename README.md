# NexusMQ

[![CI](../../actions/workflows/ci.yml/badge.svg)](../../actions/workflows/ci.yml)

A self-hosted message broker in C# / .NET 10 that combines **RabbitMQ routing** (direct, fanout, topic and headers exchanges) with **Azure Service Bus semantics** (queues, topics and subscriptions, peek-lock, dead-letter queues, TTL, scheduled and deferred messages, duplicate detection).

See [`docs/PLAN.md`](docs/PLAN.md) for the architecture and roadmap, and
**[`docs/USING-IN-YOUR-PROJECT.md`](docs/USING-IN-YOUR-PROJECT.md) for how to add NexusMQ to your own apps** (NuGet packages in `nupkg/`).

CI/CD: GitHub Actions build and test on Linux and Windows, pack, and smoke-test the Docker image on every push. A version tag publishes
packages (GitHub Packages and nuget.org), a GitHub Release and a multi-arch image on ghcr.io. See [`docs/RELEASING.md`](docs/RELEASING.md).

New in 1.8: **add and remove cluster servers at runtime** (`nexusmq cluster add/remove`, with no restart and no downtime), and **committed-only
delivery**: in a cluster, consumers only ever receive messages a majority has stored.

In 1.7: **Raft cluster mode**. 3 or 5 servers elect a leader, and a write is confirmed only once a majority has it. There is no data loss
when a server crashes and no split brain. See [`docs/CLUSTER.md`](docs/CLUSTER.md) and `docker-compose.cluster.yml`.

In 1.6: **high availability**. Run a primary plus a standby that keeps a live copy of everything (messages,
dead letters, session state, runtime keys) and takes over automatically or on command. Clients given both URLs fail over by themselves.
See [`docs/HA.md`](docs/HA.md) and `docker-compose.ha.yml`.

In 1.5: an **audit log** (who did what, and every denied request), **rate limits per key** (HTTP 429 with automatic
client retries), a **message browser** (view, resend or delete single messages from the dashboard, CLI or API), and **HTTPS**
(see [`docs/TLS.md`](docs/TLS.md)).

In 1.4: **runtime key management** (create, rotate with a grace period, and delete keys from the API, CLI or dashboard,
with no restart; only hashes are stored), the **`nexusmq` command-line tool**, and **live-server integration tests**.

In 1.3: **access keys** with Send / Listen / Manage / Monitor rights per queue or topic (like Azure SAS policies),
**session processing over push** (the server hands out sessions and renews their locks), a **Docker image** with
**docker-compose for Prometheus + Grafana** (ready-made dashboard), and live in/out rates on the web dashboard.

In 1.2: **WebSocket push delivery** with prefetch (processors use it automatically; about 1.8× the throughput of
long-polling in `Sample.Benchmark`), **auto-forwarding**, **SQL rule actions**, and **metrics** (Prometheus at `/api/metrics`,
plus a .NET `Meter` named `NexusMQ` for OpenTelemetry).

In 1.1: **message sessions** (ordered processing per key, with session state), **SQL subscription filters**,
the **NexusMQ.AspNetCore** package (embed the broker, DI client, background handlers), and NuGet packaging.

## Requirements
- Visual Studio 2022 17.14+ or Visual Studio 2026, with the **ASP.NET and web development** workload
- .NET 10 SDK

## Open and run in Visual Studio
1. Open `NexusMQ.slnx` (or `NexusMQ.sln` on older Visual Studio).
2. Right-click the solution → **Configure Startup Projects** → **Multiple startup projects**. Set `NexusMQ.Server`, `Sample.Consumer` and `Sample.Producer` to **Start**, with the server first.
   To try the embedded-broker web app instead, set `Sample.WebApp` as the single startup project and POST to `http://localhost:5690/orders`.
3. Press **F5**. The dashboard opens at http://localhost:5680.
4. Run the tests from **Test → Test Explorer**.

`src/NexusMQ.Server/NexusMQ.Server.http` has ready-made API calls you can run from inside Visual Studio.

## Server setup (run NexusMQ without Visual Studio)

**دليل الاستخدام بالعربية خطوة بخطوة مع صور:** [docs/GUIDE-AR.md](docs/GUIDE-AR.md) (Arabic step-by-step guide with screenshots).
To see it working right away: start the server, open http://localhost:5680, and run `nexusmq demo`.

Build a ready-to-run setup into the `setup` folder of this project:
```powershell
.\scripts\make-setup.ps1                    # portable (needs the .NET 10 runtime)
.\scripts\make-setup.ps1 -Runtime win-x64   # self-contained NexusMQ.Server.exe (no .NET needed)
```
Then `setup\start-server.cmd` starts the broker (dashboard at http://localhost:5680), `setup\nexusmq.cmd` is the CLI, and
`setup\install-service.ps1` (as administrator) installs it as a **Windows service** (automatic start, in services.msc, restarted if it
fails). See `setup\README.md`.
Every GitHub release also has these setups as downloads (`NexusMQ-<version>-setup-win-x64.zip`, `-linux-x64.tar.gz`, `-portable.zip`).

## Command-line tool
```powershell
dotnet tool install -g NexusMQ.Cli --add-source C:\projects-claude\NexusMQ\nupkg
nexusmq create queue orders --max-delivery 5
echo '{"id":42}' | nexusmq send orders --subject order.created --prop region=eu
nexusmq listen orders
nexusmq stats
nexusmq keys create orders-api --rights Send --entity orders
```
Set `NEXUSMQ_URL` and `NEXUSMQ_KEY` (or pass `--url` / `--key`). Run `nexusmq help` for every command.

## Operations
- **Message browser:** the dashboard's peek/DLQ view lists messages with **view**, **resend** and **delete** buttons.
  From the CLI: `nexusmq resend 'orders/$deadletter' 12` moves one message back to where it failed, `nexusmq resend orders 12 --to archive --copy`
  copies one elsewhere, and `nexusmq delete-message orders 12` removes one.
- **Audit log:** management actions and denied requests are recorded with who, what, target, result and IP. See them with
  `nexusmq audit`, the dashboard's **Recent activity** panel, or `GET /api/audit`. They are saved to `data/audit.log` (JSON lines). Secrets and message bodies are never logged.
- **Rate limits:** set `RequestsPerSecond` (and an optional `Burst`) on a key in config, with `nexusmq keys create … --rps 50`, or
  with `nexusmq keys limits <name> --rps 50`. Over the limit the server answers 429 with `Retry-After`, and the .NET client waits and retries
  (`NexusClientOptions.MaxRateLimitRetries`, default 3).
- **Servers panel:** in a cluster or primary/standby pair, the dashboard shows each server's role, health and lag, and lets you add or remove cluster servers.
- **HTTPS:** see [`docs/TLS.md`](docs/TLS.md) for Kestrel certificates, `docker-compose.tls.yml` and reverse proxies. `scripts/new-dev-cert.ps1` makes a development certificate.

## Docker (broker + Prometheus + Grafana)
```powershell
docker compose up -d --build
```
Broker and dashboard on http://localhost:5680 (master key `change-me`, set `NEXUSMQ_API_KEY` to change it),
Prometheus on http://localhost:9090, Grafana on http://localhost:3000 (admin / admin) with the **NexusMQ** dashboard.

## Command line
```powershell
dotnet run --project src/NexusMQ.Server            # broker + dashboard on http://localhost:5680
dotnet run --project samples/Sample.Producer
dotnet run --project samples/Sample.Consumer
dotnet test
```

## Using the client SDK
```csharp
await using var client = new NexusClient("http://localhost:5680");

// Service Bus style
await client.Admin.CreateQueueAsync("orders", new NexusQueueOptions { MaxDeliveryCount = 5 });
await client.CreateSender("orders").SendMessageAsync(NexusMessage.FromJson(new { Id = 1 }));

var processor = client.CreateProcessor("orders", new NexusProcessorOptions { MaxConcurrentCalls = 4 });
processor.ProcessMessageAsync += async args => Console.WriteLine(args.Message.BodyAsString);
processor.ProcessErrorAsync += args => Task.CompletedTask;
await processor.StartProcessingAsync();

// RabbitMQ style
await client.Admin.CreateExchangeAsync("logs", NexusExchangeType.Topic);
await client.Admin.BindAsync("logs", "orders", "*.error");
await client.CreateSender("exchanges/logs").PublishAsync("billing.error", new NexusMessage("boom"));

// Topics and filtered subscriptions (wildcard or SQL)
await client.Admin.CreateTopicAsync("events");
await client.Admin.CreateSubscriptionAsync("events", "billing",
    [new NexusSubscriptionRule { SubjectPattern = "order.#" }]);
await client.Admin.CreateSubscriptionAsync("events", "big-eu",
    [new NexusSubscriptionRule { SqlFilter = "region = 'eu' AND amount > 100" }]);

// Sessions: ordered processing per key
await client.Admin.CreateQueueAsync("shipments", new NexusQueueOptions { RequiresSession = true });
await client.CreateSender("shipments").SendMessageAsync(new NexusMessage("packed") { SessionId = "order-42" });
await using var session = await client.AcceptNextSessionAsync("shipments");
```

## Entity paths
| Path | Meaning |
|---|---|
| `orders` | queue |
| `orders/$deadletter` | the queue's dead-letter queue |
| `events/subscriptions/billing` | subscription |
| `events/subscriptions/billing/$deadletter` | the subscription's dead-letter queue |
| `exchanges/logs` | publish through an exchange |

## Configuration (`src/NexusMQ.Server/appsettings.json`)
| Key | Default | Meaning |
|---|---|---|
| `NexusMQ:DataDirectory` | `data` | Folder for message journals and `topology.json` |
| `NexusMQ:Durable` | `true` | `false` keeps everything in memory only |
| `NexusMQ:StorageFormat` | `Binary` | How journals and the Raft log are written. `Binary` (from 1.13): raw bodies, about 25% smaller on disk for large messages. `Json`: the format of 1.12 and earlier; set it **before** upgrading if you may need to go back. Files in the other format are read and converted either way. |
| `NexusMQ:FlushToDisk` | `false` | fsync writes before confirming them (safer, slower; in a cluster one fsync covers a whole batch) |
| `NexusMQ:ApiKey` | empty | Master key with all rights. Clients send it in the `X-Api-Key` header |
| `NexusMQ:AccessKeys` | empty | Named keys with rights on chosen entities (see below). No keys at all = open, for development |
| `NexusMQ:MaxWaitSeconds` | `60` | Longest allowed long-poll wait |

## Access keys
```json
"NexusMQ": {
  "ApiKey": "master-secret",
  "AccessKeys": [
    { "Name": "orders-api",  "Key": "…", "Rights": "Send",          "Entities": [ "orders", "events" ] },
    { "Name": "billing-svc", "Key": "…", "Rights": "Listen",        "Entities": [ "events/subscriptions/billing" ] },
    { "Name": "tenant-acme", "Key": "…", "Rights": "Send, Listen",  "Entities": [ "acme-*" ] },
    { "Name": "prometheus",  "Key": "…", "Rights": "Monitor" }
  ]
}
```
- **Send**: send messages and cancel scheduled ones.
- **Listen**: receive, peek, settle, use sessions.
- **Manage**: create and delete entities, bindings, purge, resubmit. Includes Monitor.
- **Monitor**: read-only stats, topology and `/api/metrics`.

`orders` also covers `orders/$deadletter`, `events` covers its subscriptions, and `*` is a wildcard. Leaving out `Entities` means all entities.
Clients pass the key as `new NexusClient(url, key)`. Prometheus can pass it as `?apiKey=`.

Keys can also be managed at runtime, with no restart: `nexusmq keys create|rotate|delete`, the dashboard's **Access keys** panel,
or `client.Admin.CreateKeyAsync / RotateKeyAsync / DeleteKeyAsync`. Runtime keys are stored as SHA-256 hashes in
`data/access-keys.json`, and the secret is shown once. `rotate --grace 300` keeps the old secret working for 5 minutes so apps can switch
without downtime. Safety rules:
- On an open broker, the first key must include Manage.
- A key can only grant rights it holds itself.
- You can't delete the last broker-wide Manage key.

## Project layout
```
src/NexusMQ.Core        broker engine (no web dependencies)              → NuGet: NexusMQ.Core
src/NexusMQ.Client      .NET client SDK (no dependencies)                → NuGet: NexusMQ.Client
src/NexusMQ.AspNetCore  HTTP API, dashboard, DI + hosted handlers        → NuGet: NexusMQ.AspNetCore
src/NexusMQ.Server      standalone broker host (3 lines on top of AspNetCore)
src/NexusMQ.Cli         nexusmq command-line tool                         → NuGet tool: NexusMQ.Cli
samples/                producer + consumer console apps, Sample.WebApp (embedded broker), Sample.Benchmark
nupkg/                  built packages (regenerate with pack.ps1)
Dockerfile, docker-compose.yml, deploy/   container image, Prometheus + Grafana setup
docker-compose.tls.yml, scripts/          HTTPS override and development certificate script
docker-compose.ha.yml                     primary + standby pair with automatic failover
docker-compose.cluster.yml                3-node Raft cluster
tests/NexusMQ.Tests    xUnit tests: engine, routing, persistence, access rules, plus Integration/ tests against a live server
scripts/live-regression.sh  end-to-end check of every feature against real servers (failover, leader kill, …), PASS/FAIL per item
tools/NexusMQ.ChaosClient   nexusmq-chaos: steady producer + consumer that reports lost and duplicated messages
docs/PLAN.md           architecture and roadmap
```
