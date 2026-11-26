# NexusMQ — a message broker inspired by RabbitMQ and Azure Service Bus

## Goal
A self-hosted message broker written in C#/.NET 10 that combines:
- **RabbitMQ routing**: exchanges (direct, fanout, topic, headers) bound to queues.
- **Azure Service Bus semantics**: queues and topics/subscriptions, peek-lock receive,
  complete / abandon / dead-letter / defer, max delivery count, dead-letter queues,
  TTL, scheduled messages, duplicate detection.

## Solution layout (open `NexusMQ.sln` or `NexusMQ.slnx` in Visual Studio)

| Project | Role |
|---|---|
| `src/NexusMQ.Core` | Broker engine: entities, routing, locking, DLQ, persistence. No web dependency. |
| `src/NexusMQ.AspNetCore` | HTTP/JSON API (`MapNexusMQ`), dashboard, DI client and hosted handlers. NuGet package. |
| `src/NexusMQ.Server` | Standalone broker host built on NexusMQ.AspNetCore, dashboard at `/`. |
| `src/NexusMQ.Client` | .NET SDK: `NexusClient`, `NexusSender`, `NexusReceiver`, `NexusProcessor`, `NexusAdminClient`. |
| `samples/Sample.Producer` | Creates entities and publishes messages. |
| `samples/Sample.Consumer` | Processor with concurrency + auto-complete, like `ServiceBusProcessor`. |
| `samples/Sample.WebApp` | Your-app example: embedded broker, DI handlers, SQL-filtered subscription, ordered sessions. |
| `tests/NexusMQ.Tests` | xUnit tests for engine behaviour and an end-to-end HTTP test. |

## Concepts

| NexusMQ | RabbitMQ equivalent | Azure Service Bus equivalent |
|---|---|---|
| Queue | Queue | Queue |
| Exchange (Direct/Fanout/Topic/Headers) + Binding | Exchange + Binding | — |
| Topic + Subscription (+ filter rules) | Fanout exchange + queues | Topic + Subscription + Rules |
| Peek-lock receive, `Complete` | `basic.ack` | `CompleteMessageAsync` |
| `Abandon` | `basic.nack(requeue=true)` | `AbandonMessageAsync` |
| `DeadLetter` | `basic.reject` → DLX | `DeadLetterMessageAsync` |
| `$deadletter` sub-queue | Dead-letter exchange | `/$deadletterqueue` |
| ReceiveAndDelete | auto-ack | ReceiveAndDelete mode |

Entity paths:
- Queue: `orders`
- Queue DLQ: `orders/$deadletter`
- Subscription: `events/subscriptions/billing` (DLQ: `events/subscriptions/billing/$deadletter`)
- Exchanges publish via `exchanges/{name}` with a routing key.

## Message lifecycle
1. Send → (duplicate check) → if `ScheduledEnqueueTimeUtc` is in the future, hold as scheduled; else Active.
2. Receive (peek-lock) → message Locked for `LockDuration`, `DeliveryCount++`, lock token returned.
3. Complete → removed. Abandon / lock expiry → Active again. `DeliveryCount > MaxDeliveryCount` → DLQ.
4. TTL expired while Active → DLQ (reason `TTLExpiredException`) if `DeadLetterOnExpiration`, else dropped.
5. Defer → kept aside, retrievable only by sequence number.

## Persistence
`IMessageStore` with two implementations:
- `InMemoryMessageStore` (fast, non-durable; used in tests).
- `FileMessageStore`: per-entity append-only journal (JSON lines: put/remove records) in `data/`,
  replayed on startup, compacted when the journal grows. Entity definitions live in `data/topology.json`.

## Transport (Phase 1: HTTP/JSON)
- `PUT/DELETE /api/queues/{name}`, `/api/topics/{name}`, `/api/topics/{t}/subscriptions/{s}`, `/api/exchanges/{name}`, `/api/bindings`
- `POST /api/messages/{*entity}` send (single or batch)
- `POST /api/receive/{*entity}?max=10&waitSeconds=20&mode=peeklock` long-poll receive
- `POST /api/locks/{*entity}` complete / abandon / deadletter / defer / renew
- `GET /api/peek/{*entity}`, `GET /api/stats`, dashboard at `/`.
- Optional API key via `NexusMQ:ApiKey` (header `X-Api-Key`).

## Sessions (Phase 2)
Entities with `RequiresSession = true` need a `SessionId` on every message. A receiver locks a whole session
(`AcceptNextSession` or `AcceptSession(id)`), gets that session's messages in order, can store session state,
and renews or releases the session lock. `NexusSessionProcessor` runs many sessions in parallel while keeping each one in order.
HTTP: `/api/sessions/accept|receive|renew|release|state/get|state/set/{entity}`.

## SQL filters (Phase 2)
`SubscriptionRule.SqlFilter` supports `= <> != > >= < <=`, `AND OR NOT`, `LIKE` (with `%`, `_` and `ESCAPE`), `IN`, `IS [NOT] NULL`, `EXISTS()`,
`sys.*` system properties, `[quoted names]`, numeric comparison of numeric strings, and SQL three-valued logic.

## Push delivery (Phase 3)
`/api/ws` is a WebSocket with JSON frames (`subscribe`, `unsubscribe`, `settle`, `credit`, `send`, `ping`).
The server pushes messages as they arrive, holding at most `prefetch` unsettled messages per subscription (credit-based flow
control, like RabbitMQ `basic.qos`). When a socket closes, every message it holds a lock on is abandoned at once.
`NexusProcessor` uses push by default (`Transport = WebSocket`) and reconnects with back-off; `LongPolling` remains available.
Session-enabled entities are still consumed through the HTTP session API.

## Auto-forwarding and rule actions (Phase 3)
`QueueOptions.ForwardTo` passes every arriving message to another queue, topic or exchange (max 8 hops; loops stop and keep the message).
`ForwardDeadLetteredMessagesTo` sends dead letters to a shared queue and falls back to the entity's own DLQ if the target is gone.
`SubscriptionRule.SqlAction` (`SET x = 'v'; SET sys.Subject = 'a.' + region; REMOVE y`) edits the copy delivered to that subscription.

## Metrics (Phase 3)
Meter `NexusMQ`: counters `nexusmq.messages.sent|delivered|completed|abandoned|deadlettered|expired`, `nexusmq.locks.expired`,
and gauges `nexusmq.entity.active|locked|scheduled|deadletter` (tag `entity`). Prometheus text at `/api/metrics`.

## Access control (Phase 4)
`NexusServerOptions.AccessKeys`: named keys with `Send`, `Listen`, `Manage` and `Monitor` rights, scoped to entity patterns
(`orders`, `events/subscriptions/billing`, `tenant-*`; empty = all). `ApiKey` stays as the master key. Keys come from the
`X-Api-Key` header or `?apiKey=`, compared in constant time. HTTP routes map to a right and entity (`NexusAuthorizer.RequirementFor`).
WebSocket connections authenticate once, then each subscribe, settle and send is checked. 401 = no or invalid key, 403 = key lacks the right.

## Sessions over push (Phase 4)
`subscribe` with `"sessions": true`: the server accepts the next free session, sends `session`, pushes that session's messages
(prefetch 1 by default for strict order even across retries), renews the session lock itself, and sends `sessionClosed` after
`idleSeconds` without messages. `NexusSessionProcessor` uses one subscription per concurrent session slot.

## Deployment (Phase 4)
`Dockerfile` (multi-stage, non-root, data volume `/data`) and `docker-compose.yml` with Prometheus (scraping `/api/metrics` with a
Monitor key) and Grafana (provisioned data source plus a "NexusMQ" dashboard: throughput, backlog, locks, dead letters, failures).

## Runtime key management (Phase 5)
`GET/POST /api/keys`, `POST /api/keys/{name}/rotate?graceSeconds=`, `DELETE /api/keys/{name}` (broker-wide Manage).
Secrets look like `nmq_…` (256 random bits). They are returned once and stored as SHA-256 hashes in `access-keys.json`.
Rules: the first key on an open broker must include Manage, a key cannot grant rights it lacks (no escalation),
config keys are read-only through the API, and the last broker-wide Manage key cannot be deleted. Last-used time is tracked in memory.

## CLI (Phase 5)
`NexusMQ.Cli` is a .NET tool (`nexusmq`) with send/peek/receive/listen, stats/topology/metrics, create/delete/bind/unbind,
purge/resubmit and keys list/create/rotate/delete. It also offers `--json` output, `NEXUSMQ_URL`/`NEXUSMQ_KEY`, and exit codes 0 / 1 (server error) / 2 (usage).

## Integration tests (Phase 5)
`tests/NexusMQ.Tests/Integration` starts a real server (Kestrel on a random port, in-memory storage) per test class and drives it
through the public client: HTTP and WebSocket delivery, dead-lettering, sessions, filters, exchanges, metrics, and key enforcement and rotation.

## Operations (Phase 6)
- **Audit log** (`NexusAuditLog`): every Manage action (except reads) and every 401/403, over HTTP or WebSocket, records actor (key name),
  action (method + route), target, result, detail (options, binding, key rights, query) and remote IP. The latest 2000 entries
  are kept in memory for `GET /api/audit`, and the log is appended to `audit.log` (rolled at 10 MB). Secrets and bodies are never recorded.
- **Rate limits** (`NexusRateLimiter`, token bucket per key): `RequestsPerSecond` and `Burst` on config or runtime keys
  (`POST /api/keys/{name}/limits`). Each HTTP call and each WebSocket operation costs one token. Over the limit the server returns 429 with
  `Retry-After`, or a WebSocket error frame with `retryAfterMs`. The client retries automatically.
- **Single-message operations:** `DELETE /api/messages/{entity}?seq=N` and
  `POST /api/messages/resend/{entity}?seq=N[&to=dest][&move=]`. From a DLQ the default moves the message back to its source,
  including back to that one subscription. Elsewhere the default copies it. A move keeps the original if the copy was not accepted.
- **TLS:** Kestrel certificates by configuration, `docker-compose.tls.yml`, reverse-proxy notes (`docs/TLS.md`), and
  `NexusClientOptions.AllowUntrustedCertificates` / CLI `--insecure` for development.

## High availability (Phase 7)
- **Replication log:** `ReplicatingMessageStore` wraps the primary's store, and every Put, Remove, DeleteEntity, Replace (compaction) and Topology change, plus
  runtime-key changes, is appended to an in-memory ring (`ReplicationLog`, default 200,000 changes). Ops are idempotent.
- **Stream:** `GET {api}/replication/stream?from=N` (WebSocket, broker-wide Manage key). A standby that is new or too far behind gets a
  snapshot (`Broker.ExportState`, consistent because ops after the snapshot position are replayed idempotently), then batches of ops and
  2-second heartbeats.
- **Standby** (`NexusNode` role Standby): applies the stream to its own store, answers 503 `Standby` to client traffic (status and health still work),
  and on promotion builds the broker from the replicated store and starts its own replication log, so a new standby can follow it.
  Auto-failover promotes after `FailoverAfterSeconds` of silence, but only once it has synced.
- **Clients:** comma-separated URLs. On a connection failure or 503 Standby they move to the next server (HTTP and WebSocket), and wait
  through a failover for up to `FailoverWaitTime`.
- **Limits:** replication is asynchronous (a tiny loss window on a crash), delivery is at-least-once, and there is a split-brain risk with auto-failover
  across an unreliable network. See `docs/HA.md`.

## Raft cluster (Phase 8)
- `NexusMQ.Core/Cluster`: `RaftLog` (durable log plus term/vote/applied state, compaction, truncation) and `RaftNode` (elections with randomized
  timeouts, AppendEntries replication with fast conflict back-off, majority commit of current-term entries, a no-op on election, InstallSnapshot
  for laggards, and check-quorum so a cut-off leader steps down). Transport is pluggable (`IRaftTransport`), so there are fake-network unit tests and HTTP in production.
- **State machine:** the existing replication ops. The leader's broker writes through `RaftStore`, which turns each store change into a log entry.
  All nodes apply committed entries to their store, so every store holds exactly the committed state and snapshots are store exports.
- **Write barrier:** each HTTP request and WebSocket operation records the log entries it produced (AsyncLocal) and answers only after
  majority commit; otherwise it returns 503 `NotCommitted`.
- **Leadership:** when a node becomes leader and has applied its no-op, it builds the broker from its store. On losing leadership it drops the broker and
  closes push connections. Followers answer 503 with `X-NexusMQ-Leader`, which clients follow.

## Membership changes and committed-only delivery (Phase 10)
- **Membership** is a Raft log entry (`RaftEntry.Config`) and takes effect as soon as it is in a node's log. Snapshots carry the membership too.
  The first leader writes the static configuration into the log.
- Changes go **one server at a time** (Ongaro, section 4.1): new servers catch up as non-voting **learners**, then get a config entry. Only one change can be
  uncommitted at a time, and a leader that removes itself steps down once the change commits.
- **Join mode** (`Cluster:Join`): a new server has no membership and never campaigns until the leader replicates the configuration to it.
- **Leader stickiness:** a server that has heard from a leader within the election timeout rejects higher-term vote requests, so removed servers can't
  disrupt the cluster. Only voters run elections.
- **Committed-only delivery:** `RaftStore.Put` records the entry index on the message. Entities stop delivering at the first message whose index is
  above the commit index, which preserves order. The node wakes receivers whenever the commit index moves (taking the signal before reading the index, so no
  advance is missed: a regression test covers the lost-wake-up bug found by the benchmark).
- WebSocket frames are handled concurrently per connection (up to 64), so cluster settlements share commits (push throughput in a 3-node cluster went from 455 to 1,100 msg/s).

## Performance (Phase 11)
- Profiled a 3-node cluster: the servers were CPU-bound, but the Raft and store code itself costs only ~10 µs per entry. Most of the time went into
  overhead: a rewrite of `raft-state.json` after every apply batch, and the appsettings file watcher waking on every data write (the data folder sits
  inside the content root).
- **Group commit:** `RaftLog.Append` buffers, `TakePending`/`Flush` write a batch (fsync with `FlushToDisk`) outside the Raft lock, and
  `DurableIndex` limits what the leader counts for itself. Followers flush once per AppendEntries call before acknowledging. A rewrite (truncate,
  compaction, snapshot) bumps a generation number, so a flush captured before it is dropped instead of appending stale entries.
- **Batched apply:** `FileMessageStore.DeferFlush()` buffers journal writes for the whole apply batch. The store is flushed before LastApplied is
  persisted (every 500 entries or once a second) and before log compaction.
- The server starts with `hostBuilder:reloadConfigOnChange=false`, and embedded brokers warn when the data folder is under a watched content root.
- Results on the 2-core machine: cluster push throughput +60% (2,200–2,500 → 3,700–4,000 msg/s), with `FlushToDisk` +65% push and +95% long-polling.
  Tests prove the leader never counts an entry it has not written (with the bug put back, the test fails).
- `scripts/live-regression.sh` replays every live scenario from phases 1–10 (samples, CLI, keys, rate limits, HTTPS, failover, leader kill,
  membership changes, minority refusal, benchmarks) against real servers and prints PASS/FAIL. `tools/NexusMQ.ChaosClient` is its
  producer/consumer that counts lost and duplicated messages.

## Cluster management in the dashboard (Phase 12)
- A **Servers** panel at the top of the dashboard, fed by `GET /api/replication/status` every 2 s (Monitor key).
  - **Raft leader:** every member and learner with role, health (from `PeerLastContactMs`, new on `RaftStatus`: ok under 1.5 s, slow, unreachable over 5 s),
    entries behind, time since its last answer, voters reachable against the majority (amber when one more failure would stop writes), an
    **Add server** form and a **remove** button per server (the leader can remove itself and hands over).
  - **Follower:** a banner with a link to the leader's dashboard (browsers can't follow the 503 leader hint across servers). The entity panels pause
    instead of showing an error every 2 s.
  - **Removed or not-yet-joined server:** says so and links to the members' dashboards, instead of claiming an election is running.
  - **Primary/standby pair:** primary and standbys with positions, lag and contact. On the standby, a banner links to the primary.
- Also fixed: wide entity rows pushed the right-hand column off-screen (panels now scroll sideways), and an empty tag showed as a dot.
- Checked in headless Chrome against live servers: leader and follower views, adding a 4th server with the form, a killed server turning
  unreachable, removing it, removing the leader from its own page (hand-over to a new leader), and the pair views. No JavaScript errors.

## Faster links (Phase 13)
Every change was measured on the 2-core machine. To simulate servers about 1 ms apart, a latency proxy holds every byte for 1 ms in each
direction (the sandbox has no `tc netem`). Only changes that measured better became defaults.
- **Server-to-server WebSocket** (`Cluster:Transport`, default `WebSocket`): one connection per peer, binary frames `[kind][id][JSON]`, many
  calls multiplexed, handled in order by the receiver. A full HTTP request cost more CPU than the Raft work it carried. Results:
  +5–15% cluster throughput and about 25% lower latency with 1 ms between servers (median 3.1–3.6 ms → 2.4–2.5 ms).
  Snapshots stay on HTTP. A peer without the endpoint (404/405, i.e. an older version) is reached over HTTP for 5 minutes, then the socket
  is retried. Tested live with a mixed 1.10/1.11 cluster: a 1.10 leader killed under traffic, a 1.11 leader taking over, the 1.10 server
  rejoining over HTTP, and 987/987 messages delivered with no loss and no duplicates.
- **Pipelined replication** (`Cluster:MaxInflightAppends`, default **1**): implemented and correct (all Raft tests pass with 4), but slower here.
  With several calls in flight, batches shrink (4.6 → 3 entries per call), so a CPU-bound cluster does more calls for the same work, and
  long-polling throughput dropped 10–25%. It stays available for servers far apart with spare CPU.
- **Binary message bodies on push** (`NexusClientOptions.BinaryBodies`, default on): a client that connects with `?binaryBodies=1` receives each
  message as `[header length][header JSON][raw body]` instead of base64 inside JSON. Base64 plus JSON escaping of a 64 KB body took 182 µs and
  a large-object-heap string per message. Single server, push throughput: 16 KB bodies 2,000–2,200 → 9,400–11,700 msg/s (about 5×), 64 KB
  bodies 850–990 → 3,100–4,300 msg/s (about 4×), small bodies unchanged (checked with 30,000-message runs; the short runs were too noisy).
  Older servers ignore the flag and send JSON, which the client still reads. `NexusReceivedMessage.Body` is now decoded once instead of on every read.
- Not done, by measurement: a binary format for small messages. JSON costs about 3 µs of the ~45 µs per message, so at most ~5% to gain.

## Binary sends (Phase 14)
- `POST /api/send/{entity}` also accepts `Content-Type: application/vnd.nexusmq.batch`: `[header length: int32 LE][JSON list of messages, each
  with "bodyLength" instead of a body][raw bodies, in order]`. Same endpoint, so access keys, rate limits, audit and the cluster commit barrier
  apply unchanged. Malformed batches (lengths that don't add up, impossible header length) get 400 with a reason.
- The .NET client sends binary batches by default (`NexusClientOptions.BinaryBodies`, which now covers both directions). A server before 1.12
  answers 415 (checked against a real 1.10 server); the client then resends as JSON and keeps using JSON with that client for 5 minutes.
- Single server, send throughput (250-message batches): 100 B bodies 38k–65k → 75k–83k msg/s, 16 KB 2,500–2,800 → 6,600–11,100 msg/s,
  64 KB 850–880 → 2,200–2,500 msg/s. Median end-to-end latency at 64 KB 3.4–3.6 ms → 1.0 ms.
- 3-node cluster at 16 KB: consuming 1,000–1,160 → 3,200–4,000 msg/s, sending 900–1,160 → 1,440–1,960 msg/s, latency 2.7–3.7 → 1.85 ms.
  Sending gains less here because inside the cluster bodies are still base64 in the Raft log, between servers and in the journals.
- `Sample.Benchmark` now also reports send throughput and takes a body size (`… [messages] [key|-] [bodyBytes]`, `NEXUSMQ_BINARY=0` for JSON).

## Binary storage (Phase 15)
- Measured first: inside a server, a 16 KB message costs ~13 µs of base64 per encode, but the bigger cost is bytes. Every server writes each body
  twice (Raft log and journal), and base64 makes both copies a third bigger.
- **Record format** (`BinaryRecords`): magic `NMQBIN1\n`, then `[length][CRC-32C][flags][JSON length][JSON without the body][raw body]`. The CRC
  is hardware-accelerated (`BitOperations.Crc32C`). A cut-off or corrupt record ends the readable part of the file. The tail is trimmed before
  anything new is appended (journals) or the file is rewritten from memory (Raft log), so new records never land behind damage.
- **Journals** keep their names and are detected by the magic. **The Raft log** is `raft-log.bin` (`raft-log.jsonl` in Json mode).
- **Compatibility:** files in the other format are read and converted on load, in both directions. `NexusMQ:StorageFormat=Json` keeps the 1.12
  format, so you can still go back. Tested live: a 1.10 single server and a 1.10 cluster with data (including a 20 KB random body) restarted on
  1.13. All messages were present, the body matched by SHA-256, the files were converted, and writes and replication continued.
- Also fixed: journal lines were built as UTF-16 strings before writing (costly for large bodies), and `Dispose` didn't flush pending writes.
- Results, 3-node cluster, Json vs Binary storage on the same build:
  - 16 KB: send 1,510–1,780 → 1,830–2,070 msg/s, consume 2,670–2,820 → 2,750–3,000 msg/s, p99 11–26 → 7–8 ms, disk 1,309 → 989 MB (−24%).
  - 100 B: no clear difference (within run-to-run noise).
- Tests prove the checksum and the tail trim (each test fails with its protection removed).

## Windows service and xUnit v3 (Phase 18, 1.17)
- `NexusMQ.Server --windows-service` runs under Windows' Service Control Manager. It calls advapi32 directly
  (`StartServiceCtrlDispatcher`, `RegisterServiceCtrlHandlerEx`, `SetServiceStatus`) on a background thread, as ServiceBase does, so no
  extra package is needed (NuGet cannot be reached from the build sandbox). It reports RUNNING once Kestrel listens, and STOP_PENDING →
  host shutdown → STOPPED on Stop or machine shutdown. Content root is the program folder (services start in System32). Started by hand
  with the flag, it falls back to a console app with a warning.
- `setup/install-service.ps1` / `uninstall-service.ps1`: automatic start, shown in services.msc, recovery restarts after 5 s / 5 s / 1 min.
  The startup-task scripts remain as an alternative.
- CI (Windows runner) installs the service, checks it serves, `Restart-Service` (message kept), kills the process (Windows restarts it),
  `Stop-Service` (nothing serving afterwards, data in the chosen folder), then uninstalls it.
- Tests moved to xUnit v3 (`xunit.v3`, test project is an executable). No test code changes were needed (only facts, theories and
  assertions are used). Dependabot no longer holds back xunit.runner.visualstudio 4.x.

## Demo and Arabic guide (1.16)
- `nexusmq demo [--minutes 5]` creates demo-orders (queue, poison orders dead-lettered after 3 tries), demo-events (topic; "billing" with
  SQL filter amount > 500, "shipping" processed slowly so a backlog builds up), demo-logs (topic exchange, `*.error` → demo-errors) and a
  scheduled message, then sends and processes orders with Arabic and English customer names, printing progress every 3 s.
- `docs/GUIDE-AR.md`: Arabic step-by-step guide (start, dashboard columns, peek, dead letters, routed errors, trying it yourself, CLI,
  C# client, clean-up, troubleshooting), with screenshots in `docs/guide/` taken from a live run. Every command in it was run as written.
- First GitHub release: v1.15.0 (packages, symbols, win-x64/linux-x64/portable setups, container image), started from the API with the
  manual release trigger, because this session's git proxy does not push tags.

## Setup and first release (Phase 17)
- `scripts/make-setup.ps1` / `make-setup.sh` build `setup/` in the project folder: `server/` (published broker), `cli/` (nexusmq),
  `start-server.cmd/.sh`, `nexusmq.cmd/.sh`, `install-startup-task.ps1` / `uninstall-startup-task.ps1` and a README. Portable by default;
  `-Runtime win-x64` (or linux-x64, …) makes a self-contained single-file build. The start scripts pass `--urls` (default
  `http://localhost:5680`), because a published server would otherwise listen on .NET's default port 5000.
- Starting with Windows uses a scheduled task (at boot, as SYSTEM, restarts every minute if it stops, data in `C:\ProgramData\NexusMQ\data`).
  A Windows service would need `Microsoft.Extensions.Hosting.WindowsServices`, which could not be downloaded and tested here.
- CI: a `setup-windows` job builds the self-contained setup on a real Windows runner, starts it, uses the CLI, and installs, checks and removes
  the startup task. The release workflow attaches win-x64, linux-x64 and portable setups to every GitHub Release.
- Dependabot: GitHub Actions bumps (checkout stays v5; setup-dotnet v6, upload-artifact v7, docker actions v4/v6) and test tools
  (Microsoft.NET.Test.Sdk 18.12, coverlet 10.1) applied on main. Ignored from now on: .NET 11 container images (they must move with the
  target framework) and xunit.runner.visualstudio 4.x (that is for xUnit v3).
- **Duplicate detection now survives restarts and leader changes** (found by an intermittent Windows CI failure: a batch of 10 stored twice
  after an election). Its history was only in memory, so a server that restored a queue from disk forgot it, and a client's retried send
  became a second copy. Now the history is rebuilt from the stored messages still inside the window, and `NexusMessage` gets its
  `MessageId` when created, so a retry carries the same id. Limit: ids of messages already completed before a restart are not remembered.
- Also fixed a race in a Raft test (checked the snapshot index right after the message count, which the follower updates a moment apart).
- Tested here: the portable setup started from `start-server.sh`, served the dashboard, and the bundled CLI created a queue, sent and received a
  message, with the data in `setup/server/data`.

## Binary replication between servers (Phase 16)
- New server-to-server call `KindAppendBinary`: `[header length][AppendEntries JSON without bodies][body count][body lengths][raw bodies]`
  (`RaftFrames`). A server announces it with `X-NexusMQ-Raft-Features: binary-append` on the WebSocket upgrade, so leaders send it only to
  servers that understand it and keep JSON for 1.11–1.13 peers.
- `GET /api/replication/status` → `appendsReceived` `{binary, socketJson, http}`: how this server's AppendEntries arrived, which shows when a
  rolling upgrade has reached the binary format.
- Mixed-version cluster tested live, with two new servers and one 1.13.1:
  - While the 1.13.1 server led, the new followers received JSON.
  - After it was killed under traffic, a new leader sent binary to the new follower (986/986 messages, no loss, no duplicates).
  - The restarted 1.13.1 server was sent JSON and caught up to the identical log.
- Measured, 3-node cluster, 1.13.1 vs 1.14 (alternating runs):
  - 64 KB: sending 740–1,000 → 960–1,070 msg/s, p99 7.2–7.4 → 6.4–6.9 ms (with a settle pause before consuming).
  - 16 KB: sending +3–12%. Consuming unchanged (its run-to-run spread is larger than any difference).
  - Server-to-server bytes about 25% fewer, which matters more on real networks than on this loopback test.
- `scripts/live-regression.sh` has an **upgrade** scenario. With `OLD_SERVER=<older NexusMQ.Server.dll>` it writes data with the old server and
  checks that the new one reads every message, matches a 20 KB random body by SHA-256, and converts the journal. CI builds the previous commit of
  `main` on every push and runs it.
- `Sample.Benchmark`: `NEXUSMQ_SETTLE_MS` waits between the send and consume phases.

## 1.13.1: connections on Windows (found by the first GitHub CI run)
- On the Windows runner every cluster test failed with "No leader elected", and an HA failover test timed out. `localhost` resolves to `::1` and
  `127.0.0.1`. The test servers listen on IPv4 only, and Windows takes ~2 s to refuse the IPv6 attempt. .NET tries addresses in order, so the
  server-to-server WebSocket (2 s connect timeout) never reached the IPv4 address.
- `NexusConnect` (client library) races all of a host's addresses and keeps the first that connects. It is used for every outgoing connection:
  Raft HTTP and WebSocket, the standby's replication stream, and the client's HTTP and push connections.
- Tested with a listener whose accept queue is full (attempts get no answer) placed first in the address list: connected through the
  second address in milliseconds, and the test fails when the race is removed.

## CI/CD (Phase 9)
- `.github/workflows/ci.yml`: build and test on ubuntu-latest and windows-latest (test classes run sequentially via `xunit.runner.json`),
  pack with `-ci.N` versions and install the CLI tool from the packages, and build and smoke-test the Docker image.
- `.github/workflows/release.yml` (tags `v*.*.*`): test, pack, push to GitHub Packages (and nuget.org if `NUGET_API_KEY` is set),
  create a GitHub Release, and push a multi-arch image to ghcr.io with semver and latest tags.
- Package metadata: MIT license, repository URL and commit (Source Link), `.snupkg` symbols, deterministic CI builds, and a configurable package-ID prefix.
- Checked locally: actionlint + shellcheck are clean, the pack job's commands produce correct packages, and the whole test suite passes against the real
  xUnit v2 assertion library compiled from source.

## Raft PreVote (Phase 19)
- Before raising its term, a server whose election timer expired asks the others "would you vote for me?" (`HandlePreVote`): the
  same checks as a real vote (no live leader, term, log up to date), but nothing is recorded. Only with a majority of yes does it run
  a real election. A server that is cut off gets no majority, so its term never moves.
- Peers agree on it when they connect: the Raft socket advertises `pre-vote` next to `binary-append`, and HTTP has
  `POST {api}/raft/prevote`. A peer without either (1.17 and older) is counted as yes without being asked, which is the old behaviour.
- `RaftStatus.PreVoteRounds` / `ElectionsStarted` show it working.
- Measured with three real servers, one of them cut off for 10 s through a TCP proxy and then reconnected:

  | | cut-off server's term | after reconnecting |
  |---|---|---|
  | 1.17.0 | 1 → 7 | forced an election (term 8) and took over as leader |
  | 1.18.0 | stayed 1 | followed the same leader, same term, no election |

- `live-regression.sh` (upgrade section, also run in CI) now starts a real mixed cluster: one server of this version and two of the
  previous release. This version is elected with the older servers' votes, the older ones elect each other, this version rejoins
  without an election and takes over again, and all confirmed messages are kept. The checks fail when older peers are not counted.
- Found and fixed while repeating the tests 10×: a server whose election was split stayed "candidate", and the first version of the
  PreVote check only let followers try again, so it could get stuck. The tests now run 25× clean.

## Compared with other Raft systems, leadership transfer (Phase 20, 1.19)
- Compared NexusMQ's elections with etcd raft, HashiCorp Raft, Kafka KRaft (KIP-996) and RabbitMQ Ra; the table is in
  `docs/CLUSTER.md`. Main gap: **leadership transfer** (Raft thesis §3.10), which all of them except KRaft have.
- `RaftNode.TransferLeadershipAsync(target?)`: refuse new writes (retryable 503), wait until the target has every entry, send
  `TimeoutNow`; the target campaigns at once, skipping PreVote, with `Transfer` set on its vote so voters skip the leader check.
  Given up after about two election timeouts, and the leader carries on.
- `HandleTimeoutNow` only accepts its current leader's request, in the current term, with its log complete (the raft-rs 234
  late-message problem cannot start an election).
- Negotiated: socket call kind 5 with the `timeout-now` feature, `POST {api}/raft/timeout-now`; older servers cannot be targets.
- Uses: `nexusmq cluster transfer [id]`, **make leader** in the dashboard, `POST /api/cluster/leader/transfer`, automatic on a clean
  leader shutdown (`TransferLeadershipOnShutdown`, an `ApplicationStopping` hook) and when the leader removes itself.
- `Cluster:PreVote` option (default on), as in etcd, Ratis and openraft.
- Tests (each sabotage-checked): hand-over to a named server and to the most up-to-date one, a transfer that cannot finish is
  given up and writes resume, stray/late TimeoutNow ignored, self-removal hands over, Ra issue 439 scenario, PreVote off; on real
  servers over both links and on a clean shutdown. The crash tests keep the hand-over off so they still test crashes.

## Planned leader restart, measured (Phase 21, 1.19)
- Measured a clean stop of the leader (SIGTERM, like a service stop or `docker stop`) of three real servers under steady
  traffic (`nexusmq-chaos` now reports `longestPauseMs`, the longest gap between confirmed sends, counting a gap still open at the end):

  | | stopping took | writes paused |
  |---|---|---|
  | 1.18.0 | 30.1 s | 30.2 s |
  | 1.19, hand-over off | 0.22–0.23 s | 2.7–2.8 s (an election) |
  | 1.19, hand-over on (default) | 0.22–0.44 s | 0.23–0.31 s |

- **Found by measuring: stopping any cluster server took 30 s**, and a stopping leader blocked writes all that time. The web host
  waits up to 30 s for open requests on shutdown, and the other servers' Raft WebSocket links (and clients' push connections, and a
  standby's stream) only ended when the other side closed them. Meanwhile the stopping leader kept sending heartbeats, so no one
  else took over. Now one shutdown hook runs in order: hand over (it needs those links), stop Raft (`RaftNode.Stop`: no heartbeats,
  votes or elections, as if gone), then end the long-lived connections (`NexusNode.Closing`).
- The .NET client waited a fixed 500 ms when no server could name a leader; now 50, 100, 200, 400, then 500 ms. That took the
  hand-over pause from 0.57–0.59 s to the figures above.
- `scripts/live-regression.sh` scenario `restart` (also in CI): a clean leader stop under traffic must hand over, stop within 5 s,
  pause writes under 1.5 s with no loss and no failed send; then `nexusmq cluster transfer` to a chosen server. A duplicate is a
  warning, not a failure (at-least-once: a completion that arrives during the hand-over is refused, so that message comes again).
- On GitHub's runner: planned restart 207 ms pause, leader removing itself under traffic 210 ms (it hands over too), a hard-killed
  leader 3.3 s (an election).
- The in-process cluster tests now take 1–3 s each instead of 30–60 s (they were waiting for the same 30 s on every stop).

## Tests on xUnit 4 (1.19)
- xUnit 4 runs on Microsoft Testing Platform v2, and the .NET 10 SDK needs `global.json` (`"test": {"runner":
  "Microsoft.Testing.Platform"}`) for `dotnet test` to use it. The VSTest packages (Microsoft.NET.Test.Sdk, xunit.runner.visualstudio,
  coverlet.collector) were removed. Run the tests with `dotnet test --project tests/NexusMQ.Tests` or Visual Studio 2026's Test Explorer.
- CI writes the results as TRX with xUnit's own reporter (`--report-xunit-trx`).

## Packaging
`NexusMQ.Core`, `NexusMQ.Client` and `NexusMQ.AspNetCore` are NuGet packages (`pack.ps1` → `nupkg/`).
See `docs/USING-IN-YOUR-PROJECT.md`.

## Roadmap
- **Phase 1 (done)**: queues, exchanges, topics/subscriptions, peek-lock, DLQ, TTL, scheduling, dedup, durable journal, HTTP API, dashboard, client SDK.
- **Phase 2 (done)**: message sessions + session state, SQL filters, ASP.NET Core integration (embedded broker, DI, hosted handlers), NuGet packages.
- **Phase 3 (done)**: WebSocket push with prefetch and abandon-on-disconnect, auto-forwarding, SQL rule actions, metrics (Meter + Prometheus), benchmark sample.
- **Phase 4 (done)**: access keys with per-entity rights, sessions over push, Docker image + Prometheus/Grafana compose, dashboard rates.
- **Phase 5 (done)**: runtime key management (API, CLI, dashboard), `nexusmq` CLI tool, live-server integration tests.
- **Phase 6 (done)**: audit log, per-key rate limits, message browser (view/resend/delete), HTTPS guide + Docker TLS + client options.
- **Phase 7 (done)**: primary/standby replication, manual and automatic failover, multi-URL client failover, HA compose file.
- **Phase 8 (done)**: Raft cluster with majority-confirmed writes, automatic leader election, catch-up by log or snapshot, cluster compose file.
- **Phase 9 (done)**: GitHub Actions CI (Linux + Windows), release pipeline to GitHub Packages, nuget.org and ghcr.io, Dependabot, and the release guide.
- **Phase 10 (done)**: runtime membership changes (learners, join mode, leader stickiness) and committed-only delivery.
- **Phase 11 (done)**: performance (group commit, batched apply, no per-batch state rewrite, no file watcher on the data) and the live regression script.
- **Phase 12 (done)**: Servers panel in the dashboard (members, roles, health, lag; add and remove servers; follower and standby guidance).
- **Phase 13 (done)**: server-to-server WebSocket transport, pipelined replication (opt-in), binary message bodies for push.
- **Phase 14 (done)**: binary send batches with automatic JSON fallback for older servers.
- **Phase 15 (done)**: binary journals and Raft log with checksums, converting old files in both directions.
- **Phase 16 (done)**: binary AppendEntries between servers (negotiated), append-format counters, automatic upgrade test in CI.
- **Phase 17 (done)**: server setup (scripts, start-with-Windows task, tested on a Windows runner), Dependabot updates, first GitHub release.
- **Phase 18 (done)**: Windows service (no extra package), tested install/restart/crash/stop on a Windows runner; tests on xUnit v3.
- **Phase 19 (done)**: Raft PreVote: a server cut off from the network no longer raises its term, so when it comes back it follows the
  leader again instead of forcing an election. Negotiated with peers, so mixed-version clusters keep working during an upgrade.
- **Phase 20 (done)**: compared with etcd, HashiCorp Raft, Kafka KRaft and RabbitMQ Ra; leadership transfer (CLI, API,
  dashboard, clean shutdown, self-removal) and a PreVote switch.
- **Phase 21 (done)**: measured a planned leader restart; fixed the 30 s shutdown wait; faster client retries; regression scenario.
  Tests on xUnit 4; GitHub Actions updates (checkout 7, setup-qemu 4, build-push 7).
- **Phase 22 (next)**: read-only queries (peek, stats) on followers, so dashboards and monitoring don't all load the leader.
- **Later**: a published benchmark on proper hardware.
