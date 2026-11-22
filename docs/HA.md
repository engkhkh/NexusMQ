# High availability: primary + standby

NexusMQ can run as a **pair**:
- The **primary** serves all clients and streams every change to the standby: queues, topics, messages, dead letters, session state and runtime access keys.
- The **standby** keeps a live copy, answers `503 Standby` to client traffic, and becomes the primary on failover.

Clients that know both addresses switch automatically.

## Setup

**Primary** (`appsettings.json`, or environment variables `NexusMQ__…`):
```json
"NexusMQ": { "ApiKey": "secret", "Replication": { "Role": "Primary" } }
```

**Standby:**
```json
"NexusMQ": {
  "ApiKey": "secret",
  "Replication": {
    "Role": "Standby",
    "PrimaryUrl": "http://mq-a:5680",
    "PrimaryKey": "secret",
    "AutoFailover": true,
    "FailoverAfterSeconds": 15
  }
}
```
- `PrimaryKey` must be a key on the primary with broker-wide **Manage** rights. The replication stream carries everything, including key hashes.
- **Config keys (`ApiKey`, `AccessKeys`) are not replicated.** Give both servers the same ones. Runtime keys (created with `nexusmq keys create`) are replicated.
- Each server needs its own `DataDirectory`.

Docker: `docker compose -f docker-compose.ha.yml up -d --build` starts `nexusmq-a` (primary, port 5680) and `nexusmq-b` (standby, port 5682).

## Clients

Give every client both URLs, comma-separated:
```csharp
builder.Services.AddNexusMQClient("http://mq-a:5680,http://mq-b:5680", key);
```
```
nexusmq --url http://mq-a:5680,http://mq-b:5680 --key … stats
```
When a server is unreachable or answers as a standby, the client moves to the next one. HTTP calls and push (WebSocket) connections both do this.
While a failover is under way (no server is primary yet), calls keep retrying for up to `NexusClientOptions.FailoverWaitTime` (default 30 s with
several URLs) instead of failing. Processors reconnect by themselves.

## Failover

- **Automatic:** with `AutoFailover: true`, the standby promotes itself after `FailoverAfterSeconds` without hearing from the primary.
  It only does this if it has received at least one full copy, so an empty standby never takes over.
- **Manual:** `nexusmq --url http://mq-b:5680 --key … replication promote`, or `POST /api/replication/promote` on the standby.
- **Check:** `nexusmq --url http://mq-a:5680,http://mq-b:5680 replication status` shows the role, position and connection of each server.
  The dashboard header shows the role, and `GET /api/health` returns `{"role": "Primary" | "Standby"}`, which load balancers can check.

**After a failover**, bring the old primary back **as a standby of the new primary**. Set `Role: Standby` and `PrimaryUrl` to the
new primary, and give it an empty data directory or simply restart it. It loads a full snapshot and follows from there. Never start it
again as `Primary` while the other server is primary.

## What is guaranteed, and what is not

- **Replication is asynchronous.** The primary confirms a send before the standby has it. If the primary crashes, the standby has
  everything up to its last received change, which is normally milliseconds behind. Messages confirmed in that last instant can be lost.
  In tests, hard-killing the primary under steady traffic lost nothing across two failovers (about 1,800 messages), but the window exists.
- **Delivery is at-least-once.** A message being processed when the primary died is redelivered by the new primary. A request sent just as a
  server failed can be retried on the other one. Use `MessageId` with `DuplicateDetectionWindow` on queues where duplicates matter.
- **Split brain.** Automatic failover cannot tell "the primary crashed" from "the network between them broke". If the network splits
  while the primary keeps running, both servers can act as primary for a while. Use automatic failover only when both servers are on the
  same reliable network, or turn it off and promote by hand (or from your monitoring) once you have confirmed the primary is really down.
- Locks and in-flight push deliveries are not replicated. After failover, unsettled messages simply become available again.

## Monitoring

- Primary: `standbys` in `GET /api/replication/status` lists connected standbys and how far they have been sent.
- Standby: compare `position` with `primaryPosition`, and watch `secondsSinceContact`.
- Alert when a standby has been disconnected for longer than you are comfortable running without a spare.
