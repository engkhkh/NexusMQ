# Raft cluster (3 or 5 servers)

In cluster mode NexusMQ servers elect a **leader** with the Raft consensus algorithm. The leader serves all clients. Every change
(a sent message, a completion, a new queue, a new key…) becomes a Raft log entry, and **the client gets its "OK" only once a majority
of servers have stored it**. Every server applies committed entries to its own storage.

What this fixes compared with the primary/standby pair (`docs/HA.md`):

| | Primary + standby | Raft cluster |
|---|---|---|
| Confirmed message lost when a server crashes | possible (tiny window) | no, as long as a majority survives |
| Two servers both acting as leader (split brain) | possible with auto-failover | no: a leader needs a majority, and a cut-off leader steps down |
| Servers needed | 2 | 3 (survives 1 failure) or 5 (survives 2) |
| Failover | after `FailoverAfterSeconds` | automatic, ~1–3 s |

## Configuration

Every server gets the same member list, the same cluster secret and the same `ApiKey`/`AccessKeys`, plus its own `NodeId` and data directory:
```json
"NexusMQ": {
  "ApiKey": "secret",
  "DataDirectory": "data",
  "Cluster": {
    "NodeId": "n1",
    "Secret": "long-random-cluster-secret",
    "Nodes": [
      { "Id": "n1", "Url": "http://mq-1:5680" },
      { "Id": "n2", "Url": "http://mq-2:5680" },
      { "Id": "n3", "Url": "http://mq-3:5680" }
    ]
  }
}
```
- `Url` is how the *other servers* reach this one. Members talk to each other on `/api/raft/*`, authenticated with `Secret`.
- Storage must be durable (the default). In-memory mode cannot be clustered.
- Optional tuning: `ElectionTimeoutMs` (1500), `HeartbeatMs` (250), `CommitTimeoutMs` (5000), and `MaxLogEntries` (50,000; the log is compacted
  into the store beyond this, and servers that fall further behind receive a snapshot).
- `Transport` (`WebSocket`, the default, or `Http`): how members talk to each other. The WebSocket link is one long-lived connection per peer
  and needs WebSocket upgrades through anything between servers (proxies, load balancers). Members on an older version are reached over HTTP
  automatically, so a rolling upgrade one server at a time works.
- `MaxInflightAppends` (1): replication batches in flight per follower. Raise it to 4 only for servers far apart (tens of ms) with spare CPU;
  on nearby servers it measured slower.
- Use an **odd** number of servers. 3 tolerates one failure and 5 tolerates two. 4 tolerates only one, same as 3.
- `DeliverOnlyCommitted` (default `true`): consumers receive a message only after a majority has stored it. Set it to `false` to shave a few
  milliseconds of latency, at the cost of a consumer possibly seeing a message whose send then fails.

`Nodes` is only the **starting** membership. After the first election the membership lives in the Raft log and is changed at runtime (below).

Docker: `docker compose -f docker-compose.cluster.yml up -d --build` starts n1, n2 and n3 on ports 5680–5682.

## Adding and removing servers (no restart, no downtime)

Change membership **one server at a time**:

```powershell
# 1. Start the new server in join mode (no Nodes list needed):
#    NexusMQ:Cluster:NodeId=n4, NexusMQ:Cluster:Secret=<same secret>, NexusMQ:Cluster:Join=true, plus the same ApiKey
# 2. Add it (run against any server; followers redirect to the leader):
nexusmq --url http://mq-1:5680,http://mq-2:5680,http://mq-3:5680 --key … cluster add n4 http://mq-4:5680
# 3. To retire a server, remove it, then shut it down:
nexusmq --url … cluster remove n1
nexusmq --url … cluster members
```
- The new server first catches up as a **non-voting learner**: it receives the log, or a snapshot if it is far behind. Only once it is close does it
  become a voter, so adding it never stalls commits. If it can't catch up within 60 s (wrong secret, not reachable, not in join mode), the add
  fails and nothing changes.
- **Removing the leader** is allowed. It keeps leading until the change commits, then steps aside and the others elect a new leader in about 1 s.
- A removed server goes quiet: it no longer starts elections, and the remaining servers ignore its vote requests while they hear from their leader,
  so a forgotten server can't disrupt the cluster.
- API: `GET/POST /api/cluster/members`, `DELETE /api/cluster/members/{id}` (broker-wide Manage).
  Client: `Admin.AddClusterMemberAsync` / `RemoveClusterMemberAsync` / `GetClusterMembersAsync`.
- Clients find new servers through the leader hint, but they only fail over to URLs they know, so add new servers' URLs to your apps' configuration.

## Dashboard

Open any server's dashboard (`http://<server>:5680/`). The **Servers** panel at the top shows the cluster:
- **On the leader:** each server with its role, health (green ok, amber slow, red unreachable), how many log entries it is behind, and when it last
  answered. The panel also shows how many voters are reachable against the majority, in amber when one more failure would stop writes.
  **Add server** and **remove** do the same as `nexusmq cluster add/remove` (remove the leader itself to hand over before maintenance).
- **On a follower:** a link to the leader's dashboard. Messages and servers are managed there.
- **On a removed server:** a note that it is no longer a member, with links to the members.

The panel needs a key with Monitor rights (Manage to add or remove servers).

## Clients

Give clients all the URLs:
```csharp
builder.Services.AddNexusMQClient("http://mq-1:5680,http://mq-2:5680,http://mq-3:5680", key);
```
Followers answer `503` with an `X-NexusMQ-Leader` header, and clients jump straight to the leader. During an election (about 1–3 s)
calls wait and retry (`FailoverWaitTime`, default 30 s with several URLs). Push consumers reconnect to the new leader automatically.

`nexusmq --url … replication status` shows each server's role, term, leader and log, commit and applied positions.
`GET /api/health` returns `{"role": "Leader" | "Follower", "leader": "<url>"}` for load balancers.

## Guarantees and limits

- **Confirmed means majority-stored.** A send, completion or any management call that returned OK survives the loss of any minority of servers.
- **No majority, no writes.** If too few servers are up, writes are refused with `503` (`Standby` or `NotCommitted`). Clients see a clear error
  rather than a silent loss.
- **At-least-once.** If the leader dies after storing a change but before answering, the client gets `NotCommitted` and retries, so the change can
  happen twice. Messages being processed when the leader died are redelivered. Use `MessageId` with duplicate detection where repeats matter.
- **Consumers only see confirmed messages** (with the default `DeliverOnlyCommitted`). The dashboard's peek view and stats read the leader's
  memory and can show a send a few milliseconds before it is confirmed.
- **Membership changes go one server at a time** and need a working majority, both before and after the change.
- Message locks and push subscriptions live on the leader only. After a leader change, unsettled messages become available again.

## Performance

3-node cluster on one 2-core test machine (all three servers and the benchmark share the CPU, so these are relative numbers):

| | 1.8 | 1.9 |
|---|---|---|
| Push (WebSocket) throughput | 2,200–2,500 msg/s | 3,700–4,000 msg/s |
| Long-polling throughput | 1,800–2,300 msg/s | 2,400–2,600 msg/s |
| Push throughput with `FlushToDisk` | 1,450–1,550 msg/s | 2,150–2,700 msg/s |
| Median send→deliver latency | 2.4–4.3 ms | 1.7–3 ms |

A single server on the same machine does 11,000–13,000 msg/s over WebSocket.

1.14 (Phase 16) sends message bodies between servers as raw bytes (negotiated, so mixed versions work). `appendsReceived` in
`nexusmq replication status` / `GET /api/replication/status` shows whether a server gets binary, socket-JSON or HTTP appends.

1.13 (Phase 15) stores journals and the Raft log as binary records: about 24% less disk for 16 KB messages, p99 latency 11–26 → 7–8 ms.

1.11 (Phase 13) adds the server-to-server WebSocket link: +5–15% throughput and about 25% lower latency when servers are ~1 ms apart.
Large messages are much faster to consume (binary bodies: 16 KB about 5×, 64 KB about 4×). See `docs/PLAN.md` for the measurements.

What 1.9 changed (Phase 11, found by profiling with strace and per-step timing):
- **Group commit.** The leader buffers new log entries and a background flusher writes (and, with `FlushToDisk`, fsyncs) everything
  pending in one go, outside the Raft lock. The leader counts itself towards a majority only for entries already written. A follower writes
  all entries of an AppendEntries call at once before acknowledging. One fsync per batch instead of one per message is the big win on real
  disks, where an fsync takes milliseconds.
- **Batched store writes.** Committed entries are applied to the message store with one flush per batch. The store is always flushed before the
  applied index is saved and before the log is compacted.
- **No state-file rewrite per batch.** The applied index (a restart hint only: replaying applied entries is harmless) is saved every 500 entries or once a
  second, not after every batch.
- **No config file watcher on the data.** The standalone server no longer reloads appsettings.json. That watcher covers the whole content root, which by
  default holds the `data` folder, so every journal write woke it. Embedded brokers log a warning when their data folder is inside a watched content root.
- `GET /api/replication/status` now shows `appendCallsSent` and `appendEntriesSent` on the leader (entries per call shows how well replication batches).

Each confirmed write still costs one round trip to a majority. Throughput comes from many requests sharing the same commits, so use several
concurrent senders and consumers (`MaxConcurrentCalls`, `PrefetchCount`).

## Tested

- **Hard-killed leader under steady traffic:** a new leader was elected in about 3 s. All 889 confirmed messages were delivered, with none missing, no duplicates and zero
  client errors.
- **Restarted node:** it caught up to the identical log.
- **Two of three killed:** the survivor refused writes, and the refused message never appeared later. With a majority back, writes resumed with all data intact.
- **Membership changes under steady traffic:** added a 4th server, had the leader remove itself, then hard-killed that old leader. All 1,044 confirmed
  messages were delivered, with none missing, no duplicates and zero client errors, and the remaining servers' logs were identical.
- **In-process tests:** fake-network partitions show a cut-off leader cannot confirm writes and steps down, its unconfirmed write is discarded
  after healing, and a node that fell behind catches up through a snapshot.
