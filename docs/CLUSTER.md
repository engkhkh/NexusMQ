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
- `PreVote` (default `true`): ask "would you vote for me?" before an election (see below). Off gives the behaviour of 1.17.
- `TransferLeadershipOnShutdown` (default `true`): a leader that is stopped cleanly hands over first (see below).
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

## Handing over the leader role (before maintenance)

Restarting or updating the leader used to mean the others wait for an election timeout (1.5 s by default) before electing a new
leader. Hand the role over first and the pause is about one round trip:

```powershell
nexusmq --url … cluster transfer          # to the most up-to-date server
nexusmq --url … cluster transfer n2       # to a chosen server
```
Also **make leader** in the dashboard's Servers panel, `POST /api/cluster/leader/transfer?to=<id>` (broker-wide Manage) and
`Admin.TransferLeadershipAsync(...)`.

- A leader that is **stopped cleanly** (Windows service stop or restart, Ctrl+C, `docker stop`) does this by itself first
  (`TransferLeadershipOnShutdown`). A crash still means a normal election.
- A leader that **removes itself** (`cluster remove <leader>`) hands over the same way once the change is confirmed.
- How it works (Raft thesis §3.10, as in etcd, HashiCorp Raft and Ratis): the leader stops taking new writes (clients get a
  retryable 503 and retry), waits until the target has every entry, then tells it to start an election at once. The target skips
  PreVote, and the others vote for it even though they still hear from the leader. If it has not taken over within about two
  election timeouts, the transfer is dropped and the leader carries on (the call reports why).
- The target only acts on the request from its current leader, in the current term, and once its log is complete, so a late
  or repeated request cannot start an election later.
- Mixed versions: a server on 1.18 or older cannot be the target (the call says so). Older voters ignore the "hand-over" mark on
  the vote; with only older voters besides the target, the hand-over falls back to a normal election.

### What a planned restart costs (measured)

Three servers on one machine, steady traffic, the leader stopped cleanly and started again:

| | stopping took | writes paused |
|---|---|---|
| 1.18.0 | 30.1 s | 30.2 s |
| 1.19, `TransferLeadershipOnShutdown=false` | 0.22–0.23 s | 2.7–2.8 s (an election) |
| 1.19, default | 0.22–0.44 s | 0.23–0.31 s |

Before 1.19, stopping a cluster server took 30 s because the host waited for the other servers' connections to close, and a
stopping leader blocked writes all that time. 1.19 ends those connections once the hand-over is done.

## Compared with other Raft systems

How NexusMQ's elections compare with the Raft implementations behind etcd/Kubernetes, Consul/Vault/Nomad, Kafka and RabbitMQ
(checked against their documentation and source, November 2026):

| | etcd raft | HashiCorp Raft | Kafka KRaft | RabbitMQ Ra | NexusMQ 1.19 |
|---|---|---|---|---|---|
| PreVote | yes, option | yes (1.7, 2024) | yes (KIP-996) | yes | yes, on by default, option |
| Older peers during a rolling upgrade | — | counted as yes | protocol version | — | counted as yes |
| Refuse candidates while a leader is alive | yes (leader lease) | yes | yes (fetch timeout) | not checked | yes (since 1.8) |
| Leader steps down without a majority | yes (CheckQuorum) | yes | yes | not checked | yes (since 1.8) |
| Leadership transfer | yes (`etcdctl move-leader`) | yes (`LeadershipTransfer`) | not found | yes (quorum queue leaders) | yes (`cluster transfer`, 1.19) |
| Hand over on clean shutdown | yes (leader's `Stop`) | left to the application | — | yes (maintenance mode) | yes (1.19) |
| Transfer skips PreVote and the leader check | yes | yes (flag on the vote) | — | — | yes (1.19) |

Fixed or checked in 1.19 because others hit them:
- **Leadership transfer** was the main gap. Done as above.
- **A late TimeoutNow** (raft-rs issue 234): a transfer request arriving after the transfer was given up could start an election
  out of turn. NexusMQ's target checks term, sender and log first.
- **Writes during a transfer** (HashiCorp Raft PR 581): writes accepted while handing over could make the transfer fail. NexusMQ
  refuses them with a retryable error until the transfer ends either way.
- **Shorter-log candidate vs longer-log PreVoter** (RabbitMQ Ra issue 439): the two could block each other forever. NexusMQ was
  not affected (the candidate with the shorter log learns the higher term and steps aside); a test now keeps it that way.
- **PreVote switch:** etcd, Ratis and openraft let operators turn PreVote off; NexusMQ now does too (`PreVote`).

Not adopted: lease-based reads (etcd's leader lease for reads): NexusMQ answers reads from the leader's memory and confirms writes
through the log, so it has no read path that needs a lease.

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
- **A server that was cut off rejoins quietly.** While it cannot reach a majority it only asks whether it *could* win an election
  (Raft PreVote) and never raises its term, so when the network comes back it follows the current leader and nobody notices. Servers
  agree on this when they connect; an older server in the cluster during an upgrade is treated as "yes", which is how it worked before.
  Each server's `preVoteRounds` and `electionsStarted` are in `GET /api/replication/status` (under `cluster`).
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
- **Leadership transfer, three real servers:** over the WebSocket and the HTTP link, by the API and by a clean leader shutdown:
  the chosen server takes over with exactly one election and the client follows it with no error.
- **Network cut (PreVote), three real servers:** one follower was cut off from the other two for 10 s and then reconnected (TCP proxy
  stopped and restarted). See the before/after numbers in `docs/PLAN.md` (Phase 19).
- **In-process tests:** fake-network partitions show a cut-off leader cannot confirm writes and steps down, its unconfirmed write is discarded
  after healing, and a node that fell behind catches up through a snapshot.
