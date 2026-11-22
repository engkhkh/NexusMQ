using System.Collections.Concurrent;
using System.Net.WebSockets;
using System.Text.Json;
using System.Text.Json.Serialization;
using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using NexusMQ.Core;
using NexusMQ.Core.Cluster;
using NexusMQ.Core.Persistence;
using NexusMQ.Core.Replication;

namespace NexusMQ.AspNetCore;

public enum NexusRole
{
    /// <summary>Serves clients and streams its changes to standbys.</summary>
    Primary,
    /// <summary>Keeps a live copy of a primary and refuses client traffic (HTTP 503) until promoted.</summary>
    Standby
}

/// <summary>High-availability settings (section <c>NexusMQ:Replication</c>).</summary>
public sealed class NexusReplicationOptions
{
    public NexusRole Role { get; set; } = NexusRole.Primary;
    /// <summary>Standby only: the primary's base URL, e.g. http://nexusmq-a:5680.</summary>
    public string? PrimaryUrl { get; set; }
    /// <summary>Standby only: an access key on the primary with broker-wide Manage rights (not needed when the primary is open).</summary>
    public string? PrimaryKey { get; set; }
    /// <summary>Standby only: promote itself when the primary has been silent for <see cref="FailoverAfterSeconds"/>.</summary>
    public bool AutoFailover { get; set; }
    public double FailoverAfterSeconds { get; set; } = 15;
    /// <summary>Primary only: how many recent changes are kept for standbys that briefly disconnect.</summary>
    public int LogCapacity { get; set; } = 200_000;
    /// <summary>Development only: accept the primary's self-signed TLS certificate.</summary>
    public bool AllowUntrustedCertificates { get; set; }
}

public sealed class StandbyConnectionInfo
{
    public string? RemoteIp { get; init; }
    public DateTimeOffset ConnectedUtc { get; init; }
    public long SentPosition { get; set; }
    public DateTimeOffset LastSentUtc { get; set; }
}

public sealed class ReplicationStatus
{
    public NexusRole Role { get; init; }
    public long Position { get; init; }
    // standby side
    public string? PrimaryUrl { get; init; }
    public bool ConnectedToPrimary { get; init; }
    public DateTimeOffset? LastContactUtc { get; init; }
    public double? SecondsSinceContact { get; init; }
    public long? PrimaryPosition { get; init; }
    public bool AutoFailover { get; init; }
    public DateTimeOffset? PromotedUtc { get; init; }
    // primary side
    public List<StandbyConnectionInfo> Standbys { get; init; } = [];
    // cluster mode
    public RaftStatus? Cluster { get; init; }
    public string? LeaderUrl { get; init; }
}

/// <summary>
/// This server's place in a primary/standby pair. Owns the <see cref="Broker"/> on a primary; on a standby it applies the
/// primary's replication stream to local storage and creates the broker only when promoted.
/// </summary>
public sealed class NexusNode : IDisposable
{
    private readonly IMessageStore _store;
    private readonly NexusAuthorizer _auth;
    private readonly ILogger _log;
    private readonly Lock _gate = new();
    private Broker? _broker;
    private ReplicationLog? _replicationLog;

    internal readonly SemaphoreSlim ApplyGate = new(1, 1);
    internal NexusReplicationOptions Options { get; }
    internal ConcurrentDictionary<Guid, StandbyConnectionInfo> Standbys { get; } = new();

    // standby progress
    internal long AppliedPosition;
    internal long? PrimaryPosition;
    internal DateTimeOffset? LastContactUtc;
    internal bool ConnectedToPrimary;
    internal bool HasSynced;
    private DateTimeOffset? _promotedUtc;

    public NexusRole Role { get; private set; }

    // ------------------------------------------------------------------ cluster mode (Raft)
    internal NexusClusterOptions ClusterOptions { get; }
    public RaftNode? Raft { get; }
    private readonly RaftStore? _raftStore;
    private readonly IRaftTransport? _transport;

    /// <summary>Raised when this node starts or stops serving clients (leader elected, leadership lost, promotion).</summary>
    public event Action<NexusRole>? RoleChanged;

    public bool IsCluster => Raft is not null;

    /// <summary>"Primary"/"Standby", or "Leader"/"Follower" in a cluster.</summary>
    public string RoleName => IsCluster ? (Role == NexusRole.Primary ? "Leader" : "Follower") : Role.ToString();

    /// <summary>The URL clients should use instead of this node (the leader / primary), when known.</summary>
    public string? LeaderUrl
    {
        get
        {
            if (Raft is null) return Role == NexusRole.Standby ? Options.PrimaryUrl : null;
            var id = Raft.LeaderId;
            return id is null ? null : Raft.Members.FirstOrDefault(n => n.Id == id)?.Url;
        }
    }

    public TimeSpan CommitTimeout => TimeSpan.FromMilliseconds(ClusterOptions.CommitTimeoutMs);

    public NexusNode(NexusServerOptions options, IMessageStore store, NexusAuthorizer auth, ILogger<NexusNode> log, string? dataDirectory = null)
    {
        Options = options.Replication;
        ClusterOptions = options.Cluster;
        _store = store;
        _auth = auth;
        _log = log;
        Role = Options.Role;

        if (ClusterOptions.Enabled)
        {
            if (!options.Durable || dataDirectory is null)
                throw new InvalidOperationException("A NexusMQ cluster needs durable storage (NexusMQ:Durable = true).");
            if (string.IsNullOrWhiteSpace(ClusterOptions.Secret))
                throw new InvalidOperationException("NexusMQ:Cluster:Secret is required: members authenticate each other with it.");
            if (string.IsNullOrWhiteSpace(ClusterOptions.NodeId))
                throw new InvalidOperationException("NexusMQ:Cluster:NodeId is required.");
            if (!ClusterOptions.Join && ClusterOptions.Nodes.All(n => n.Id != ClusterOptions.NodeId))
                throw new InvalidOperationException($"NexusMQ:Cluster:NodeId '{ClusterOptions.NodeId}' is not one of NexusMQ:Cluster:Nodes (set NexusMQ:Cluster:Join=true for a server being added).");

            Role = NexusRole.Standby; // until elected leader
            _transport = ClusterOptions.Transport.Equals("Http", StringComparison.OrdinalIgnoreCase)
                ? new HttpRaftTransport(ClusterOptions)
                : new WebSocketRaftTransport(ClusterOptions);
            Raft = new RaftNode(ClusterOptions.NodeId!, ClusterOptions.Nodes,
                new RaftLog(Path.Combine(dataDirectory, "raft"), options.FlushToDisk, options.StorageFormat), store, _transport,
                new RaftOptions
                {
                    ElectionTimeout = TimeSpan.FromMilliseconds(ClusterOptions.ElectionTimeoutMs),
                    HeartbeatInterval = TimeSpan.FromMilliseconds(ClusterOptions.HeartbeatMs),
                    MaxLogEntries = ClusterOptions.MaxLogEntries,
                    MaxInflightAppends = ClusterOptions.MaxInflightAppends,
                    JoinExisting = ClusterOptions.Join
                })
            {
                KeysApplied = json => _auth.ReplaceKeys(json),
                ExportKeys = () => _auth.ExportKeysJson()
            };
            _raftStore = new RaftStore(store, Raft);
            Raft.LeadershipChanged += OnLeadershipChanged;
            _auth.KeysChanged += json =>
            {
                if (Role == NexusRole.Primary) _raftStore.ProposeKeys(json);
            };
            Raft.Start();
            return;
        }
        if (Role == NexusRole.Standby && string.IsNullOrWhiteSpace(Options.PrimaryUrl))
            throw new InvalidOperationException("NexusMQ:Replication:PrimaryUrl is required when Role is Standby.");

        _auth.KeysChanged += json => _replicationLog?.Append(new ReplicationOp { Kind = ReplicationOpKind.Keys, Payload = json });
        if (Role == NexusRole.Primary) StartAsPrimary();
    }

    private void StartAsPrimary()
    {
        _replicationLog = new ReplicationLog(Options.LogCapacity);
        _broker = new Broker(new ReplicatingMessageStore(_store, _replicationLog));
    }

    /// <summary>The broker (primary only). Throws Standby on a standby.</summary>
    public Broker Broker => _broker ?? throw new NexusException(NexusErrorCode.Standby,
        $"This node is a standby (primary: {Options.PrimaryUrl}). Send requests to the primary.");

    internal ReplicationLog? ReplicationLog => _replicationLog;
    internal IMessageStore Store => _store;
    internal NexusAuthorizer Authorizer => _auth;

    private void OnLeadershipChanged(bool leading)
    {
        if (leading)
        {
            lock (_gate)
            {
                // The local store holds every committed change (applied by Raft): start the broker on it.
                var raft = Raft!;
                _broker = new Broker(_raftStore!, new BrokerOptions
                {
                    CommittedIndex = ClusterOptions.DeliverOnlyCommitted ? () => raft.CommitIndex : null
                });
                Role = NexusRole.Primary;
                if (ClusterOptions.DeliverOnlyCommitted) _ = Task.Run(() => WakeReceiversOnCommitAsync(_broker));
            }
            _log.LogWarning("NexusMQ node {Node} is now the cluster LEADER (term {Term})", ClusterOptions.NodeId, Raft!.CurrentTerm);
            RoleChanged?.Invoke(NexusRole.Primary);
        }
        else
        {
            Broker? old;
            lock (_gate)
            {
                old = _broker;
                _broker = null;
                Role = NexusRole.Standby;
            }
            _log.LogWarning("NexusMQ node {Node} lost cluster leadership; now a follower", ClusterOptions.NodeId);
            RoleChanged?.Invoke(NexusRole.Standby);
            // Let in-flight requests fail on their own (they cannot commit), then release the old broker.
            if (old is not null) _ = Task.Delay(TimeSpan.FromSeconds(10)).ContinueWith(_ => old.Dispose());
        }
    }

    /// <summary>While <paramref name="broker"/> is ours: each time the cluster commits more, wake receivers waiting for those messages.</summary>
    private async Task WakeReceiversOnCommitAsync(Broker broker)
    {
        var raft = Raft!;
        var seen = raft.CommitIndex;
        while (ReferenceEquals(_broker, broker))
        {
            // Take the signal BEFORE reading the index: an advance after this point completes the task, and an advance
            // before it shows up in the index. Either way none is missed (a missed one would leave a message undelivered).
            var changed = raft.CommitChanged();
            if (raft.CommitIndex == seen)
            {
                try { await changed.WaitAsync(TimeSpan.FromSeconds(1)); } catch (TimeoutException) { }
            }
            var now = raft.CommitIndex;
            if (now != seen)
            {
                seen = now;
                broker.NotifyCommitted();
            }
        }
    }

    /// <summary>Make this standby the primary: stop following, load the replicated state, start serving.</summary>
    public void Promote()
    {
        if (IsCluster)
            throw new NexusException(NexusErrorCode.BadRequest, "In a cluster the leader is elected automatically; promotion is not used.");
        ApplyGate.Wait();
        try
        {
            lock (_gate)
            {
                if (Role == NexusRole.Primary) return;
                StartAsPrimary();
                Role = NexusRole.Primary;
                _promotedUtc = DateTimeOffset.UtcNow;
            }
            _log.LogWarning("NexusMQ standby promoted to PRIMARY at replicated position {Position}", AppliedPosition);
            RoleChanged?.Invoke(NexusRole.Primary);
        }
        finally
        {
            ApplyGate.Release();
        }
    }

    public ReplicationStatus Status()
    {
        var now = DateTimeOffset.UtcNow;
        return new ReplicationStatus
        {
            Role = Role,
            Position = Role == NexusRole.Primary ? _replicationLog?.Position ?? 0 : AppliedPosition,
            PrimaryUrl = Options.PrimaryUrl,
            ConnectedToPrimary = Role == NexusRole.Standby && ConnectedToPrimary,
            LastContactUtc = LastContactUtc,
            SecondsSinceContact = LastContactUtc is { } c ? Math.Round((now - c).TotalSeconds, 1) : null,
            PrimaryPosition = PrimaryPosition,
            AutoFailover = Options.AutoFailover,
            PromotedUtc = _promotedUtc,
            Standbys = [.. Standbys.Values],
            Cluster = Raft?.Status(),
            LeaderUrl = LeaderUrl
        };
    }

    public void Dispose()
    {
        Raft?.Dispose();
        (_transport as IDisposable)?.Dispose();
        _broker?.Dispose();
        if (_broker is null || IsCluster) _store.Dispose();
    }
}

/// <summary>Primary side of the replication stream: <c>{prefix}/replication/stream</c> (WebSocket, broker-wide Manage key).</summary>
internal static class NexusReplicationStream
{
    internal static readonly JsonSerializerOptions Json = new(JsonSerializerDefaults.Web)
    {
        DefaultIgnoreCondition = JsonIgnoreCondition.WhenWritingNull,
        Converters = { new JsonStringEnumConverter() }
    };

    public static async Task HandleAsync(HttpContext http)
    {
        var node = http.RequestServices.GetRequiredService<NexusNode>();
        var auth = http.RequestServices.GetRequiredService<NexusAuthorizer>();
        try
        {
            auth.Demand(auth.Authenticate(NexusAuthorizer.PresentedKey(http)), NexusRights.Manage, null);
        }
        catch (NexusException ex)
        {
            http.RequestServices.GetRequiredService<NexusAuditLog>().Record("unknown", "replication connect", null, "denied", ex.Message,
                http.Connection.RemoteIpAddress?.ToString());
            http.Response.StatusCode = ex.Code == NexusErrorCode.Unauthorized ? 401 : 403;
            return;
        }
        if (node.Role != NexusRole.Primary || node.ReplicationLog is null)
        {
            http.Response.StatusCode = StatusCodes.Status503ServiceUnavailable;
            return;
        }
        if (!http.WebSockets.IsWebSocketRequest)
        {
            http.Response.StatusCode = StatusCodes.Status400BadRequest;
            return;
        }

        using var ws = await http.WebSockets.AcceptWebSocketAsync();
        var log = node.ReplicationLog;
        var id = Guid.NewGuid();
        var info = new StandbyConnectionInfo { RemoteIp = http.Connection.RemoteIpAddress?.ToString(), ConnectedUtc = DateTimeOffset.UtcNow };
        node.Standbys[id] = info;
        var ct = http.RequestAborted;
        try
        {
            // Read loop only to notice the standby closing the socket.
            var closed = Task.Run(async () =>
            {
                var buf = new byte[1024];
                while (ws.State == WebSocketState.Open && !ct.IsCancellationRequested)
                    if ((await ws.ReceiveAsync(buf, ct)).MessageType == WebSocketMessageType.Close) return;
            }, ct);

            var after = long.TryParse(http.Request.Query["from"], out var from) ? from : -1;
            List<ReplicationOp>? ops = null;
            if (after >= 0) ops = log.ReadAfter(after, 500, out _);
            if (after < 0 || ops is null) after = await SendSnapshotAsync(ws, node, ct);

            var lastSend = DateTimeOffset.UtcNow;
            while (ws.State == WebSocketState.Open && !closed.IsCompleted && !ct.IsCancellationRequested)
            {
                ops = log.ReadAfter(after, 500, out var wake);
                if (ops is null)
                {
                    after = await SendSnapshotAsync(ws, node, ct); // fell out of the ring: start over
                    continue;
                }
                if (ops.Count > 0)
                {
                    await SendAsync(ws, new { type = "ops", ops, position = log.Position }, ct);
                    after = ops[^1].Position;
                    info.SentPosition = after;
                    info.LastSentUtc = lastSend = DateTimeOffset.UtcNow;
                    continue;
                }
                if (DateTimeOffset.UtcNow - lastSend >= TimeSpan.FromSeconds(2))
                {
                    await SendAsync(ws, new { type = "heartbeat", position = log.Position, utc = DateTimeOffset.UtcNow }, ct);
                    info.LastSentUtc = lastSend = DateTimeOffset.UtcNow;
                }
                try { await wake.WaitAsync(TimeSpan.FromSeconds(1), ct); } catch (TimeoutException) { }
            }
        }
        catch (OperationCanceledException) { }
        catch (WebSocketException) { }
        finally
        {
            node.Standbys.TryRemove(id, out _);
        }
    }

    private static async Task<long> SendSnapshotAsync(WebSocket ws, NexusNode node, CancellationToken ct)
    {
        var position = node.ReplicationLog!.Position;
        var snapshot = node.Broker.ExportState(position);
        snapshot.KeysPayload = node.Authorizer.ExportKeysJson();
        await SendAsync(ws, new { type = "snapshot", snapshot }, ct);
        return position;
    }

    internal static Task SendAsync(WebSocket ws, object payload, CancellationToken ct) =>
        ws.SendAsync(JsonSerializer.SerializeToUtf8Bytes(payload, Json), WebSocketMessageType.Text, true, ct);
}

/// <summary>Standby side: follows the primary's stream into local storage, and promotes on silence when AutoFailover is on.</summary>
internal sealed class NexusStandbyService(NexusNode node, ILogger<NexusStandbyService> log) : BackgroundService
{
    private sealed class Frame
    {
        public string Type { get; set; } = "";
        public ReplicationSnapshot? Snapshot { get; set; }
        public List<ReplicationOp>? Ops { get; set; }
        public long? Position { get; set; }
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        if (node.Role != NexusRole.Standby || node.IsCluster) return;
        var opts = node.Options;
        using var watchdog = opts.AutoFailover ? Task.Run(() => WatchdogAsync(stoppingToken), stoppingToken) : null;

        while (!stoppingToken.IsCancellationRequested && node.Role == NexusRole.Standby)
        {
            try
            {
                await FollowAsync(stoppingToken);
            }
            catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
            {
                return;
            }
            catch (Exception ex)
            {
                if (node.ConnectedToPrimary) log.LogWarning("NexusMQ standby lost the primary: {Error}", ex.Message);
            }
            finally
            {
                node.ConnectedToPrimary = false;
            }
            if (node.Role != NexusRole.Standby) return;
            try { await Task.Delay(TimeSpan.FromSeconds(1), stoppingToken); } catch (OperationCanceledException) { return; }
        }
    }

    private async Task FollowAsync(CancellationToken ct)
    {
        var opts = node.Options;
        var baseUri = new Uri(opts.PrimaryUrl!.TrimEnd('/') + "/");
        var ub = new UriBuilder(new Uri(baseUri, "api/replication/stream"))
        {
            Scheme = baseUri.Scheme == Uri.UriSchemeHttps ? "wss" : "ws",
            Query = $"from={(node.HasSynced ? node.AppliedPosition : -1)}"
        };
        using var ws = new ClientWebSocket();
        ws.Options.KeepAliveInterval = TimeSpan.FromSeconds(10);
        if (!string.IsNullOrEmpty(opts.PrimaryKey)) ws.Options.SetRequestHeader("X-Api-Key", opts.PrimaryKey);
        using var invoker = new HttpMessageInvoker(NexusMQ.Client.NexusConnect.CreateHandler(opts.AllowUntrustedCertificates), disposeHandler: true);
        await ws.ConnectAsync(ub.Uri, invoker, ct); // races IPv6/IPv4 addresses (see NexusConnect)
        node.ConnectedToPrimary = true;
        node.LastContactUtc = DateTimeOffset.UtcNow;
        log.LogInformation("NexusMQ standby connected to primary {Primary}", opts.PrimaryUrl);

        var buffer = new byte[256 * 1024];
        while (ws.State == WebSocketState.Open && !ct.IsCancellationRequested && node.Role == NexusRole.Standby)
        {
            using var ms = new MemoryStream();
            WebSocketReceiveResult r;
            using (var silence = CancellationTokenSource.CreateLinkedTokenSource(ct))
            {
                silence.CancelAfter(TimeSpan.FromSeconds(Math.Max(5, opts.FailoverAfterSeconds)));
                do
                {
                    r = await ws.ReceiveAsync(buffer, silence.Token);
                    if (r.MessageType == WebSocketMessageType.Close) return;
                    ms.Write(buffer, 0, r.Count);
                } while (!r.EndOfMessage);
            }
            node.LastContactUtc = DateTimeOffset.UtcNow;
            var frame = JsonSerializer.Deserialize<Frame>(ms.GetBuffer().AsSpan(0, (int)ms.Length), NexusReplicationStream.Json)!;

            await node.ApplyGate.WaitAsync(ct);
            try
            {
                if (node.Role != NexusRole.Standby) return; // promoted meanwhile: stop applying
                switch (frame.Type)
                {
                    case "snapshot":
                        ReplicationApplier.ApplySnapshot(node.Store, frame.Snapshot!);
                        node.Authorizer.ReplaceKeys(frame.Snapshot!.KeysPayload);
                        node.AppliedPosition = frame.Snapshot.Position;
                        node.HasSynced = true;
                        log.LogInformation("NexusMQ standby loaded snapshot at position {Position} ({Entities} entities)",
                            frame.Snapshot.Position, frame.Snapshot.Entities.Count);
                        break;
                    case "ops":
                        foreach (var op in frame.Ops!)
                        {
                            if (op.Position <= node.AppliedPosition && node.HasSynced) continue;
                            if (ReplicationApplier.Apply(node.Store, op) is { } keys) node.Authorizer.ReplaceKeys(keys);
                            node.AppliedPosition = op.Position;
                        }
                        node.PrimaryPosition = frame.Position;
                        break;
                    case "heartbeat":
                        node.PrimaryPosition = frame.Position;
                        break;
                }
            }
            finally
            {
                node.ApplyGate.Release();
            }
        }
    }

    private async Task WatchdogAsync(CancellationToken ct)
    {
        var limit = TimeSpan.FromSeconds(node.Options.FailoverAfterSeconds);
        while (!ct.IsCancellationRequested && node.Role == NexusRole.Standby)
        {
            await Task.Delay(TimeSpan.FromMilliseconds(500), ct);
            // Only fail over once we have a copy worth serving; never promote an empty standby.
            if (node.HasSynced && node.LastContactUtc is { } last && DateTimeOffset.UtcNow - last > limit)
            {
                log.LogWarning("NexusMQ primary silent for {Seconds:0}s: automatic failover", (DateTimeOffset.UtcNow - last).TotalSeconds);
                node.Promote();
                return;
            }
        }
    }
}
