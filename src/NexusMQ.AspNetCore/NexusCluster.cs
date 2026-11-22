using System.Net.Http.Json;
using System.Security.Cryptography;
using System.Text;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Routing;
using Microsoft.Extensions.DependencyInjection;
using NexusMQ.Core.Cluster;

namespace NexusMQ.AspNetCore;

/// <summary>Raft cluster settings (section <c>NexusMQ:Cluster</c>). A cluster is on when <see cref="Nodes"/> is not empty.</summary>
public sealed class NexusClusterOptions
{
    /// <summary>This server's id; must be one of <see cref="Nodes"/>.</summary>
    public string? NodeId { get; set; }
    /// <summary>Every member (this one included), each with an id and the URL the others reach it on. Use 3 or 5.</summary>
    public List<ClusterMember> Nodes { get; set; } = [];
    /// <summary>Shared secret the members use to talk to each other. Required.</summary>
    public string? Secret { get; set; }
    public int ElectionTimeoutMs { get; set; } = 1500;
    public int HeartbeatMs { get; set; } = 250;
    /// <summary>How long a request waits for a majority to confirm its changes before failing with NotCommitted.</summary>
    public int CommitTimeoutMs { get; set; } = 5000;
    public int MaxLogEntries { get; set; } = 50_000;
    /// <summary>
    /// How members talk to each other: <c>WebSocket</c> (default; one connection per peer, binary frames) or <c>Http</c>
    /// (one request per call). Members on an older version are reached over HTTP automatically.
    /// </summary>
    public string Transport { get; set; } = "WebSocket";
    /// <summary>
    /// Replication batches that may be in flight to one server at once (pipelining). 1 (default) waits for each answer and
    /// gets bigger batches; raise it (e.g. 4) only for servers far apart (long round trip) with CPU to spare.
    /// </summary>
    public int MaxInflightAppends { get; set; } = 1;
    /// <summary>Development only: accept self-signed certificates between members.</summary>
    public bool AllowUntrustedCertificates { get; set; }
    /// <summary>
    /// Start as a NEW server joining an existing cluster: it stays passive until an operator adds it
    /// (<c>nexusmq cluster add &lt;id&gt; &lt;url&gt;</c>), then receives the data and the membership from the leader.
    /// <see cref="Nodes"/> is not needed.
    /// </summary>
    public bool Join { get; set; }
    /// <summary>
    /// Deliver a message only after a majority has stored it (default). Consumers then never see a message that could still
    /// be lost if the leader failed. Turning it off saves a little latency.
    /// </summary>
    public bool DeliverOnlyCommitted { get; set; } = true;

    public bool Enabled => Nodes.Count > 0 || Join;
}

/// <summary>Raft RPCs over HTTP between cluster members (authenticated with the shared cluster secret).</summary>
internal sealed class HttpRaftTransport : IRaftTransport, IDisposable
{
    private readonly HttpClient _http;
    private readonly HttpClient _bulk;

    public HttpRaftTransport(NexusClusterOptions options)
    {
        HttpClient Make(TimeSpan timeout)
        {
            var handler = new SocketsHttpHandler { PooledConnectionLifetime = TimeSpan.FromMinutes(5), ConnectTimeout = TimeSpan.FromSeconds(2) };
            if (options.AllowUntrustedCertificates)
                handler.SslOptions.RemoteCertificateValidationCallback = (_, _, _, _) => true;
            var c = new HttpClient(handler) { Timeout = timeout };
            c.DefaultRequestHeaders.Add("X-Raft-Secret", options.Secret);
            return c;
        }
        _http = Make(TimeSpan.FromSeconds(5));
        _bulk = Make(TimeSpan.FromMinutes(5)); // snapshots
    }

    private static async Task<TResp?> Post<TReq, TResp>(HttpClient http, ClusterMember peer, string path, TReq body, CancellationToken ct) where TResp : class
    {
        try
        {
            using var resp = await http.PostAsJsonAsync(peer.Url.TrimEnd('/') + "/api/raft/" + path, body, NexusReplicationStream.Json, ct);
            if (!resp.IsSuccessStatusCode) return null;
            return await resp.Content.ReadFromJsonAsync<TResp>(NexusReplicationStream.Json, ct);
        }
        catch (Exception ex) when (ex is HttpRequestException or TaskCanceledException or System.Text.Json.JsonException)
        {
            return null;
        }
    }

    public Task<VoteResponse?> RequestVoteAsync(ClusterMember peer, VoteRequest r, CancellationToken ct) =>
        Post<VoteRequest, VoteResponse>(_http, peer, "vote", r, ct);

    public Task<AppendResponse?> AppendEntriesAsync(ClusterMember peer, AppendRequest r, CancellationToken ct) =>
        Post<AppendRequest, AppendResponse>(_http, peer, "append", r, ct);

    public Task<SnapshotResponse?> InstallSnapshotAsync(ClusterMember peer, SnapshotRequest r, CancellationToken ct) =>
        Post<SnapshotRequest, SnapshotResponse>(_bulk, peer, "snapshot", r, ct);

    public void Dispose()
    {
        _http.Dispose();
        _bulk.Dispose();
    }
}

internal static class NexusClusterEndpoints
{
    public static void Map(IEndpointRouteBuilder endpoints, string apiRoot)
    {
        static RaftNode? Authorized(HttpContext h)
        {
            var node = h.RequestServices.GetRequiredService<NexusNode>();
            var secret = node.ClusterOptions.Secret;
            var presented = h.Request.Headers["X-Raft-Secret"].ToString();
            if (node.Raft is null || string.IsNullOrEmpty(secret)
                || !CryptographicOperations.FixedTimeEquals(Encoding.UTF8.GetBytes(secret), Encoding.UTF8.GetBytes(presented)))
                return null;
            return node.Raft;
        }

        endpoints.MapPost(apiRoot + "/raft/vote", (HttpContext h, VoteRequest r) =>
            Authorized(h) is { } raft ? Results.Json(raft.HandleVote(r), NexusReplicationStream.Json) : Results.StatusCode(403));
        endpoints.MapPost(apiRoot + "/raft/append", (HttpContext h, AppendRequest r) =>
            Authorized(h) is { } raft ? Results.Json(raft.HandleAppend(r), NexusReplicationStream.Json) : Results.StatusCode(403));
        endpoints.Map(apiRoot + "/raft/ws", WebSocketRaftTransport.ServeAsync);
        endpoints.MapPost(apiRoot + "/raft/snapshot", async (HttpContext h, SnapshotRequest r) =>
            Authorized(h) is { } raft ? Results.Json(await raft.HandleSnapshotAsync(r), NexusReplicationStream.Json) : Results.StatusCode(403));
    }
}
