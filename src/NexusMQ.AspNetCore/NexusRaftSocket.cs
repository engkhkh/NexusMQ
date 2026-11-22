using System.Buffers;
using System.Buffers.Binary;
using System.Collections.Concurrent;
using System.Net;
using System.Net.WebSockets;
using System.Security.Cryptography;
using System.Text;
using System.Text.Json;
using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.DependencyInjection;
using NexusMQ.Core.Cluster;

namespace NexusMQ.AspNetCore;

/// <summary>
/// Raft RPCs between cluster members over one long-lived WebSocket per peer (<c>{api}/raft/ws</c>).
/// <para>
/// A full HTTP request costs far more CPU than the Raft work it carries, so votes and AppendEntries travel as binary
/// frames on a connection that stays open: <c>[kind:1][id:8][JSON]</c>, answered with <c>[kind|0x80][id:8][JSON]</c>.
/// Many calls can be in flight on one connection. The receiving server handles them strictly in order, which is what
/// pipelined replication needs. Snapshots stay on HTTP (large and rare). A peer without the endpoint (an older version
/// during a rolling upgrade) is reached over HTTP instead.
/// </para>
/// </summary>
internal sealed class WebSocketRaftTransport(NexusClusterOptions options) : IRaftTransport, IDisposable
{
    internal const byte KindVote = 1, KindAppend = 2, ReplyFlag = 0x80;
    private const int HeaderSize = 9;

    private readonly HttpRaftTransport _http = new(options);
    private readonly ConcurrentDictionary<string, PeerLink> _links = new();

    public Task<VoteResponse?> RequestVoteAsync(ClusterMember peer, VoteRequest r, CancellationToken ct) =>
        CallAsync<VoteRequest, VoteResponse>(peer, KindVote, r, ct, () => _http.RequestVoteAsync(peer, r, ct));

    public Task<AppendResponse?> AppendEntriesAsync(ClusterMember peer, AppendRequest r, CancellationToken ct) =>
        CallAsync<AppendRequest, AppendResponse>(peer, KindAppend, r, ct, () => _http.AppendEntriesAsync(peer, r, ct));

    public Task<SnapshotResponse?> InstallSnapshotAsync(ClusterMember peer, SnapshotRequest r, CancellationToken ct) =>
        _http.InstallSnapshotAsync(peer, r, ct);

    private async Task<TResp?> CallAsync<TReq, TResp>(ClusterMember peer, byte kind, TReq request, CancellationToken ct,
        Func<Task<TResp?>> overHttp) where TResp : class
    {
        var link = _links.GetOrAdd(peer.Url, url => new PeerLink(url, options));
        if (link.UseHttp) return await overHttp();
        var payload = JsonSerializer.SerializeToUtf8Bytes(request, NexusReplicationStream.Json);
        var reply = await link.CallAsync(kind, payload, ct);
        if (reply is null) return link.UseHttp ? await overHttp() : null; // just found out the peer has no socket endpoint
        try { return JsonSerializer.Deserialize<TResp>(reply, NexusReplicationStream.Json); }
        catch (JsonException) { return null; }
    }

    public void Dispose()
    {
        foreach (var link in _links.Values) link.Dispose();
        _http.Dispose();
    }

    /// <summary>The connection to one peer: connects on first use and again after a failure.</summary>
    private sealed class PeerLink(string baseUrl, NexusClusterOptions options) : IDisposable
    {
        private readonly Uri _uri = new(baseUrl.TrimEnd('/').Replace("https://", "wss://").Replace("http://", "ws://") + "/api/raft/ws");
        private readonly HttpMessageInvoker _invoker =
            new(NexusMQ.Client.NexusConnect.CreateHandler(options.AllowUntrustedCertificates), disposeHandler: true);
        private readonly SemaphoreSlim _connectLock = new(1, 1);
        private readonly SemaphoreSlim _sendLock = new(1, 1);
        private readonly ConcurrentDictionary<long, TaskCompletionSource<byte[]?>> _pending = new();
        private ClientWebSocket? _ws;
        private long _nextId;
        private long _retryAfter;     // Stopwatch timestamp: no reconnect attempts before this
        private long _httpUntil;      // Stopwatch timestamp: peer has no socket endpoint, use HTTP until this
        private bool _disposed;

        public bool UseHttp => System.Diagnostics.Stopwatch.GetTimestamp() < Interlocked.Read(ref _httpUntil);

        public async Task<byte[]?> CallAsync(byte kind, byte[] payload, CancellationToken ct)
        {
            var ws = await ConnectAsync(ct);
            if (ws is null) return null;

            var id = Interlocked.Increment(ref _nextId);
            var answer = new TaskCompletionSource<byte[]?>(TaskCreationOptions.RunContinuationsAsynchronously);
            _pending[id] = answer;
            using var reg = ct.Register(() => { if (_pending.TryRemove(id, out var t)) t.TrySetResult(null); });

            var frame = ArrayPool<byte>.Shared.Rent(HeaderSize + payload.Length);
            try
            {
                frame[0] = kind;
                BinaryPrimitives.WriteInt64LittleEndian(frame.AsSpan(1), id);
                payload.CopyTo(frame, HeaderSize);
                await _sendLock.WaitAsync(ct);
                try { await ws.SendAsync(frame.AsMemory(0, HeaderSize + payload.Length), WebSocketMessageType.Binary, true, ct); }
                finally { _sendLock.Release(); }
            }
            catch (Exception)
            {
                _pending.TryRemove(id, out _);
                if (!ct.IsCancellationRequested) Fail(ws);
                return null;
            }
            finally { ArrayPool<byte>.Shared.Return(frame); }

            return await answer.Task;
        }

        private async Task<ClientWebSocket?> ConnectAsync(CancellationToken ct)
        {
            var ws = _ws;
            if (ws is { State: WebSocketState.Open }) return ws;
            if (System.Diagnostics.Stopwatch.GetTimestamp() < Interlocked.Read(ref _retryAfter) || _disposed) return null;

            await _connectLock.WaitAsync(ct);
            try
            {
                if (_ws is { State: WebSocketState.Open } open) return open;
                if (_ws is { } broken) Fail(broken); // release calls still waiting on the old connection
                var fresh = new ClientWebSocket();
                fresh.Options.SetRequestHeader("X-Raft-Secret", options.Secret);
                fresh.Options.KeepAliveInterval = TimeSpan.FromSeconds(15);
                fresh.Options.CollectHttpResponseDetails = true;
                using var timeout = CancellationTokenSource.CreateLinkedTokenSource(ct);
                timeout.CancelAfter(TimeSpan.FromSeconds(2));
                try
                {
                    await fresh.ConnectAsync(_uri, _invoker, timeout.Token); // races IPv6/IPv4 (see NexusConnect)
                }
                catch (Exception) when (!ct.IsCancellationRequested)
                {
                    // 404/405: the peer runs a version without the socket endpoint → HTTP for a while, then try again.
                    if (fresh.HttpStatusCode is HttpStatusCode.NotFound or HttpStatusCode.MethodNotAllowed)
                        Interlocked.Exchange(ref _httpUntil, Later(TimeSpan.FromMinutes(5)));
                    else
                        Interlocked.Exchange(ref _retryAfter, Later(TimeSpan.FromMilliseconds(250)));
                    fresh.Dispose();
                    return null;
                }
                _ws = fresh;
                _ = Task.Run(() => ReceiveLoopAsync(fresh));
                return fresh;
            }
            finally { _connectLock.Release(); }
        }

        private static long Later(TimeSpan t) =>
            System.Diagnostics.Stopwatch.GetTimestamp() + (long)(t.TotalSeconds * System.Diagnostics.Stopwatch.Frequency);

        private async Task ReceiveLoopAsync(ClientWebSocket ws)
        {
            var buffer = new ArrayBufferWriter<byte>(64 * 1024);
            try
            {
                while (ws.State == WebSocketState.Open)
                {
                    buffer.ResetWrittenCount();
                    ValueWebSocketReceiveResult r;
                    do
                    {
                        r = await ws.ReceiveAsync(buffer.GetMemory(16 * 1024), CancellationToken.None);
                        if (r.MessageType == WebSocketMessageType.Close) { Fail(ws); return; }
                        buffer.Advance(r.Count);
                    } while (!r.EndOfMessage);

                    var frame = buffer.WrittenSpan;
                    if (frame.Length < HeaderSize) continue;
                    var id = BinaryPrimitives.ReadInt64LittleEndian(frame[1..]);
                    if (_pending.TryRemove(id, out var answer)) answer.TrySetResult(frame[HeaderSize..].ToArray());
                }
            }
            catch (Exception) { }
            Fail(ws);
        }

        /// <summary>The connection broke: every call waiting on it gets "no answer" (the caller retries later).</summary>
        private void Fail(ClientWebSocket ws)
        {
            if (ReferenceEquals(Interlocked.CompareExchange(ref _ws, null, ws), ws) || _ws is null)
            {
                foreach (var id in _pending.Keys)
                    if (_pending.TryRemove(id, out var t)) t.TrySetResult(null);
            }
            try { ws.Abort(); ws.Dispose(); } catch { }
        }

        public void Dispose()
        {
            _disposed = true;
            if (_ws is { } ws) Fail(ws);
            _invoker.Dispose();
        }
    }

    /// <summary>Server side of <c>{api}/raft/ws</c>: one connection from one leader (or candidate), calls handled in order.</summary>
    internal static async Task ServeAsync(HttpContext http)
    {
        var node = http.RequestServices.GetRequiredService<NexusNode>();
        var secret = node.ClusterOptions.Secret;
        var presented = http.Request.Headers["X-Raft-Secret"].ToString();
        if (node.Raft is not { } raft || string.IsNullOrEmpty(secret)
            || !CryptographicOperations.FixedTimeEquals(Encoding.UTF8.GetBytes(secret), Encoding.UTF8.GetBytes(presented)))
        {
            http.Response.StatusCode = StatusCodes.Status403Forbidden;
            return;
        }
        if (!http.WebSockets.IsWebSocketRequest)
        {
            http.Response.StatusCode = StatusCodes.Status400BadRequest;
            return;
        }

        using var ws = await http.WebSockets.AcceptWebSocketAsync();
        var buffer = new ArrayBufferWriter<byte>(64 * 1024);
        var stop = http.RequestAborted;
        try
        {
            while (ws.State == WebSocketState.Open && !stop.IsCancellationRequested)
            {
                buffer.ResetWrittenCount();
                ValueWebSocketReceiveResult r;
                do
                {
                    r = await ws.ReceiveAsync(buffer.GetMemory(16 * 1024), stop);
                    if (r.MessageType == WebSocketMessageType.Close) return;
                    buffer.Advance(r.Count);
                } while (!r.EndOfMessage);

                var frame = buffer.WrittenMemory;
                if (frame.Length < HeaderSize) return;
                var kind = frame.Span[0];
                var json = frame[HeaderSize..];
                byte[] reply = kind switch
                {
                    KindVote => JsonSerializer.SerializeToUtf8Bytes(
                        raft.HandleVote(JsonSerializer.Deserialize<VoteRequest>(json.Span, NexusReplicationStream.Json)!), NexusReplicationStream.Json),
                    KindAppend => JsonSerializer.SerializeToUtf8Bytes(
                        raft.HandleAppend(JsonSerializer.Deserialize<AppendRequest>(json.Span, NexusReplicationStream.Json)!), NexusReplicationStream.Json),
                    _ => []
                };
                if (reply.Length == 0) return; // unknown call: close and let the peer fall back

                var answer = ArrayPool<byte>.Shared.Rent(HeaderSize + reply.Length);
                try
                {
                    answer[0] = (byte)(kind | ReplyFlag);
                    frame.Span.Slice(1, 8).CopyTo(answer.AsSpan(1)); // same id
                    reply.CopyTo(answer, HeaderSize);
                    await ws.SendAsync(answer.AsMemory(0, HeaderSize + reply.Length), WebSocketMessageType.Binary, true, stop);
                }
                finally { ArrayPool<byte>.Shared.Return(answer); }
            }
        }
        catch (Exception) when (stop.IsCancellationRequested || ws.State != WebSocketState.Open) { }
        catch (WebSocketException) { }
    }
}
