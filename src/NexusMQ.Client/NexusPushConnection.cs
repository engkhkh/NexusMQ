using System.Collections.Concurrent;
using System.Net.WebSockets;
using System.Text.Json;
using System.Threading.Channels;

namespace NexusMQ.Client;

/// <summary>
/// One WebSocket to the server's push endpoint (<c>/api/ws</c>). Carries subscriptions (server pushes messages),
/// settlements and sends. If the socket drops, every pending call and subscription fails so the owner can reconnect.
/// </summary>
internal sealed class NexusPushConnection : IAsyncDisposable
{
    private readonly ClientWebSocket _ws;
    private readonly SemaphoreSlim _write = new(1, 1);
    private readonly ConcurrentDictionary<string, TaskCompletionSource<JsonElement>> _pending = new();
    private readonly ConcurrentDictionary<string, NexusPushSubscription> _subs = new();
    private readonly CancellationTokenSource _cts = new();
    private Task? _receiveLoop;
    private long _nextId;
    private int _maxRetries;

    private NexusPushConnection(ClientWebSocket ws) => _ws = ws;

    public static async Task<NexusPushConnection> ConnectAsync(NexusClient client, CancellationToken ct)
    {
        // Try the current server first, then the others (a standby refuses the upgrade with 503).
        ClientWebSocket? ws = null;
        Exception? last = null;
        for (var i = 0; i < client.Endpoints.Count && ws is null; i++)
        {
            var baseUri = client.CurrentEndpoint;
            var ub = new UriBuilder(new Uri(baseUri, "api/ws")) { Scheme = baseUri.Scheme == Uri.UriSchemeHttps ? "wss" : "ws" };
            // Ask for raw (binary) message bodies; servers before 1.11 ignore this and keep sending JSON, which we also read.
            if (client.Options.BinaryBodies) ub.Query = "binaryBodies=1";
            var candidate = new ClientWebSocket();
            candidate.Options.CollectHttpResponseDetails = true; // to read the leader hint when a follower refuses us
            candidate.Options.KeepAliveInterval = TimeSpan.FromSeconds(20);
            if (!string.IsNullOrEmpty(client.ApiKey)) candidate.Options.SetRequestHeader("X-Api-Key", client.ApiKey);
            if (client.Options.AllowUntrustedCertificates) candidate.Options.RemoteCertificateValidationCallback = (_, _, _, _) => true;
            try
            {
                await candidate.ConnectAsync(ub.Uri, ct).ConfigureAwait(false);
                ws = candidate;
            }
            catch (Exception ex) when (ex is WebSocketException or HttpRequestException && !ct.IsCancellationRequested)
            {
                var hint = candidate.HttpResponseHeaders?.TryGetValue("X-NexusMQ-Leader", out var h) == true ? h.FirstOrDefault() : null;
                candidate.Dispose();
                last = ex;
                if (!client.FollowLeaderHint(hint) || client.CurrentEndpoint == baseUri) client.MarkFailed(baseUri);
            }
        }
        if (ws is null) throw last ?? new InvalidOperationException("No NexusMQ server reachable.");
        var conn = new NexusPushConnection(ws) { _maxRetries = client.Options.MaxRateLimitRetries };
        conn._receiveLoop = Task.Run(conn.ReceiveLoopAsync);
        return conn;
    }

    public bool IsOpen => _ws.State == WebSocketState.Open;

    private string NextId(string prefix) => prefix + Interlocked.Increment(ref _nextId);

    /// <summary>Send a frame and wait for its ok/error reply. Retries after the server's delay when the key is rate limited.</summary>
    public async Task<JsonElement> RequestAsync(Dictionary<string, object?> frame, CancellationToken ct)
    {
        for (var attempt = 0; ; attempt++)
        {
            try
            {
                return await RequestOnceAsync(frame, ct).ConfigureAwait(false);
            }
            catch (NexusClientException ex) when (ex.Code == "RateLimited" && attempt < _maxRetries)
            {
                await Task.Delay(ex.RetryAfter ?? TimeSpan.FromSeconds(1), ct).ConfigureAwait(false);
            }
        }
    }

    private async Task<JsonElement> RequestOnceAsync(Dictionary<string, object?> frame, CancellationToken ct)
    {
        var req = NextId("r");
        frame["req"] = req;
        var tcs = new TaskCompletionSource<JsonElement>(TaskCreationOptions.RunContinuationsAsynchronously);
        _pending[req] = tcs;
        try
        {
            await SendFrameAsync(frame, ct).ConfigureAwait(false);
            return await tcs.Task.WaitAsync(TimeSpan.FromSeconds(60), ct).ConfigureAwait(false);
        }
        finally
        {
            _pending.TryRemove(req, out _);
        }
    }

    public async Task SendFrameAsync(Dictionary<string, object?> frame, CancellationToken ct)
    {
        var bytes = JsonSerializer.SerializeToUtf8Bytes(frame, NexusJson.Options);
        await _write.WaitAsync(ct).ConfigureAwait(false);
        try
        {
            await _ws.SendAsync(bytes, WebSocketMessageType.Text, true, ct).ConfigureAwait(false);
        }
        finally
        {
            _write.Release();
        }
    }

    public Task<NexusPushSubscription> SubscribeAsync(string entityPath, NexusReceiveMode mode, int prefetch, CancellationToken ct) =>
        SubscribeCoreAsync(new() { ["entity"] = entityPath, ["mode"] = mode.ToString(), ["prefetch"] = prefetch }, entityPath, mode, ct);

    /// <summary>Session subscription: the server locks one session at a time and pushes its messages in order.</summary>
    public Task<NexusPushSubscription> SubscribeSessionsAsync(string entityPath, NexusReceiveMode mode, int prefetch,
        TimeSpan idleTimeout, string? sessionId, CancellationToken ct) =>
        SubscribeCoreAsync(new()
        {
            ["entity"] = entityPath,
            ["mode"] = mode.ToString(),
            ["prefetch"] = prefetch,
            ["sessions"] = true,
            ["sessionId"] = sessionId,
            ["idleSeconds"] = idleTimeout.TotalSeconds
        }, entityPath, mode, ct);

    private async Task<NexusPushSubscription> SubscribeCoreAsync(Dictionary<string, object?> frame, string entityPath, NexusReceiveMode mode, CancellationToken ct)
    {
        var id = NextId("s");
        var sub = new NexusPushSubscription(this, id, entityPath, mode);
        _subs[id] = sub;
        frame["op"] = "subscribe";
        frame["id"] = id;
        try
        {
            await RequestAsync(frame, ct).ConfigureAwait(false);
        }
        catch
        {
            _subs.TryRemove(id, out _);
            throw;
        }
        return sub;
    }

    private async Task ReceiveLoopAsync()
    {
        var buffer = new byte[64 * 1024];
        Exception failure = new NexusClientException("ConnectionClosed", 0, "The NexusMQ push connection was closed.");
        try
        {
            while (_ws.State == WebSocketState.Open && !_cts.IsCancellationRequested)
            {
                using var ms = new MemoryStream();
                WebSocketReceiveResult r;
                do
                {
                    r = await _ws.ReceiveAsync(buffer, _cts.Token).ConfigureAwait(false);
                    if (r.MessageType == WebSocketMessageType.Close) return;
                    ms.Write(buffer, 0, r.Count);
                } while (!r.EndOfMessage);

                // Binary frame: [header length: int32 LE][header JSON][raw body] (a pushed message with binary bodies).
                var data = ms.GetBuffer().AsMemory(0, (int)ms.Length);
                byte[]? rawBody = null;
                if (r.MessageType == WebSocketMessageType.Binary)
                {
                    if (data.Length < 4) continue;
                    var headerLength = System.Buffers.Binary.BinaryPrimitives.ReadInt32LittleEndian(data.Span);
                    if (headerLength < 0 || headerLength > data.Length - 4) continue;
                    rawBody = data[(4 + headerLength)..].ToArray();
                    data = data.Slice(4, headerLength);
                }
                using var doc = JsonDocument.Parse(data);
                var root = doc.RootElement;
                var type = root.TryGetProperty("type", out var t) ? t.GetString() : null;
                var req = root.TryGetProperty("req", out var rq) && rq.ValueKind == JsonValueKind.String ? rq.GetString() : null;
                var id = root.TryGetProperty("id", out var idEl) && idEl.ValueKind == JsonValueKind.String ? idEl.GetString() : null;

                switch (type)
                {
                    case "message" when id is not null && _subs.TryGetValue(id, out var sub):
                        var message = root.GetProperty("message").Deserialize<NexusReceivedMessage>(NexusJson.Options)!;
                        if (rawBody is not null) message.SetBody(rawBody);
                        sub.Deliver(new PushEvent(message, null, null, null, false));
                        break;
                    case "session" when id is not null && _subs.TryGetValue(id, out var ss):
                        ss.Deliver(new PushEvent(null, root.GetProperty("sessionId").GetString(),
                            root.GetProperty("sessionLockToken").GetGuid(), root.GetProperty("lockedUntilUtc").GetDateTimeOffset(), false));
                        break;
                    case "sessionClosed" when id is not null && _subs.TryGetValue(id, out var sc):
                        sc.Deliver(new PushEvent(null, root.GetProperty("sessionId").GetString(), null, null, true));
                        break;
                    case "ok" when req is not null && _pending.TryGetValue(req, out var ok):
                        ok.TrySetResult(root.TryGetProperty("result", out var res) ? res.Clone() : default);
                        break;
                    case "error":
                        var ex = new NexusClientException(
                            root.TryGetProperty("code", out var c) ? c.GetString() ?? "Error" : "Error", 0,
                            root.TryGetProperty("message", out var m) ? m.GetString() ?? "Error" : "Error")
                        {
                            RetryAfter = root.TryGetProperty("retryAfterMs", out var ra) && ra.ValueKind == JsonValueKind.Number
                                ? TimeSpan.FromMilliseconds(ra.GetInt32()) : null
                        };
                        if (req is not null && _pending.TryGetValue(req, out var pending)) pending.TrySetException(ex);
                        else if (id is not null && _subs.TryRemove(id, out var failed)) failed.Fail(ex);
                        break;
                }
            }
        }
        catch (Exception ex) when (ex is WebSocketException or OperationCanceledException or JsonException)
        {
            failure = new NexusClientException("ConnectionClosed", 0, "The NexusMQ push connection was lost: " + ex.Message);
        }
        finally
        {
            foreach (var p in _pending.Values) p.TrySetException(failure);
            foreach (var s in _subs.Values) s.Fail(failure);
            _subs.Clear();
        }
    }

    public async ValueTask DisposeAsync()
    {
        if (_ws.State == WebSocketState.Open)
        {
            try
            {
                using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(2));
                await _ws.CloseAsync(WebSocketCloseStatus.NormalClosure, "bye", timeout.Token).ConfigureAwait(false);
            }
            catch { /* best effort */ }
        }
        await _cts.CancelAsync().ConfigureAwait(false);
        if (_receiveLoop is not null)
        {
            try { await _receiveLoop.ConfigureAwait(false); } catch { }
        }
        _ws.Dispose();
        _cts.Dispose();
    }
}

/// <summary>Something the server pushed: a message, or (session subscriptions) a session starting or closing.</summary>
internal sealed record PushEvent(NexusReceivedMessage? Message, string? SessionId, Guid? SessionLockToken, DateTimeOffset? LockedUntilUtc, bool SessionClosed);

/// <summary>A live push subscription: events arrive on <see cref="Reader"/>; settle through this object to return credit.</summary>
internal sealed class NexusPushSubscription : IMessageSettler
{
    private readonly NexusPushConnection _conn;
    private readonly Channel<PushEvent> _channel = Channel.CreateUnbounded<PushEvent>(
        new UnboundedChannelOptions { SingleWriter = true });

    public NexusPushSubscription(NexusPushConnection conn, string id, string entityPath, NexusReceiveMode mode)
    {
        _conn = conn;
        Id = id;
        EntityPath = entityPath;
        Mode = mode;
    }

    public string Id { get; }
    public string EntityPath { get; }
    public NexusReceiveMode Mode { get; }
    public ChannelReader<PushEvent> Reader => _channel.Reader;

    internal void Deliver(PushEvent e) => _channel.Writer.TryWrite(e);
    internal void Fail(Exception ex) => _channel.Writer.TryComplete(ex);

    /// <summary>ReceiveAndDelete: tell the server one message was handled so it can push another.</summary>
    public Task CreditAsync(CancellationToken ct) =>
        _conn.SendFrameAsync(new() { ["op"] = "credit", ["id"] = Id, ["count"] = 1 }, ct);

    private Task<JsonElement> Settle(NexusReceivedMessage m, string action, CancellationToken ct,
        string? reason = null, string? description = null, IDictionary<string, string>? props = null)
    {
        if (m.LockToken is null)
            throw new InvalidOperationException("This message has no lock token (it was received in ReceiveAndDelete mode).");
        return _conn.RequestAsync(new()
        {
            ["op"] = "settle",
            ["id"] = Id,
            ["lockToken"] = m.LockToken.Value,
            ["action"] = action,
            ["reason"] = reason,
            ["description"] = description,
            ["properties"] = props
        }, ct);
    }

    public Task CompleteAsync(NexusReceivedMessage m, CancellationToken ct) => Settle(m, "Complete", ct);
    public Task AbandonAsync(NexusReceivedMessage m, IDictionary<string, string>? p, CancellationToken ct) => Settle(m, "Abandon", ct, props: p);
    public Task DeadLetterAsync(NexusReceivedMessage m, string? r, string? d, CancellationToken ct) => Settle(m, "DeadLetter", ct, r, d);
    public Task DeferAsync(NexusReceivedMessage m, CancellationToken ct) => Settle(m, "Defer", ct);

    public async Task<DateTimeOffset> RenewAsync(NexusReceivedMessage m, CancellationToken ct)
    {
        var result = await Settle(m, "Renew", ct).ConfigureAwait(false);
        if (result.ValueKind == JsonValueKind.Object && result.TryGetProperty("lockedUntilUtc", out var u) && u.TryGetDateTimeOffset(out var until))
            m.LockedUntilUtc = until;
        return m.LockedUntilUtc ?? DateTimeOffset.UtcNow;
    }
}
