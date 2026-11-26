using System.Collections.Concurrent;
using System.Net.WebSockets;
using System.Text;
using System.Text.Json;
using System.Text.Json.Serialization;
using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using NexusMQ.Core;

namespace NexusMQ.AspNetCore;

/// <summary>
/// NexusMQ push protocol over WebSocket (JSON text frames). The server pushes messages as soon as they arrive,
/// up to <c>prefetch</c> unsettled messages per subscription (credit-based flow control, like RabbitMQ basic.qos).
/// <code>
/// client → server
///   {"op":"subscribe","id":"s1","entity":"orders","mode":"PeekLock","prefetch":10}
///   {"op":"subscribe","id":"s2","entity":"shipments","sessions":true,"sessionId":null,"idleSeconds":5,"prefetch":1}
///       → the server locks the next free session, pushes {"type":"session",…}, its messages in order, then
///         {"type":"sessionClosed",…} after idleSeconds without messages, and moves on to the next session.
///         The session lock is renewed by the server while the socket is open.
///   {"op":"unsubscribe","id":"s1"}
///   {"op":"settle","req":"r1","id":"s1","lockToken":"…","action":"Complete|Abandon|DeadLetter|Defer|Renew","reason":…,"description":…,"properties":{…}}
///   {"op":"credit","id":"s1","count":1}                       (ReceiveAndDelete: tell the server a message was handled)
///   {"op":"send","req":"r2","entity":"orders","routingKey":null,"messages":[…]}
///   {"op":"ping","req":"r3"}
/// server → client
///   {"type":"message","id":"s1","message":{…}}
///   {"type":"ok","req":"r1","result":{…}}
///   {"type":"error","req":"r1","id":"s1","code":"MessageLockLost","message":"…"}
/// </code>
/// When the socket closes, every message it still holds a lock on is abandoned at once (no waiting for lock expiry).
/// </summary>
internal sealed class NexusWebSocketHandler
{
    internal static readonly JsonSerializerOptions Json = new(JsonSerializerDefaults.Web)
    {
        DefaultIgnoreCondition = JsonIgnoreCondition.WhenWritingNull,
        Converters = { new JsonStringEnumConverter() }
    };

    private sealed class Frame
    {
        public string Op { get; set; } = "";
        public string? Id { get; set; }
        public string? Req { get; set; }
        public string? Entity { get; set; }
        public string? Mode { get; set; }
        public int? Prefetch { get; set; }
        public int? Count { get; set; }
        public Guid? LockToken { get; set; }
        public SettleAction? Action { get; set; }
        public string? Reason { get; set; }
        public string? Description { get; set; }
        public Dictionary<string, string>? Properties { get; set; }
        public string? RoutingKey { get; set; }
        public List<SendMessageRequest>? Messages { get; set; }
        public bool? Sessions { get; set; }
        public string? SessionId { get; set; }
        public double? IdleSeconds { get; set; }
    }

    private sealed class Subscription(string id, string entity, ReceiveMode mode, int prefetch)
    {
        public string Id { get; } = id;
        public string Entity { get; } = entity;
        public ReceiveMode Mode { get; } = mode;
        public int Prefetch { get; } = prefetch;
        public CancellationTokenSource Cts { get; } = new();
        public ConcurrentDictionary<Guid, DateTimeOffset> InFlight { get; } = new();
        public int UnackedNoLock; // ReceiveAndDelete messages not yet credited back
        public SemaphoreSlim CreditChanged { get; } = new(0);
        public Task? Pump { get; set; }

        // Session subscriptions
        public bool Sessions { get; init; }
        public string? RequestedSessionId { get; init; }
        public TimeSpan IdleTimeout { get; init; } = TimeSpan.FromSeconds(5);
        public SessionHandle? CurrentSession { get; set; }

        public int Outstanding()
        {
            var now = DateTimeOffset.UtcNow;
            foreach (var (token, until) in InFlight)
                if (until < now) InFlight.TryRemove(token, out _); // lock expired → broker redelivers; reclaim credit
            return InFlight.Count + Volatile.Read(ref UnackedNoLock);
        }

        public void ReturnCredit()
        {
            if (CreditChanged.CurrentCount == 0) CreditChanged.Release();
        }
    }

    private readonly WebSocket _socket;
    private readonly Broker _broker;
    private readonly NexusServerOptions _options;
    private readonly ILogger _log;
    private readonly NexusAuthorizer _auth;
    private readonly IReadOnlyList<NexusAccessKey>? _caller;
    private readonly NexusAuditLog _audit;
    private readonly string? _remoteIp;
    private readonly NexusNode _node;
    // Frames are handled concurrently (bounded), so that in a cluster many settlements can wait for the same commit.
    private readonly SemaphoreSlim _inFlightFrames = new(64, 64);
    private readonly SemaphoreSlim _writeLock = new(1, 1);
    /// <summary>Client asked for <c>?binaryBodies=1</c>: deliveries are binary frames carrying the raw body (no base64).</summary>
    internal bool BinaryBodies { get; init; }
    private readonly ConcurrentDictionary<string, Subscription> _subs = new(StringComparer.Ordinal);

    private NexusWebSocketHandler(WebSocket socket, Broker broker, NexusServerOptions options, ILogger log,
        NexusAuthorizer auth, IReadOnlyList<NexusAccessKey>? caller, NexusAuditLog audit, string? remoteIp, NexusNode node)
    {
        _node = node;
        _audit = audit;
        _remoteIp = remoteIp;
        _socket = socket;
        _broker = broker;
        _options = options;
        _log = log;
        _auth = auth;
        _caller = caller;
    }

    public static async Task HandleAsync(HttpContext http)
    {
        var options = http.RequestServices.GetRequiredService<IOptions<NexusServerOptions>>().Value;
        if (!http.WebSockets.IsWebSocketRequest)
        {
            http.Response.StatusCode = StatusCodes.Status400BadRequest;
            await http.Response.WriteAsync("Expected a WebSocket request. Did you call app.UseWebSockets()? (MapNexusMQ does it for WebApplication.)");
            return;
        }
        var auth = http.RequestServices.GetRequiredService<NexusAuthorizer>();
        var caller = auth.Authenticate(NexusAuthorizer.PresentedKey(http));
        var audit = http.RequestServices.GetRequiredService<NexusAuditLog>();
        if (!auth.IsOpen && caller is null)
        {
            audit.Record("unauthenticated", "WS connect", null, "denied", "Missing or invalid access key.", http.Connection.RemoteIpAddress?.ToString());
            http.Response.StatusCode = StatusCodes.Status401Unauthorized;
            return;
        }

        using var socket = await http.WebSockets.AcceptWebSocketAsync();
        var log = http.RequestServices.GetRequiredService<ILoggerFactory>().CreateLogger("NexusMQ.WebSocket");
        var handler = new NexusWebSocketHandler(socket, http.RequestServices.GetRequiredService<Broker>(), options, log, auth, caller, audit, http.Connection.RemoteIpAddress?.ToString(),
            http.RequestServices.GetRequiredService<NexusNode>())
        {
            BinaryBodies = http.Request.Query["binaryBodies"] is { Count: > 0 } b && (b[0] == "1" || string.Equals(b[0], "true", StringComparison.OrdinalIgnoreCase))
        };
        using var shutdown = NexusShutdown.Linked(http); // clients reconnect (to the new leader) instead of holding up the shutdown
        await handler.RunAsync(shutdown.Token);
    }

    private async Task RunAsync(CancellationToken aborted)
    {
        var buffer = new byte[64 * 1024];
        // If this node stops serving (lost leadership), drop the connection so the client reconnects to the new leader.
        void OnRole(NexusRole role)
        {
            if (role == NexusRole.Standby) _socket.Abort();
        }
        _node.RoleChanged += OnRole;
        try
        {
            while (_socket.State == WebSocketState.Open && !aborted.IsCancellationRequested)
            {
                using var ms = new MemoryStream();
                WebSocketReceiveResult result;
                do
                {
                    result = await _socket.ReceiveAsync(buffer, aborted);
                    if (result.MessageType == WebSocketMessageType.Close) return;
                    ms.Write(buffer, 0, result.Count);
                    if (ms.Length > 16 * 1024 * 1024) throw new InvalidOperationException("Frame too large.");
                } while (!result.EndOfMessage);

                Frame? frame;
                try { frame = JsonSerializer.Deserialize<Frame>(ms.GetBuffer().AsSpan(0, (int)ms.Length), Json); }
                catch (JsonException ex) { await SendErrorAsync(null, null, NexusErrorCode.BadRequest, "Invalid JSON: " + ex.Message); continue; }
                if (frame is null) continue;

                await _inFlightFrames.WaitAsync(aborted);
                _ = Task.Run(async () =>
                {
                    try { await DispatchAsync(frame); }
                    finally { _inFlightFrames.Release(); }
                }, CancellationToken.None);
            }
        }
        catch (OperationCanceledException) { }
        catch (WebSocketException) { }
        catch (Exception ex)
        {
            _log.LogWarning(ex, "NexusMQ WebSocket connection failed");
        }
        finally
        {
            _node.RoleChanged -= OnRole;
            foreach (var sub in _subs.Values) await StopAsync(sub);
            _subs.Clear();
            if (_socket.State == WebSocketState.Open)
            {
                try { await _socket.CloseAsync(WebSocketCloseStatus.NormalClosure, "bye", CancellationToken.None); } catch { }
            }
        }
    }

    private async Task DispatchAsync(Frame f)
    {
        if (_node.IsCluster) NexusMQ.Core.Cluster.RaftStore.Track(); // per frame: each Task.Run has its own async flow
        try
        {
            _auth.CheckRate(_caller);
            switch (f.Op.ToLowerInvariant())
            {
                case "subscribe": await SubscribeAsync(f); break;
                case "unsubscribe":
                    if (f.Id is not null && _subs.TryRemove(f.Id, out var gone)) await StopAsync(gone);
                    await SendOkAsync(f.Req, null);
                    break;
                case "credit":
                    if (f.Id is not null && _subs.TryGetValue(f.Id, out var cs))
                    {
                        var n = Math.Max(1, f.Count ?? 1);
                        var left = Interlocked.Add(ref cs.UnackedNoLock, -n);
                        if (left < 0) Interlocked.Exchange(ref cs.UnackedNoLock, 0);
                        cs.ReturnCredit();
                    }
                    break;
                case "settle": await SettleAsync(f); break;
                case "send":
                {
                    if (string.IsNullOrEmpty(f.Entity) || f.Messages is not { Count: > 0 })
                        throw new NexusException(NexusErrorCode.BadRequest, "send needs 'entity' and at least one message.");
                    _auth.Demand(_caller, NexusRights.Send, f.Entity);
                    var r = _broker.Send(f.Entity, f.Messages.Select(m => m.ToOutgoing()).ToList(), f.RoutingKey);
                    await SendOkAsync(f.Req, r);
                    break;
                }
                case "ping": await SendOkAsync(f.Req, new { pong = DateTimeOffset.UtcNow }); break;
                default: throw new NexusException(NexusErrorCode.BadRequest, $"Unknown op '{f.Op}'.");
            }
        }
        catch (NexusException ex)
        {
            if (ex.Code is NexusErrorCode.Unauthorized or NexusErrorCode.Forbidden)
                _audit.Record(_auth.Actor(_caller), "WS " + f.Op, f.Entity ?? f.Id, "denied", ex.Message, _remoteIp);
            await SendErrorAsync(f.Req, f.Id, ex.Code, ex.Message, ex.RetryAfter);
        }
        catch (FormatException ex)
        {
            await SendErrorAsync(f.Req, f.Id, NexusErrorCode.BadRequest, ex.Message);
        }
    }

    private async Task SubscribeAsync(Frame f)
    {
        if (string.IsNullOrEmpty(f.Id) || string.IsNullOrEmpty(f.Entity))
            throw new NexusException(NexusErrorCode.BadRequest, "subscribe needs 'id' and 'entity'.");
        _auth.Demand(_caller, NexusRights.Listen, f.Entity);
        var mode = ReceiveMode.PeekLock;
        if (!string.IsNullOrEmpty(f.Mode) && !Enum.TryParse(f.Mode, true, out mode))
            throw new NexusException(NexusErrorCode.BadRequest, $"Unknown receive mode '{f.Mode}'.");
        var entity = _broker.GetEntity(f.Entity); // throws EntityNotFound
        var sessionEntity = entity.Options.RequiresSession && !entity.IsDeadLetterQueue;
        var wantsSessions = f.Sessions == true || f.SessionId is not null;
        if (sessionEntity && !wantsSessions)
            throw new NexusException(NexusErrorCode.BadRequest, "This entity requires sessions: subscribe with \"sessions\": true.");
        if (!sessionEntity && wantsSessions)
            throw new NexusException(NexusErrorCode.BadRequest, "This entity is not session-enabled.");

        var sub = new Subscription(f.Id, f.Entity, mode, Math.Clamp(f.Prefetch ?? (wantsSessions ? 1 : 10), 1, 1000))
        {
            Sessions = wantsSessions,
            RequestedSessionId = f.SessionId,
            IdleTimeout = TimeSpan.FromSeconds(Math.Clamp(f.IdleSeconds ?? 5, 0.2, 3600))
        };
        if (!_subs.TryAdd(sub.Id, sub))
            throw new NexusException(NexusErrorCode.EntityAlreadyExists, $"Subscription id '{sub.Id}' is already in use on this connection.");
        sub.Pump = Task.Run(() => sub.Sessions ? PumpSessionsAsync(sub, entity.Options.LockDuration) : PumpAsync(sub));
        await SendOkAsync(f.Req, new { subscribed = sub.Id });
    }

    private async Task PumpAsync(Subscription sub)
    {
        var ct = sub.Cts.Token;
        try
        {
            while (!ct.IsCancellationRequested)
            {
                var free = sub.Prefetch - sub.Outstanding();
                if (free <= 0)
                {
                    // Wait for a settle/credit; the timeout lets us reclaim credit from expired locks.
                    await sub.CreditChanged.WaitAsync(TimeSpan.FromSeconds(1), ct);
                    continue;
                }

                var batch = await _broker.ReceiveAsync(sub.Entity, free, TimeSpan.FromSeconds(20), sub.Mode, ct);
                foreach (var r in batch)
                {
                    if (r.LockToken is { } token) sub.InFlight[token] = r.LockedUntilUtc ?? DateTimeOffset.UtcNow.AddSeconds(30);
                    else Interlocked.Increment(ref sub.UnackedNoLock);
                }
                foreach (var r in batch)
                    await SendMessageAsync(sub.Id, r);
            }
        }
        catch (OperationCanceledException) { }
        catch (WebSocketException) { }
        catch (NexusException ex)
        {
            // Entity deleted, etc. Tell the client and end this subscription.
            await SendErrorAsync(null, sub.Id, ex.Code, ex.Message);
            _subs.TryRemove(sub.Id, out _);
        }
        catch (Exception ex)
        {
            _log.LogWarning(ex, "NexusMQ push pump for {Entity} failed", sub.Entity);
            await SendErrorAsync(null, sub.Id, NexusErrorCode.BadRequest, ex.Message);
            _subs.TryRemove(sub.Id, out _);
        }
    }

    private async Task PumpSessionsAsync(Subscription sub, TimeSpan lockDuration)
    {
        var ct = sub.Cts.Token;
        var renewMargin = TimeSpan.FromTicks(Math.Max(lockDuration.Ticks / 2, TimeSpan.FromSeconds(1).Ticks));
        try
        {
            while (!ct.IsCancellationRequested)
            {
                var handle = await _broker.AcceptSessionAsync(sub.Entity, sub.RequestedSessionId, TimeSpan.FromSeconds(20), ct);
                if (handle is null) continue;
                sub.CurrentSession = handle;
                var until = handle.LockedUntilUtc;
                await SendAsync(new { type = "session", id = sub.Id, sessionId = handle.SessionId, sessionLockToken = handle.SessionLockToken, lockedUntilUtc = until });

                var lastActivity = DateTimeOffset.UtcNow;
                try
                {
                    while (!ct.IsCancellationRequested)
                    {
                        if (until - DateTimeOffset.UtcNow < renewMargin)
                            until = _broker.RenewSessionLock(sub.Entity, handle.SessionId, handle.SessionLockToken);

                        var outstanding = sub.Outstanding();
                        var free = sub.Prefetch - outstanding;
                        if (free <= 0)
                        {
                            await sub.CreditChanged.WaitAsync(TimeSpan.FromSeconds(1), ct);
                            continue;
                        }

                        var batch = await _broker.ReceiveFromSessionAsync(sub.Entity, handle.SessionId, handle.SessionLockToken,
                            free, TimeSpan.FromSeconds(1), sub.Mode, ct);
                        if (batch.Count > 0)
                        {
                            foreach (var r in batch)
                            {
                                if (r.LockToken is { } token) sub.InFlight[token] = r.LockedUntilUtc ?? DateTimeOffset.UtcNow.AddSeconds(30);
                                else Interlocked.Increment(ref sub.UnackedNoLock);
                            }
                            foreach (var r in batch)
                                await SendMessageAsync(sub.Id, r);
                            lastActivity = DateTimeOffset.UtcNow;
                            continue;
                        }

                        if (outstanding == 0 && DateTimeOffset.UtcNow - lastActivity >= sub.IdleTimeout) break;
                    }
                }
                catch (NexusException ex) when (ex.Code == NexusErrorCode.SessionLockLost)
                {
                    // Lock lost (e.g. the session was stolen after expiry): drop it and look for the next one.
                }
                finally
                {
                    if (!ct.IsCancellationRequested) await CloseSessionAsync(sub);
                }

                if (sub.RequestedSessionId is not null) break; // a specific-session subscription ends with that session
            }
        }
        catch (OperationCanceledException) { }
        catch (WebSocketException) { }
        catch (NexusException ex)
        {
            await SendErrorAsync(null, sub.Id, ex.Code, ex.Message);
            _subs.TryRemove(sub.Id, out _);
        }
        catch (Exception ex)
        {
            _log.LogWarning(ex, "NexusMQ session pump for {Entity} failed", sub.Entity);
            await SendErrorAsync(null, sub.Id, NexusErrorCode.BadRequest, ex.Message);
            _subs.TryRemove(sub.Id, out _);
        }
    }

    /// <summary>Abandon what the client still holds for the current session, release the session and tell the client.</summary>
    private async Task CloseSessionAsync(Subscription sub)
    {
        if (sub.CurrentSession is not { } s) return;
        foreach (var token in sub.InFlight.Keys)
        {
            try { _broker.Abandon(sub.Entity, token); } catch (NexusException) { }
        }
        sub.InFlight.Clear();
        Interlocked.Exchange(ref sub.UnackedNoLock, 0);
        try { _broker.ReleaseSession(sub.Entity, s.SessionId, s.SessionLockToken); } catch (NexusException) { }
        sub.CurrentSession = null;
        await SendAsync(new { type = "sessionClosed", id = sub.Id, sessionId = s.SessionId });
    }

    private async Task SettleAsync(Frame f)
    {
        if (f.LockToken is not { } token || f.Action is not { } action)
            throw new NexusException(NexusErrorCode.BadRequest, "settle needs 'lockToken' and 'action'.");
        Subscription? sub = null;
        if (f.Id is not null) _subs.TryGetValue(f.Id, out sub);
        var entity = sub?.Entity ?? f.Entity ?? throw new NexusException(NexusErrorCode.BadRequest, "settle needs 'id' or 'entity'.");
        _auth.Demand(_caller, NexusRights.Listen, entity);

        try
        {
            object? result = null;
            switch (action)
            {
                case SettleAction.Complete: _broker.Complete(entity, token); break;
                case SettleAction.Abandon: _broker.Abandon(entity, token, f.Properties); break;
                case SettleAction.DeadLetter: _broker.DeadLetter(entity, token, f.Reason, f.Description, f.Properties); break;
                case SettleAction.Defer: result = new SettleResponse { SequenceNumber = _broker.Defer(entity, token) }; break;
                case SettleAction.Renew:
                {
                    var until = _broker.RenewLock(entity, token);
                    if (sub is not null) sub.InFlight[token] = until;
                    await SendOkAsync(f.Req, new SettleResponse { LockedUntilUtc = until });
                    return;
                }
            }
            await SendOkAsync(f.Req, result);
        }
        finally
        {
            if (action != SettleAction.Renew && sub is not null && sub.InFlight.TryRemove(token, out _))
                sub.ReturnCredit();
        }
    }

    private async Task StopAsync(Subscription sub)
    {
        await sub.Cts.CancelAsync();
        if (sub.Pump is not null)
        {
            try { await sub.Pump; } catch { /* already reported */ }
        }
        // Give unsettled messages back right away instead of waiting for their locks to expire.
        foreach (var token in sub.InFlight.Keys)
        {
            try { _broker.Abandon(sub.Entity, token); } catch (NexusException) { }
        }
        sub.InFlight.Clear();
        if (sub.CurrentSession is { } s)
        {
            try { _broker.ReleaseSession(sub.Entity, s.SessionId, s.SessionLockToken); } catch (NexusException) { }
            sub.CurrentSession = null;
        }
        sub.Cts.Dispose();
    }

    private async Task SendOkAsync(string? req, object? result)
    {
        // Cluster: confirm only once a majority has the changes this operation made.
        if (NexusMQ.Core.Cluster.RaftStore.CurrentTracker is { Index: > 0 } w && !await _node.Raft!.WaitCommittedAsync(w.Index, w.Term, _node.CommitTimeout))
            throw new NexusException(NexusErrorCode.NotCommitted, "The cluster did not confirm this change in time; retry.");
        if (req is not null) await SendAsync(new { type = "ok", req, result });
    }

    private Task SendErrorAsync(string? req, string? id, NexusErrorCode code, string message, TimeSpan? retryAfter = null) =>
        SendAsync(new { type = "error", req, id, code = code.ToString(), message, retryAfterMs = retryAfter is { } r ? (int?)Math.Ceiling(r.TotalMilliseconds) : null });

    /// <summary>
    /// Push one message. With binary bodies the frame is <c>[header length: int32 LE][header JSON][raw body]</c>: the header is
    /// the usual <c>{"type":"message","id":…,"message":{…}}</c> with an empty <c>bodyBase64</c>. That skips base64 and JSON escaping of
    /// the body, which for large bodies costs more than everything else together.
    /// </summary>
    private async Task SendMessageAsync(string subscriptionId, ReceivedMessage r)
    {
        if (!BinaryBodies)
        {
            await SendAsync(new { type = "message", id = subscriptionId, message = MessageResponse.From(r) });
            return;
        }
        var header = JsonSerializer.SerializeToUtf8Bytes(
            new { type = "message", id = subscriptionId, message = MessageResponse.From(r.Message, r.LockToken, r.LockedUntilUtc, includeBody: false) }, Json);
        var body = r.Message.Body;
        var length = 4 + header.Length + body.Length;
        var frame = System.Buffers.ArrayPool<byte>.Shared.Rent(length);
        try
        {
            System.Buffers.Binary.BinaryPrimitives.WriteInt32LittleEndian(frame, header.Length);
            header.CopyTo(frame, 4);
            body.CopyTo(frame, 4 + header.Length);
            await _writeLock.WaitAsync();
            try
            {
                if (_socket.State == WebSocketState.Open)
                    await _socket.SendAsync(frame.AsMemory(0, length), WebSocketMessageType.Binary, true, CancellationToken.None);
            }
            catch (WebSocketException) { }
            finally { _writeLock.Release(); }
        }
        finally { System.Buffers.ArrayPool<byte>.Shared.Return(frame); }
    }

    private async Task SendAsync(object payload)
    {
        var bytes = JsonSerializer.SerializeToUtf8Bytes(payload, Json);
        await _writeLock.WaitAsync();
        try
        {
            if (_socket.State == WebSocketState.Open)
                await _socket.SendAsync(bytes, WebSocketMessageType.Text, true, CancellationToken.None);
        }
        catch (WebSocketException) { }
        finally
        {
            _writeLock.Release();
        }
    }
}
