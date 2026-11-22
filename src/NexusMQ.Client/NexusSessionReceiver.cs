using System.Text;

namespace NexusMQ.Client;

/// <summary>
/// A locked session (like <c>ServiceBusSessionReceiver</c>). Only this receiver gets the session's messages, in order,
/// until the lock expires or the receiver is disposed. Settle messages with the methods here.
/// </summary>
public sealed class NexusSessionReceiver : IAsyncDisposable
{
    private readonly NexusClient _client;
    private readonly NexusReceiver _settler;
    private readonly string _encodedPath;
    private bool _released;

    private sealed class Handle
    {
        public string SessionId { get; set; } = "";
        public Guid SessionLockToken { get; set; }
        public DateTimeOffset LockedUntilUtc { get; set; }
    }

    private NexusSessionReceiver(NexusClient client, string entityPath, NexusReceiveMode mode, Handle handle)
    {
        _client = client;
        EntityPath = entityPath;
        ReceiveMode = mode;
        SessionId = handle.SessionId;
        SessionLockToken = handle.SessionLockToken;
        SessionLockedUntil = handle.LockedUntilUtc;
        _encodedPath = NexusClient.Encode(entityPath);
        _settler = new NexusReceiver(client, entityPath, mode);
    }

    public string EntityPath { get; }
    public string SessionId { get; }
    public Guid SessionLockToken { get; }
    public DateTimeOffset SessionLockedUntil { get; private set; }
    public NexusReceiveMode ReceiveMode { get; }

    /// <summary>Wrap a session that the server locked for a push subscription (state calls go over HTTP with the same lock token).</summary>
    internal static NexusSessionReceiver FromPush(NexusClient client, string entityPath, NexusReceiveMode mode,
        string sessionId, Guid token, DateTimeOffset lockedUntil) =>
        new(client, entityPath, mode, new Handle { SessionId = sessionId, SessionLockToken = token, LockedUntilUtc = lockedUntil })
        {
            _released = true // the server owns the lock and releases it
        };

    /// <summary>Lock <paramref name="sessionId"/>, or the next session with waiting messages when it is null. Returns null on timeout.</summary>
    internal static async Task<NexusSessionReceiver?> AcceptAsync(NexusClient client, string entityPath, string? sessionId,
        TimeSpan? maxWait, NexusReceiveMode mode, CancellationToken ct)
    {
        var wait = (int)Math.Ceiling((maxWait ?? TimeSpan.FromSeconds(30)).TotalSeconds);
        var url = $"api/sessions/accept/{NexusClient.Encode(entityPath)}?wait={wait}";
        if (sessionId is not null) url += $"&sessionId={Uri.EscapeDataString(sessionId)}";
        var handle = await client.SendAsync<Handle?>(HttpMethod.Post, url, null, ct).ConfigureAwait(false);
        return handle is null ? null : new NexusSessionReceiver(client, entityPath, mode, handle);
    }

    private object Body => new { SessionId, SessionLockToken };

    public async Task<NexusReceivedMessage?> ReceiveMessageAsync(TimeSpan? maxWaitTime = null, CancellationToken ct = default) =>
        (await ReceiveMessagesAsync(1, maxWaitTime, ct).ConfigureAwait(false)).FirstOrDefault();

    /// <summary>Next messages of this session, in send order. Empty when the session has nothing more within the wait.</summary>
    public Task<List<NexusReceivedMessage>> ReceiveMessagesAsync(int maxMessages, TimeSpan? maxWaitTime = null, CancellationToken ct = default)
    {
        var wait = (int)Math.Ceiling((maxWaitTime ?? TimeSpan.FromSeconds(5)).TotalSeconds);
        return _client.SendAsync<List<NexusReceivedMessage>>(HttpMethod.Post,
            $"api/sessions/receive/{_encodedPath}?sessionId={Uri.EscapeDataString(SessionId)}&token={SessionLockToken}&max={maxMessages}&wait={wait}&mode={ReceiveMode}",
            null, ct);
    }

    public Task CompleteMessageAsync(NexusReceivedMessage m, CancellationToken ct = default) => _settler.CompleteMessageAsync(m, ct);
    public Task AbandonMessageAsync(NexusReceivedMessage m, IDictionary<string, string>? props = null, CancellationToken ct = default) => _settler.AbandonMessageAsync(m, props, ct);
    public Task DeadLetterMessageAsync(NexusReceivedMessage m, string? reason = null, string? description = null, CancellationToken ct = default) => _settler.DeadLetterMessageAsync(m, reason, description, ct);
    public Task DeferMessageAsync(NexusReceivedMessage m, CancellationToken ct = default) => _settler.DeferMessageAsync(m, ct);
    public Task<DateTimeOffset> RenewMessageLockAsync(NexusReceivedMessage m, CancellationToken ct = default) => _settler.RenewMessageLockAsync(m, ct);

    public async Task<DateTimeOffset> RenewSessionLockAsync(CancellationToken ct = default)
    {
        var r = await _client.SendAsync<Dictionary<string, DateTimeOffset?>>(HttpMethod.Post, $"api/sessions/renew/{_encodedPath}", Body, ct).ConfigureAwait(false);
        SessionLockedUntil = r.GetValueOrDefault("lockedUntilUtc") ?? SessionLockedUntil;
        return SessionLockedUntil;
    }

    /// <summary>Durable per-session state, e.g. a workflow checkpoint. Null when none was set.</summary>
    public async Task<byte[]?> GetSessionStateAsync(CancellationToken ct = default)
    {
        var r = await _client.SendAsync<Dictionary<string, string?>>(HttpMethod.Post, $"api/sessions/state/get/{_encodedPath}", Body, ct).ConfigureAwait(false);
        return r.GetValueOrDefault("stateBase64") is { } b64 ? Convert.FromBase64String(b64) : null;
    }

    public async Task<string?> GetSessionStateAsStringAsync(CancellationToken ct = default) =>
        await GetSessionStateAsync(ct).ConfigureAwait(false) is { } bytes ? Encoding.UTF8.GetString(bytes) : null;

    /// <summary>Set (or with null, clear) the session state.</summary>
    public Task SetSessionStateAsync(byte[]? state, CancellationToken ct = default) =>
        _client.SendAsync<bool>(HttpMethod.Post, $"api/sessions/state/set/{_encodedPath}", new
        {
            SessionId,
            SessionLockToken,
            StateBase64 = state is null ? null : Convert.ToBase64String(state)
        }, ct);

    public Task SetSessionStateAsync(string? state, CancellationToken ct = default) =>
        SetSessionStateAsync(state is null ? null : Encoding.UTF8.GetBytes(state), ct);

    /// <summary>Release the session lock so another receiver can take the session.</summary>
    public async Task CloseAsync(CancellationToken ct = default)
    {
        if (_released) return;
        _released = true;
        try { await _client.SendAsync<bool>(HttpMethod.Post, $"api/sessions/release/{_encodedPath}", Body, ct).ConfigureAwait(false); }
        catch (NexusClientException) { /* lock already gone */ }
    }

    public async ValueTask DisposeAsync() => await CloseAsync().ConfigureAwait(false);
}
