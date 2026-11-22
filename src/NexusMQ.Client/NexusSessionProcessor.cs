namespace NexusMQ.Client;

public sealed class NexusSessionProcessorOptions
{
    /// <summary>Sessions processed in parallel. Messages inside one session are always handled one at a time, in order.</summary>
    public int MaxConcurrentSessions { get; set; } = 8;
    /// <summary>Release a session after it has had no messages for this long, so the slot can take another session.</summary>
    public TimeSpan SessionIdleTimeout { get; set; } = TimeSpan.FromSeconds(5);
    public bool AutoCompleteMessages { get; set; } = true;
    public NexusReceiveMode ReceiveMode { get; set; } = NexusReceiveMode.PeekLock;
    /// <summary>WebSocket push (default) or HTTP long-polling.</summary>
    public NexusTransport Transport { get; set; } = NexusTransport.WebSocket;
    /// <summary>
    /// WebSocket only: messages of the current session the server may push ahead. Keep 1 (default) for strict ordering
    /// even when a message is abandoned and retried; raise it for throughput when retries are rare.
    /// </summary>
    public int PrefetchCount { get; set; } = 1;
    /// <summary>Long-polling only: wait when looking for the next free session.</summary>
    public TimeSpan AcceptWaitTime { get; set; } = TimeSpan.FromSeconds(20);
}

public sealed class ProcessSessionMessageEventArgs
{
    private readonly IMessageSettler _settler;

    internal ProcessSessionMessageEventArgs(NexusReceivedMessage message, NexusSessionReceiver session, IMessageSettler settler, CancellationToken ct)
    {
        Message = message;
        Session = session;
        _settler = settler;
        CancellationToken = ct;
    }

    public NexusReceivedMessage Message { get; }
    /// <summary>The locked session: use it for session state (<c>GetSessionStateAsync</c> / <c>SetSessionStateAsync</c>).</summary>
    public NexusSessionReceiver Session { get; }
    public CancellationToken CancellationToken { get; }
    internal bool Settled { get; private set; }

    public Task CompleteMessageAsync() { Settled = true; return _settler.CompleteAsync(Message, CancellationToken); }
    public Task AbandonMessageAsync() { Settled = true; return _settler.AbandonAsync(Message, null, CancellationToken); }
    public Task DeadLetterMessageAsync(string? reason = null, string? description = null)
    { Settled = true; return _settler.DeadLetterAsync(Message, reason, description, CancellationToken); }
    public Task DeferMessageAsync() { Settled = true; return _settler.DeferAsync(Message, CancellationToken); }
}

/// <summary>
/// Processes a session-enabled queue or subscription (like <c>ServiceBusSessionProcessor</c>):
/// many sessions in parallel, each session strictly in order. Uses WebSocket push by default, where the server
/// hands out sessions and keeps their locks renewed.
/// </summary>
public sealed class NexusSessionProcessor : IAsyncDisposable
{
    private readonly NexusClient _client;
    private readonly NexusSessionProcessorOptions _options;
    private CancellationTokenSource? _cts;
    private Task[]? _workers;

    internal NexusSessionProcessor(NexusClient client, string entityPath, NexusSessionProcessorOptions options)
    {
        _client = client;
        EntityPath = entityPath;
        _options = options;
    }

    public string EntityPath { get; }
    public bool IsProcessing => _workers is not null;

    public event Func<ProcessSessionMessageEventArgs, Task>? ProcessMessageAsync;
    public event Func<ProcessErrorEventArgs, Task>? ProcessErrorAsync;

    public Task StartProcessingAsync(CancellationToken ct = default)
    {
        if (ProcessMessageAsync is null) throw new InvalidOperationException("Set ProcessMessageAsync before starting.");
        if (IsProcessing) return Task.CompletedTask;
        _cts = CancellationTokenSource.CreateLinkedTokenSource(ct);
        var token = _cts.Token;
        _workers = _options.Transport == NexusTransport.WebSocket
            ? [Task.Run(() => RunPushAsync(token))]
            : Enumerable.Range(0, Math.Max(1, _options.MaxConcurrentSessions)).Select(_ => Task.Run(() => PollingWorkerAsync(token))).ToArray();
        return Task.CompletedTask;
    }

    public async Task StopProcessingAsync()
    {
        if (_cts is null || _workers is null) return;
        await _cts.CancelAsync().ConfigureAwait(false);
        try { await Task.WhenAll(_workers).ConfigureAwait(false); } catch (OperationCanceledException) { }
        _cts.Dispose();
        _cts = null;
        _workers = null;
    }

    // ------------------------------------------------------------------ WebSocket push

    private async Task RunPushAsync(CancellationToken ct)
    {
        var backoff = TimeSpan.FromSeconds(1);
        while (!ct.IsCancellationRequested)
        {
            try
            {
                await using var conn = await NexusPushConnection.ConnectAsync(_client, ct).ConfigureAwait(false);
                var slots = new List<Task>();
                for (var i = 0; i < Math.Max(1, _options.MaxConcurrentSessions); i++)
                {
                    var sub = await conn.SubscribeSessionsAsync(EntityPath, _options.ReceiveMode, Math.Max(1, _options.PrefetchCount),
                        _options.SessionIdleTimeout, null, ct).ConfigureAwait(false);
                    slots.Add(Task.Run(() => SlotAsync(sub, ct), CancellationToken.None));
                }
                backoff = TimeSpan.FromSeconds(1);
                await Task.WhenAll(slots).ConfigureAwait(false); // ends when the connection drops or we stop
            }
            catch (OperationCanceledException) when (ct.IsCancellationRequested)
            {
                return;
            }
            catch (Exception ex)
            {
                await RaiseError(ex).ConfigureAwait(false);
                try { await Task.Delay(backoff, ct).ConfigureAwait(false); } catch (OperationCanceledException) { return; }
                backoff = TimeSpan.FromSeconds(Math.Min(backoff.TotalSeconds * 2, 30));
            }
        }
    }

    /// <summary>One slot = one session at a time; its messages are handled strictly one after another.</summary>
    private async Task SlotAsync(NexusPushSubscription sub, CancellationToken ct)
    {
        NexusSessionReceiver? session = null;
        await foreach (var ev in sub.Reader.ReadAllAsync(ct).ConfigureAwait(false))
        {
            if (ev.SessionClosed) { session = null; continue; }
            if (ev.SessionLockToken is { } token)
            {
                session = NexusSessionReceiver.FromPush(_client, EntityPath, _options.ReceiveMode, ev.SessionId!, token, ev.LockedUntilUtc ?? DateTimeOffset.UtcNow);
                continue;
            }
            if (ev.Message is not { } m || session is null) continue;

            await HandleAsync(new ProcessSessionMessageEventArgs(m, session, sub, ct), m, sub).ConfigureAwait(false);
            if (m.LockToken is null && !ct.IsCancellationRequested) await sub.CreditAsync(ct).ConfigureAwait(false);
        }
    }

    // ------------------------------------------------------------------ HTTP long-polling

    private async Task PollingWorkerAsync(CancellationToken ct)
    {
        var backoff = TimeSpan.FromSeconds(1);
        while (!ct.IsCancellationRequested)
        {
            try
            {
                await using var session = await NexusSessionReceiver
                    .AcceptAsync(_client, EntityPath, null, _options.AcceptWaitTime, _options.ReceiveMode, ct).ConfigureAwait(false);
                if (session is null) continue;
                await ProcessPolledSessionAsync(session, ct).ConfigureAwait(false);
                backoff = TimeSpan.FromSeconds(1);
            }
            catch (OperationCanceledException) when (ct.IsCancellationRequested)
            {
                return;
            }
            catch (Exception ex)
            {
                await RaiseError(ex).ConfigureAwait(false);
                try { await Task.Delay(backoff, ct).ConfigureAwait(false); } catch (OperationCanceledException) { return; }
                backoff = TimeSpan.FromSeconds(Math.Min(backoff.TotalSeconds * 2, 30));
            }
        }
    }

    private async Task ProcessPolledSessionAsync(NexusSessionReceiver session, CancellationToken ct)
    {
        var settler = new SessionReceiverSettler(session);
        while (!ct.IsCancellationRequested)
        {
            if (session.SessionLockedUntil - DateTimeOffset.UtcNow < TimeSpan.FromSeconds(10))
                await session.RenewSessionLockAsync(ct).ConfigureAwait(false);

            var message = await session.ReceiveMessageAsync(_options.SessionIdleTimeout, ct).ConfigureAwait(false);
            if (message is null) return; // idle → release and look for another session

            await HandleAsync(new ProcessSessionMessageEventArgs(message, session, settler, ct), message, settler).ConfigureAwait(false);
        }
    }

    private sealed class SessionReceiverSettler(NexusSessionReceiver s) : IMessageSettler
    {
        public Task CompleteAsync(NexusReceivedMessage m, CancellationToken ct) => s.CompleteMessageAsync(m, ct);
        public Task AbandonAsync(NexusReceivedMessage m, IDictionary<string, string>? p, CancellationToken ct) => s.AbandonMessageAsync(m, p, ct);
        public Task DeadLetterAsync(NexusReceivedMessage m, string? r, string? d, CancellationToken ct) => s.DeadLetterMessageAsync(m, r, d, ct);
        public Task DeferAsync(NexusReceivedMessage m, CancellationToken ct) => s.DeferMessageAsync(m, ct);
        public Task<DateTimeOffset> RenewAsync(NexusReceivedMessage m, CancellationToken ct) => s.RenewMessageLockAsync(m, ct);
    }

    // ------------------------------------------------------------------ shared

    private async Task HandleAsync(ProcessSessionMessageEventArgs args, NexusReceivedMessage message, IMessageSettler settler)
    {
        try
        {
            await ProcessMessageAsync!(args).ConfigureAwait(false);
            if (_options.AutoCompleteMessages && !args.Settled && message.LockToken is not null)
                await settler.CompleteAsync(message, CancellationToken.None).ConfigureAwait(false);
        }
        catch (OperationCanceledException) when (args.CancellationToken.IsCancellationRequested)
        {
            throw;
        }
        catch (Exception ex)
        {
            await RaiseError(ex).ConfigureAwait(false);
            if (_options.AutoCompleteMessages && !args.Settled && message.LockToken is not null)
            {
                try { await settler.AbandonAsync(message, null, CancellationToken.None).ConfigureAwait(false); }
                catch (Exception abandonError) { await RaiseError(abandonError).ConfigureAwait(false); }
            }
        }
    }

    private async Task RaiseError(Exception ex)
    {
        if (ProcessErrorAsync is null) return;
        try { await ProcessErrorAsync(new ProcessErrorEventArgs(ex, EntityPath)).ConfigureAwait(false); }
        catch { /* never let the error handler crash the loop */ }
    }

    public async ValueTask DisposeAsync() => await StopProcessingAsync().ConfigureAwait(false);
}
