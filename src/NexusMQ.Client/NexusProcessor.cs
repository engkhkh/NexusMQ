namespace NexusMQ.Client;

public sealed class NexusProcessorOptions
{
    /// <summary>Messages handled in parallel.</summary>
    public int MaxConcurrentCalls { get; set; } = 1;
    /// <summary>Complete on success, abandon on exception.</summary>
    public bool AutoCompleteMessages { get; set; } = true;
    /// <summary>Keep renewing the lock while the handler runs, up to this long. Zero disables.</summary>
    public TimeSpan MaxAutoLockRenewalDuration { get; set; } = TimeSpan.FromMinutes(5);
    public NexusReceiveMode ReceiveMode { get; set; } = NexusReceiveMode.PeekLock;
    /// <summary>WebSocket push (default, lowest latency) or HTTP long-polling.</summary>
    public NexusTransport Transport { get; set; } = NexusTransport.WebSocket;
    /// <summary>
    /// WebSocket only: how many unsettled messages the server may push ahead (RabbitMQ "prefetch"/QoS).
    /// 0 = same as <see cref="MaxConcurrentCalls"/>. Higher values raise throughput; buffered messages are locked while they wait.
    /// </summary>
    public int PrefetchCount { get; set; }
    /// <summary>Long-polling only: how long each receive call waits.</summary>
    public TimeSpan ReceiveWaitTime { get; set; } = TimeSpan.FromSeconds(20);
}

public sealed class ProcessMessageEventArgs
{
    private readonly IMessageSettler _settler;

    internal ProcessMessageEventArgs(NexusReceivedMessage message, IMessageSettler settler, CancellationToken ct)
    {
        Message = message;
        _settler = settler;
        CancellationToken = ct;
    }

    public NexusReceivedMessage Message { get; }
    public CancellationToken CancellationToken { get; }
    internal bool Settled { get; private set; }

    public Task CompleteMessageAsync() { Settled = true; return _settler.CompleteAsync(Message, CancellationToken); }
    public Task AbandonMessageAsync(IDictionary<string, string>? propertiesToModify = null)
    { Settled = true; return _settler.AbandonAsync(Message, propertiesToModify, CancellationToken); }
    public Task DeadLetterMessageAsync(string? reason = null, string? description = null)
    { Settled = true; return _settler.DeadLetterAsync(Message, reason, description, CancellationToken); }
    public Task DeferMessageAsync() { Settled = true; return _settler.DeferAsync(Message, CancellationToken); }
    public Task<DateTimeOffset> RenewMessageLockAsync() => _settler.RenewAsync(Message, CancellationToken);
}

public sealed class ProcessErrorEventArgs(Exception exception, string entityPath)
{
    public Exception Exception { get; } = exception;
    public string EntityPath { get; } = entityPath;
}

/// <summary>
/// Push-style consumer (like <c>ServiceBusProcessor</c> / a RabbitMQ consumer). By default the server pushes messages over a
/// WebSocket with prefetch-based flow control; set <see cref="NexusProcessorOptions.Transport"/> to LongPolling to use HTTP.
/// Reconnects automatically with back-off.
/// </summary>
public sealed class NexusProcessor : IAsyncDisposable
{
    private readonly NexusReceiver _receiver;
    private readonly NexusProcessorOptions _options;
    private CancellationTokenSource? _cts;
    private Task? _loop;

    internal NexusProcessor(NexusReceiver receiver, NexusProcessorOptions options)
    {
        _receiver = receiver;
        _options = options;
    }

    public event Func<ProcessMessageEventArgs, Task>? ProcessMessageAsync;
    public event Func<ProcessErrorEventArgs, Task>? ProcessErrorAsync;

    public bool IsProcessing => _loop is { IsCompleted: false };
    public string EntityPath => _receiver.EntityPath;

    public Task StartProcessingAsync(CancellationToken ct = default)
    {
        if (ProcessMessageAsync is null) throw new InvalidOperationException("Set ProcessMessageAsync before starting.");
        if (IsProcessing) return Task.CompletedTask;
        _cts = CancellationTokenSource.CreateLinkedTokenSource(ct);
        var token = _cts.Token;
        _loop = Task.Run(() => _options.Transport == NexusTransport.WebSocket ? RunPushAsync(token) : RunPollingAsync(token));
        return Task.CompletedTask;
    }

    public async Task StopProcessingAsync()
    {
        if (_cts is null || _loop is null) return;
        await _cts.CancelAsync().ConfigureAwait(false);
        try { await _loop.ConfigureAwait(false); } catch (OperationCanceledException) { }
        _cts.Dispose();
        _cts = null;
        _loop = null;
    }

    // ------------------------------------------------------------------ WebSocket push

    private async Task RunPushAsync(CancellationToken ct)
    {
        var concurrency = Math.Max(1, _options.MaxConcurrentCalls);
        var prefetch = _options.PrefetchCount > 0 ? _options.PrefetchCount : concurrency;
        var backoff = TimeSpan.FromSeconds(1);

        while (!ct.IsCancellationRequested)
        {
            try
            {
                await using var conn = await NexusPushConnection.ConnectAsync(_receiver.Client, ct).ConfigureAwait(false);
                var sub = await conn.SubscribeAsync(EntityPath, _options.ReceiveMode, prefetch, ct).ConfigureAwait(false);
                backoff = TimeSpan.FromSeconds(1);

                var workers = Enumerable.Range(0, concurrency).Select(_ => Task.Run(async () =>
                {
                    await foreach (var ev in sub.Reader.ReadAllAsync(ct).ConfigureAwait(false))
                    {
                        if (ev.Message is not { } m) continue;
                        await HandleAsync(m, sub, ct).ConfigureAwait(false);
                        if (m.LockToken is null && !ct.IsCancellationRequested)
                            await sub.CreditAsync(ct).ConfigureAwait(false); // ReceiveAndDelete flow control
                    }
                }, CancellationToken.None)).ToArray();

                // Ends when the connection drops (reader faults) or we are stopped.
                await Task.WhenAll(workers).ConfigureAwait(false);
            }
            catch (OperationCanceledException) when (ct.IsCancellationRequested)
            {
                break;
            }
            catch (Exception ex)
            {
                await RaiseError(ex).ConfigureAwait(false);
                try { await Task.Delay(backoff, ct).ConfigureAwait(false); } catch (OperationCanceledException) { break; }
                backoff = TimeSpan.FromSeconds(Math.Min(backoff.TotalSeconds * 2, 30));
            }
        }
    }

    // ------------------------------------------------------------------ HTTP long-polling

    private async Task RunPollingAsync(CancellationToken ct)
    {
        var slots = new SemaphoreSlim(Math.Max(1, _options.MaxConcurrentCalls));
        var running = new List<Task>();
        var backoff = TimeSpan.FromSeconds(1);

        while (!ct.IsCancellationRequested)
        {
            try
            {
                await slots.WaitAsync(ct).ConfigureAwait(false);
                var free = 1;
                while (slots.CurrentCount > 0 && slots.Wait(0)) free++;

                List<NexusReceivedMessage> batch;
                try
                {
                    batch = await _receiver.ReceiveMessagesAsync(free, _options.ReceiveWaitTime, ct).ConfigureAwait(false);
                }
                catch
                {
                    slots.Release(free);
                    throw;
                }

                if (batch.Count < free) slots.Release(free - batch.Count);
                foreach (var m in batch)
                    running.Add(Task.Run(async () =>
                    {
                        try { await HandleAsync(m, _receiver, ct).ConfigureAwait(false); }
                        finally { slots.Release(); }
                    }, CancellationToken.None));
                running.RemoveAll(t => t.IsCompleted);
                backoff = TimeSpan.FromSeconds(1);
            }
            catch (OperationCanceledException) when (ct.IsCancellationRequested)
            {
                break;
            }
            catch (Exception ex)
            {
                await RaiseError(ex).ConfigureAwait(false);
                try { await Task.Delay(backoff, ct).ConfigureAwait(false); } catch (OperationCanceledException) { break; }
                backoff = TimeSpan.FromSeconds(Math.Min(backoff.TotalSeconds * 2, 30));
            }
        }

        await Task.WhenAll(running).ConfigureAwait(false);
    }

    // ------------------------------------------------------------------ shared

    private async Task HandleAsync(NexusReceivedMessage message, IMessageSettler settler, CancellationToken ct)
    {
        var args = new ProcessMessageEventArgs(message, settler, ct);
        using var renewCts = CancellationTokenSource.CreateLinkedTokenSource(ct);
        var renewTask = message.LockToken is not null && _options.MaxAutoLockRenewalDuration > TimeSpan.Zero
            ? RenewLoopAsync(message, settler, renewCts.Token)
            : Task.CompletedTask;

        try
        {
            await ProcessMessageAsync!(args).ConfigureAwait(false);
            await renewCts.CancelAsync().ConfigureAwait(false);
            if (_options.AutoCompleteMessages && !args.Settled && message.LockToken is not null)
                await settler.CompleteAsync(message, CancellationToken.None).ConfigureAwait(false);
        }
        catch (Exception ex)
        {
            await renewCts.CancelAsync().ConfigureAwait(false);
            await RaiseError(ex).ConfigureAwait(false);
            if (_options.AutoCompleteMessages && !args.Settled && message.LockToken is not null)
            {
                try { await settler.AbandonAsync(message, null, CancellationToken.None).ConfigureAwait(false); }
                catch (Exception abandonError) { await RaiseError(abandonError).ConfigureAwait(false); }
            }
        }
        finally
        {
            try { await renewTask.ConfigureAwait(false); } catch (OperationCanceledException) { }
        }
    }

    private async Task RenewLoopAsync(NexusReceivedMessage message, IMessageSettler settler, CancellationToken ct)
    {
        var stopAt = DateTimeOffset.UtcNow + _options.MaxAutoLockRenewalDuration;
        while (!ct.IsCancellationRequested && DateTimeOffset.UtcNow < stopAt)
        {
            var until = message.LockedUntilUtc ?? DateTimeOffset.UtcNow.AddSeconds(30);
            var delay = TimeSpan.FromTicks(Math.Max((until - DateTimeOffset.UtcNow).Ticks / 2, TimeSpan.FromMilliseconds(500).Ticks));
            await Task.Delay(delay, ct).ConfigureAwait(false);
            try { await settler.RenewAsync(message, ct).ConfigureAwait(false); }
            catch (OperationCanceledException) { throw; }
            catch (Exception ex) { await RaiseError(ex).ConfigureAwait(false); return; }
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
