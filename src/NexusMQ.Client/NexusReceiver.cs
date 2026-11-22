namespace NexusMQ.Client;

/// <summary>Pulls messages from a queue, subscription or dead-letter queue (like <c>ServiceBusReceiver</c>).</summary>
public sealed class NexusReceiver : IMessageSettler
{
    private readonly NexusClient _client;
    private readonly string _encodedPath;

    internal NexusReceiver(NexusClient client, string entityPath, NexusReceiveMode mode)
    {
        _client = client;
        EntityPath = entityPath;
        ReceiveMode = mode;
        _encodedPath = NexusClient.Encode(entityPath);
    }

    public string EntityPath { get; }
    public NexusReceiveMode ReceiveMode { get; }
    internal NexusClient Client => _client;

    Task IMessageSettler.CompleteAsync(NexusReceivedMessage m, CancellationToken ct) => CompleteMessageAsync(m, ct);
    Task IMessageSettler.AbandonAsync(NexusReceivedMessage m, IDictionary<string, string>? p, CancellationToken ct) => AbandonMessageAsync(m, p, ct);
    Task IMessageSettler.DeadLetterAsync(NexusReceivedMessage m, string? r, string? d, CancellationToken ct) => DeadLetterMessageAsync(m, r, d, ct);
    Task IMessageSettler.DeferAsync(NexusReceivedMessage m, CancellationToken ct) => DeferMessageAsync(m, ct);
    Task<DateTimeOffset> IMessageSettler.RenewAsync(NexusReceivedMessage m, CancellationToken ct) => RenewMessageLockAsync(m, ct);

    /// <summary>Receive one message, waiting up to <paramref name="maxWaitTime"/> (default 30 s). Returns null if none arrived.</summary>
    public async Task<NexusReceivedMessage?> ReceiveMessageAsync(TimeSpan? maxWaitTime = null, CancellationToken ct = default) =>
        (await ReceiveMessagesAsync(1, maxWaitTime, ct).ConfigureAwait(false)).FirstOrDefault();

    /// <summary>Long-poll for up to <paramref name="maxMessages"/> messages.</summary>
    public Task<List<NexusReceivedMessage>> ReceiveMessagesAsync(int maxMessages, TimeSpan? maxWaitTime = null, CancellationToken ct = default)
    {
        var wait = (int)Math.Ceiling((maxWaitTime ?? TimeSpan.FromSeconds(30)).TotalSeconds);
        return _client.SendAsync<List<NexusReceivedMessage>>(HttpMethod.Post,
            $"api/receive/{_encodedPath}?max={maxMessages}&wait={wait}&mode={ReceiveMode}", null, ct);
    }

    /// <summary>Browse messages without locking them.</summary>
    public Task<List<NexusReceivedMessage>> PeekMessagesAsync(int maxMessages = 50, long fromSequenceNumber = 0, CancellationToken ct = default) =>
        _client.SendAsync<List<NexusReceivedMessage>>(HttpMethod.Get,
            $"api/peek/{_encodedPath}?max={maxMessages}&from={fromSequenceNumber}", null, ct);

    public Task<List<NexusReceivedMessage>> ReceiveDeferredMessagesAsync(IEnumerable<long> sequenceNumbers, CancellationToken ct = default) =>
        _client.SendAsync<List<NexusReceivedMessage>>(HttpMethod.Post, $"api/deferred/{_encodedPath}", sequenceNumbers.ToList(), ct);

    /// <summary>Acknowledge and remove the message.</summary>
    public Task CompleteMessageAsync(NexusReceivedMessage message, CancellationToken ct = default) =>
        Settle(message, "Complete", null, null, null, ct);

    /// <summary>Release the lock so the message is redelivered (counts towards MaxDeliveryCount).</summary>
    public Task AbandonMessageAsync(NexusReceivedMessage message, IDictionary<string, string>? propertiesToModify = null, CancellationToken ct = default) =>
        Settle(message, "Abandon", null, null, propertiesToModify, ct);

    public Task DeadLetterMessageAsync(NexusReceivedMessage message, string? reason = null, string? description = null, CancellationToken ct = default) =>
        Settle(message, "DeadLetter", reason, description, null, ct);

    /// <summary>Set aside; retrieve later with <see cref="ReceiveDeferredMessagesAsync"/> using the sequence number.</summary>
    public Task DeferMessageAsync(NexusReceivedMessage message, CancellationToken ct = default) =>
        Settle(message, "Defer", null, null, null, ct);

    public async Task<DateTimeOffset> RenewMessageLockAsync(NexusReceivedMessage message, CancellationToken ct = default)
    {
        var r = await SettleCore(message, "Renew", null, null, null, ct).ConfigureAwait(false);
        message.LockedUntilUtc = r.LockedUntilUtc;
        return r.LockedUntilUtc ?? DateTimeOffset.UtcNow;
    }

    private async Task Settle(NexusReceivedMessage m, string action, string? reason, string? description,
        IDictionary<string, string>? props, CancellationToken ct) =>
        await SettleCore(m, action, reason, description, props, ct).ConfigureAwait(false);

    private sealed class SettleResponse
    {
        public DateTimeOffset? LockedUntilUtc { get; set; }
        public long? SequenceNumber { get; set; }
    }

    private Task<SettleResponse> SettleCore(NexusReceivedMessage m, string action, string? reason, string? description,
        IDictionary<string, string>? props, CancellationToken ct)
    {
        if (m.LockToken is null)
            throw new InvalidOperationException("This message has no lock token (it was received in ReceiveAndDelete mode or peeked).");
        return _client.SendAsync<SettleResponse>(HttpMethod.Post, $"api/settle/{_encodedPath}", new
        {
            LockToken = m.LockToken.Value,
            Action = action,
            Reason = reason,
            Description = description,
            Properties = props
        }, ct);
    }
}
