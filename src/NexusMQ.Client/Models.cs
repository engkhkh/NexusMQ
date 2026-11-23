using System.Text;
using System.Text.Json;
using System.Text.Json.Serialization;

namespace NexusMQ.Client;

public enum NexusReceiveMode { PeekLock, ReceiveAndDelete }

public enum NexusExchangeType { Direct, Fanout, Topic, Headers }

/// <summary>A message to send.</summary>
public sealed class NexusMessage
{
    public NexusMessage() { }
    public NexusMessage(byte[] body) => Body = body;
    public NexusMessage(string body) => Body = Encoding.UTF8.GetBytes(body);

    /// <summary>Create a message whose body is <paramref name="value"/> serialized as JSON.</summary>
    public static NexusMessage FromJson<T>(T value, JsonSerializerOptions? options = null) =>
        new(JsonSerializer.SerializeToUtf8Bytes(value, options ?? NexusJson.Options)) { ContentType = "application/json" };

    /// <summary>
    /// Identifies the message for duplicate detection. Set when the message is created, so a send that is retried (after a
    /// timeout or a failover) carries the same id and a queue with duplicate detection keeps only one copy. Set your own
    /// (e.g. an order number) to make repeated sends of the same business event idempotent.
    /// </summary>
    public string? MessageId { get; set; } = Guid.NewGuid().ToString("N");
    public string? CorrelationId { get; set; }
    /// <summary>Label; also the default routing key when publishing to an exchange.</summary>
    public string? Subject { get; set; }
    public string? ContentType { get; set; }
    public string? To { get; set; }
    public string? ReplyTo { get; set; }
    public string? SessionId { get; set; }
    public Dictionary<string, string> ApplicationProperties { get; set; } = new();
    public byte[] Body { get; set; } = [];
    public DateTimeOffset? ScheduledEnqueueTime { get; set; }
    public TimeSpan? TimeToLive { get; set; }
}

public enum NexusMessageState { Active, Locked, Scheduled, Deferred }

/// <summary>A message received from the broker.</summary>
public sealed class NexusReceivedMessage
{
    public long SequenceNumber { get; init; }
    public string MessageId { get; init; } = "";
    public string? CorrelationId { get; init; }
    public string? Subject { get; init; }
    public string? ContentType { get; init; }
    public string? To { get; init; }
    public string? ReplyTo { get; init; }
    public string? SessionId { get; init; }
    [JsonPropertyName("properties")]
    public Dictionary<string, string> ApplicationProperties { get; init; } = new();
    /// <summary>The body as base64 (as sent over JSON). Made on demand when the body arrived as raw bytes.</summary>
    public string BodyBase64
    {
        get => _bodyBase64 ??= _body is null ? "" : Convert.ToBase64String(_body);
        init => (_bodyBase64, _body) = (value, null);
    }
    public DateTimeOffset EnqueuedTimeUtc { get; init; }
    public DateTimeOffset? ScheduledEnqueueTimeUtc { get; init; }
    public DateTimeOffset? ExpiresAtUtc { get; init; }
    public int DeliveryCount { get; init; }
    public NexusMessageState State { get; init; }
    public string? DeadLetterReason { get; init; }
    public string? DeadLetterErrorDescription { get; init; }
    public string? DeadLetterSource { get; init; }
    public Guid? LockToken { get; init; }
    public DateTimeOffset? LockedUntilUtc { get; set; }

    private string? _bodyBase64;
    private byte[]? _body;

    /// <summary>The message body (decoded once, then reused).</summary>
    [JsonIgnore] public byte[] Body => _body ??= Convert.FromBase64String(_bodyBase64 ?? "");

    /// <summary>Push with binary bodies: the raw body came next to the JSON header.</summary>
    internal void SetBody(byte[] body) => (_body, _bodyBase64) = (body, null);
    [JsonIgnore] public string BodyAsString => Encoding.UTF8.GetString(Body);
    public T? BodyAs<T>(JsonSerializerOptions? options = null) => JsonSerializer.Deserialize<T>(Body, options ?? NexusJson.Options);

    public override string ToString() => $"#{SequenceNumber} {Subject} ({MessageId})";
}

public sealed class NexusQueueOptions
{
    public TimeSpan LockDuration { get; set; } = TimeSpan.FromSeconds(30);
    public int MaxDeliveryCount { get; set; } = 10;
    public TimeSpan? DefaultTimeToLive { get; set; }
    public bool DeadLetterOnExpiration { get; set; } = true;
    public TimeSpan? DuplicateDetectionWindow { get; set; }
    public int MaxSizeInMessages { get; set; }
    /// <summary>Every message needs a SessionId; consumers lock a session and get its messages in order.</summary>
    public bool RequiresSession { get; set; }
    /// <summary>Auto-forward every arriving message to this queue, topic or "exchanges/name".</summary>
    public string? ForwardTo { get; set; }
    /// <summary>Send dead-lettered messages here instead of this entity's own $deadletter queue.</summary>
    public string? ForwardDeadLetteredMessagesTo { get; set; }
}

/// <summary>Correlation filter; every non-null field must match. SubjectPattern supports '*' and '#'.</summary>
public sealed class NexusSubscriptionRule
{
    public string Name { get; set; } = "rule";
    public string? CorrelationId { get; set; }
    public string? Subject { get; set; }
    public string? SubjectPattern { get; set; }
    /// <summary>SQL filter, e.g. <c>region = 'eu' AND amount &gt; 100</c>.</summary>
    public string? SqlFilter { get; set; }
    /// <summary>Changes the delivered copy when the rule matches, e.g. <c>SET priority = 'high'; REMOVE debug</c>.</summary>
    public string? SqlAction { get; set; }
    public string? To { get; set; }
    public string? ContentType { get; set; }
    public Dictionary<string, string>? Properties { get; set; }
}

public sealed class NexusBinding
{
    public required string Exchange { get; set; }
    public required string Destination { get; set; }
    public string RoutingKey { get; set; } = "";
    public Dictionary<string, string>? Arguments { get; set; }
}

public sealed class NexusEntityStats
{
    public string Path { get; init; } = "";
    public string Kind { get; init; } = "";
    public int Active { get; init; }
    public int Locked { get; init; }
    public int Scheduled { get; init; }
    public int Deferred { get; init; }
    public int DeadLetter { get; init; }
    public int LockedSessions { get; init; }
    public long TotalEnqueued { get; init; }
    public long TotalCompleted { get; init; }

    public override string ToString() =>
        $"{Path,-40} active={Active} locked={Locked} scheduled={Scheduled} deferred={Deferred} dlq={DeadLetter}";
}

public sealed class NexusSendResult
{
    public int Accepted { get; init; }
    public int Delivered { get; init; }
    public List<long> SequenceNumbers { get; init; } = [];
}

/// <summary>How a processor gets messages from the server.</summary>
public enum NexusTransport
{
    /// <summary>Server pushes messages over a WebSocket as soon as they arrive (lowest latency). Default.</summary>
    WebSocket,
    /// <summary>HTTP long-polling. Works through any proxy that blocks WebSockets.</summary>
    LongPolling
}

/// <summary>Settlement operations shared by HTTP receivers and WebSocket subscriptions.</summary>
internal interface IMessageSettler
{
    Task CompleteAsync(NexusReceivedMessage m, CancellationToken ct);
    Task AbandonAsync(NexusReceivedMessage m, IDictionary<string, string>? properties, CancellationToken ct);
    Task DeadLetterAsync(NexusReceivedMessage m, string? reason, string? description, CancellationToken ct);
    Task DeferAsync(NexusReceivedMessage m, CancellationToken ct);
    Task<DateTimeOffset> RenewAsync(NexusReceivedMessage m, CancellationToken ct);
}

/// <summary>Client settings.</summary>
public sealed class NexusClientOptions
{
    /// <summary>HTTP request timeout (long-poll waits must fit inside it).</summary>
    public TimeSpan RequestTimeout { get; set; } = TimeSpan.FromSeconds(100);
    /// <summary>How many times a call that hits the key's rate limit is retried after the server's Retry-After. 0 = fail at once.</summary>
    public int MaxRateLimitRetries { get; set; } = 3;
    /// <summary>
    /// Move message bodies as raw bytes instead of base64 inside JSON, which is much cheaper for large bodies: sends go as a
    /// binary batch (<c>application/vnd.nexusmq.batch</c>) and push delivers binary frames. Safe to leave on with older
    /// servers: before 1.11 push simply stays JSON, and a server before 1.12 answers a binary send with 415, after which this
    /// client sends JSON to it (and tries binary again 5 minutes later).
    /// </summary>
    public bool BinaryBodies { get; set; } = true;
    /// <summary>
    /// DEVELOPMENT ONLY: accept any TLS certificate (self-signed, wrong host name). Never enable in production:
    /// it removes protection against someone intercepting the connection.
    /// </summary>
    public bool AllowUntrustedCertificates { get; set; }
    /// <summary>
    /// When every server is unreachable or a standby (a failover in progress), keep retrying for this long before failing.
    /// Default: 30 s when several server URLs are given, 0 for a single server.
    /// </summary>
    public TimeSpan? FailoverWaitTime { get; set; }
}

public sealed class NexusClientException(string code, int statusCode, string message) : Exception(message)
{
    public string Code { get; } = code;
    public int StatusCode { get; } = statusCode;
    /// <summary>For RateLimited: how long the server asked to wait.</summary>
    public TimeSpan? RetryAfter { get; init; }
    public bool IsLockLost => Code is "MessageLockLost" or "SessionLockLost";
}

internal static class NexusJson
{
    public static readonly JsonSerializerOptions Options = new(JsonSerializerDefaults.Web)
    {
        DefaultIgnoreCondition = JsonIgnoreCondition.WhenWritingNull,
        Converters = { new JsonStringEnumConverter() }
    };
}

/// <summary>What an access key may do.</summary>
[Flags]
public enum NexusKeyRights
{
    None = 0,
    Send = 1,
    Listen = 2,
    Manage = 4,
    Monitor = 8,
    All = Send | Listen | Manage | Monitor
}

public sealed class NexusAccessKeyInfo
{
    public string Name { get; init; } = "";
    public NexusKeyRights Rights { get; init; }
    public List<string> Entities { get; init; } = [];
    /// <summary>"config" or "api".</summary>
    public string Source { get; init; } = "";
    public DateTimeOffset? CreatedUtc { get; init; }
    public DateTimeOffset? RotatedUtc { get; init; }
    public DateTimeOffset? PreviousKeyExpiresUtc { get; init; }
    public DateTimeOffset? LastUsedUtc { get; init; }
    public double? RequestsPerSecond { get; init; }
    public int? Burst { get; init; }

    public override string ToString() =>
        $"{Name,-24} {Rights,-28} {(Entities.Count == 0 ? "*" : string.Join(",", Entities)),-30} {Source}";
}

/// <summary>A newly created or rotated key. <see cref="Secret"/> is returned only once; store it safely.</summary>
public sealed class NexusKeySecret
{
    public NexusAccessKeyInfo Key { get; init; } = new();
    public string Secret { get; init; } = "";
}

/// <summary>One entry of the server's audit log.</summary>
public sealed class NexusAuditEntry
{
    public DateTimeOffset TimeUtc { get; init; }
    public string Actor { get; init; } = "";
    public string Action { get; init; } = "";
    public string? Target { get; init; }
    /// <summary>"ok", "denied" or "error:Code".</summary>
    public string Result { get; init; } = "";
    public string? Detail { get; init; }
    public string? RemoteIp { get; init; }

    public override string ToString() => $"{TimeUtc.ToLocalTime():T} {Result,-8} {Actor,-20} {Action} {Target}";
}
