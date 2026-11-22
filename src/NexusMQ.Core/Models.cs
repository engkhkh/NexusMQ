namespace NexusMQ.Core;

/// <summary>Lifecycle state of a message inside an entity.</summary>
public enum MessageState
{
    Active,
    Locked,
    Scheduled,
    Deferred
}

/// <summary>How a receiver takes messages from an entity.</summary>
public enum ReceiveMode
{
    /// <summary>Message is locked; the receiver must Complete / Abandon / DeadLetter / Defer it (RabbitMQ manual ack).</summary>
    PeekLock,
    /// <summary>Message is removed as soon as it is handed out (RabbitMQ auto-ack).</summary>
    ReceiveAndDelete
}

/// <summary>RabbitMQ-style exchange types.</summary>
public enum ExchangeType
{
    /// <summary>Routes when the binding key equals the routing key.</summary>
    Direct,
    /// <summary>Routes to every bound destination.</summary>
    Fanout,
    /// <summary>Routes by dotted pattern: '*' = one word, '#' = zero or more words.</summary>
    Topic,
    /// <summary>Routes by application properties; binding argument "x-match" = "all" (default) or "any".</summary>
    Headers
}

/// <summary>A message as stored by the broker.</summary>
public sealed class BrokerMessage
{
    public long SequenceNumber { get; set; }
    public string MessageId { get; set; } = Guid.NewGuid().ToString("N");
    public string? CorrelationId { get; set; }
    /// <summary>Label / routing key (Azure "Subject", RabbitMQ routing key).</summary>
    public string? Subject { get; set; }
    public string? ContentType { get; set; }
    public string? To { get; set; }
    public string? ReplyTo { get; set; }
    public string? SessionId { get; set; }
    public Dictionary<string, string> ApplicationProperties { get; set; } = new();
    public byte[] Body { get; set; } = [];

    public DateTimeOffset EnqueuedTimeUtc { get; set; }
    public DateTimeOffset? ScheduledEnqueueTimeUtc { get; set; }
    public TimeSpan? TimeToLive { get; set; }
    public DateTimeOffset? ExpiresAtUtc { get; set; }
    public int DeliveryCount { get; set; }
    public MessageState State { get; set; }

    public string? DeadLetterReason { get; set; }
    public string? DeadLetterErrorDescription { get; set; }
    public string? DeadLetterSource { get; set; }

    /// <summary>Cluster mode: the Raft log index that must be committed before this message may be delivered (0 = none).</summary>
    internal long RequiredCommitIndex { get; set; }

    public BrokerMessage Clone() => new()
    {
        SequenceNumber = SequenceNumber,
        MessageId = MessageId,
        CorrelationId = CorrelationId,
        Subject = Subject,
        ContentType = ContentType,
        To = To,
        ReplyTo = ReplyTo,
        SessionId = SessionId,
        ApplicationProperties = new Dictionary<string, string>(ApplicationProperties),
        Body = Body,
        EnqueuedTimeUtc = EnqueuedTimeUtc,
        ScheduledEnqueueTimeUtc = ScheduledEnqueueTimeUtc,
        TimeToLive = TimeToLive,
        ExpiresAtUtc = ExpiresAtUtc,
        DeliveryCount = DeliveryCount,
        State = State,
        DeadLetterReason = DeadLetterReason,
        DeadLetterErrorDescription = DeadLetterErrorDescription,
        DeadLetterSource = DeadLetterSource
    };
}

/// <summary>A message a producer wants to send.</summary>
public sealed class OutgoingMessage
{
    public string? MessageId { get; set; }
    public string? CorrelationId { get; set; }
    public string? Subject { get; set; }
    public string? ContentType { get; set; }
    public string? To { get; set; }
    public string? ReplyTo { get; set; }
    public string? SessionId { get; set; }
    public Dictionary<string, string>? ApplicationProperties { get; set; }
    public byte[]? Body { get; set; }
    public DateTimeOffset? ScheduledEnqueueTimeUtc { get; set; }
    public TimeSpan? TimeToLive { get; set; }
}

/// <summary>A message handed to a consumer.</summary>
public sealed class ReceivedMessage
{
    public required BrokerMessage Message { get; init; }
    /// <summary>Lock token for PeekLock receives (null for ReceiveAndDelete and Peek).</summary>
    public Guid? LockToken { get; init; }
    public DateTimeOffset? LockedUntilUtc { get; init; }
}

/// <summary>Settings shared by queues and subscriptions.</summary>
public sealed class QueueOptions
{
    public TimeSpan LockDuration { get; set; } = TimeSpan.FromSeconds(30);
    public int MaxDeliveryCount { get; set; } = 10;
    public TimeSpan? DefaultTimeToLive { get; set; }
    public bool DeadLetterOnExpiration { get; set; } = true;
    /// <summary>When set, a message whose MessageId was seen within this window is silently dropped.</summary>
    public TimeSpan? DuplicateDetectionWindow { get; set; }
    /// <summary>0 = unlimited.</summary>
    public int MaxSizeInMessages { get; set; }
    /// <summary>
    /// Every message must carry a SessionId; receivers lock a whole session and get its messages in order
    /// (Azure Service Bus sessions — FIFO per customer/order/device).
    /// </summary>
    public bool RequiresSession { get; set; }
    /// <summary>Auto-forward: every message arriving here is passed on to this queue, topic or "exchanges/name" instead of being stored.</summary>
    public string? ForwardTo { get; set; }
    /// <summary>Send dead-lettered messages to this queue/topic instead of this entity's own $deadletter queue.</summary>
    public string? ForwardDeadLetteredMessagesTo { get; set; }
}

/// <summary>A session lock held by one receiver.</summary>
public sealed class SessionHandle
{
    public required string SessionId { get; init; }
    public required Guid SessionLockToken { get; init; }
    public required DateTimeOffset LockedUntilUtc { get; init; }
}

/// <summary>Azure-style correlation filter. Every non-null field must match. SubjectPattern uses topic wildcards (* and #).</summary>
public sealed class SubscriptionRule
{
    public string Name { get; set; } = "$Default";
    public string? CorrelationId { get; set; }
    public string? Subject { get; set; }
    public string? SubjectPattern { get; set; }
    /// <summary>
    /// Azure-style SQL filter, e.g. <c>region = 'eu' AND amount &gt; 100 OR sys.Subject LIKE 'order.%'</c>.
    /// Combined (AND) with the other fields when both are set.
    /// </summary>
    public string? SqlFilter { get; set; }
    /// <summary>Modifies the delivered copy when the rule matches, e.g. <c>SET priority = 'high'; REMOVE debug</c>.</summary>
    public string? SqlAction { get; set; }
    public string? To { get; set; }
    public string? ContentType { get; set; }
    public Dictionary<string, string>? Properties { get; set; }

    public static SubscriptionRule MatchAll => new() { Name = "$Default" };
}

public sealed class QueueDefinition
{
    public required string Name { get; set; }
    public QueueOptions Options { get; set; } = new();
}

public sealed class SubscriptionDefinition
{
    public required string Name { get; set; }
    public QueueOptions Options { get; set; } = new();
    public List<SubscriptionRule> Rules { get; set; } = [SubscriptionRule.MatchAll];
}

public sealed class TopicDefinition
{
    public required string Name { get; set; }
    public TimeSpan? DuplicateDetectionWindow { get; set; }
    public List<SubscriptionDefinition> Subscriptions { get; set; } = [];
}

public sealed class ExchangeDefinition
{
    public required string Name { get; set; }
    public ExchangeType Type { get; set; } = ExchangeType.Direct;
}

public sealed class BindingDefinition
{
    public required string Exchange { get; set; }
    /// <summary>Destination entity: a queue name, a topic name, or another exchange as "exchanges/{name}".</summary>
    public required string Destination { get; set; }
    public string RoutingKey { get; set; } = "";
    public Dictionary<string, string>? Arguments { get; set; }
}

public sealed class TopologySnapshot
{
    public List<QueueDefinition> Queues { get; set; } = [];
    public List<TopicDefinition> Topics { get; set; } = [];
    public List<ExchangeDefinition> Exchanges { get; set; } = [];
    public List<BindingDefinition> Bindings { get; set; } = [];
}

public sealed class EntityStats
{
    public required string Path { get; init; }
    public required string Kind { get; init; }
    public int Active { get; init; }
    public int Locked { get; init; }
    public int Scheduled { get; init; }
    public int Deferred { get; init; }
    public int DeadLetter { get; init; }
    /// <summary>Sessions currently locked by a receiver.</summary>
    public int LockedSessions { get; init; }
    public long TotalEnqueued { get; init; }
    public long TotalCompleted { get; init; }
}

public sealed class BrokerOptions
{
    /// <summary>How often lock expiry, TTL and scheduled messages are processed. Null disables the background timer (call Sweep manually).</summary>
    public TimeSpan? SweepInterval { get; set; } = TimeSpan.FromSeconds(1);

    /// <summary>
    /// Cluster mode: returns the committed Raft index. When set, a message is delivered only once the log entry that
    /// stored it is committed (call <see cref="Broker.NotifyCommitted"/> when the index moves).
    /// </summary>
    public Func<long>? CommittedIndex { get; set; }
}
