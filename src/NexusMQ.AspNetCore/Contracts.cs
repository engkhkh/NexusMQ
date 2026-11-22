using System.Text;
using NexusMQ.Core;

namespace NexusMQ.AspNetCore;

/// <summary>Wire format for sending. Use <c>body</c> for text/JSON or <c>bodyBase64</c> for binary.</summary>
public sealed class SendMessageRequest
{
    public string? MessageId { get; set; }
    public string? CorrelationId { get; set; }
    public string? Subject { get; set; }
    public string? ContentType { get; set; }
    public string? To { get; set; }
    public string? ReplyTo { get; set; }
    public string? SessionId { get; set; }
    public Dictionary<string, string>? Properties { get; set; }
    public string? Body { get; set; }
    public string? BodyBase64 { get; set; }
    public DateTimeOffset? ScheduledEnqueueTimeUtc { get; set; }
    public TimeSpan? TimeToLive { get; set; }
    /// <summary>Binary batch only: how many bytes of the body section belong to this message.</summary>
    public int? BodyLength { get; set; }
    /// <summary>Binary batch only: the raw body, cut from the body section.</summary>
    [System.Text.Json.Serialization.JsonIgnore] public byte[]? RawBody { get; set; }

    public OutgoingMessage ToOutgoing() => new()
    {
        MessageId = MessageId,
        CorrelationId = CorrelationId,
        Subject = Subject,
        ContentType = ContentType,
        To = To,
        ReplyTo = ReplyTo,
        SessionId = SessionId,
        ApplicationProperties = Properties,
        Body = RawBody is not null ? RawBody
             : BodyBase64 is not null ? Convert.FromBase64String(BodyBase64)
             : Body is not null ? Encoding.UTF8.GetBytes(Body) : [],
        ScheduledEnqueueTimeUtc = ScheduledEnqueueTimeUtc,
        TimeToLive = TimeToLive
    };
}

/// <summary>Wire format for received / peeked messages.</summary>
public sealed class MessageResponse
{
    public long SequenceNumber { get; init; }
    public string MessageId { get; init; } = "";
    public string? CorrelationId { get; init; }
    public string? Subject { get; init; }
    public string? ContentType { get; init; }
    public string? To { get; init; }
    public string? ReplyTo { get; init; }
    public string? SessionId { get; init; }
    public Dictionary<string, string> Properties { get; init; } = new();
    public string BodyBase64 { get; init; } = "";
    public DateTimeOffset EnqueuedTimeUtc { get; init; }
    public DateTimeOffset? ScheduledEnqueueTimeUtc { get; init; }
    public DateTimeOffset? ExpiresAtUtc { get; init; }
    public int DeliveryCount { get; init; }
    public MessageState State { get; init; }
    public string? DeadLetterReason { get; init; }
    public string? DeadLetterErrorDescription { get; init; }
    public string? DeadLetterSource { get; init; }
    public Guid? LockToken { get; init; }
    public DateTimeOffset? LockedUntilUtc { get; init; }

    public static MessageResponse From(BrokerMessage m, Guid? lockToken = null, DateTimeOffset? lockedUntil = null, bool includeBody = true) => new()
    {
        SequenceNumber = m.SequenceNumber,
        MessageId = m.MessageId,
        CorrelationId = m.CorrelationId,
        Subject = m.Subject,
        ContentType = m.ContentType,
        To = m.To,
        ReplyTo = m.ReplyTo,
        SessionId = m.SessionId,
        Properties = m.ApplicationProperties,
        BodyBase64 = includeBody ? Convert.ToBase64String(m.Body) : "",
        EnqueuedTimeUtc = m.EnqueuedTimeUtc,
        ScheduledEnqueueTimeUtc = m.ScheduledEnqueueTimeUtc,
        ExpiresAtUtc = m.ExpiresAtUtc,
        DeliveryCount = m.DeliveryCount,
        State = m.State,
        DeadLetterReason = m.DeadLetterReason,
        DeadLetterErrorDescription = m.DeadLetterErrorDescription,
        DeadLetterSource = m.DeadLetterSource,
        LockToken = lockToken,
        LockedUntilUtc = lockedUntil
    };

    public static MessageResponse From(ReceivedMessage r) => From(r.Message, r.LockToken, r.LockedUntilUtc);
}

public enum SettleAction { Complete, Abandon, DeadLetter, Defer, Renew }

public sealed class SettleRequest
{
    public Guid LockToken { get; set; }
    public SettleAction Action { get; set; }
    public string? Reason { get; set; }
    public string? Description { get; set; }
    public Dictionary<string, string>? Properties { get; set; }
}

public sealed class SettleResponse
{
    public DateTimeOffset? LockedUntilUtc { get; init; }
    public long? SequenceNumber { get; init; }
}

public sealed class CreateTopicRequest
{
    public TimeSpan? DuplicateDetectionWindow { get; set; }
}

public sealed class CreateSubscriptionRequest
{
    public QueueOptions? Options { get; set; }
    public List<SubscriptionRule>? Rules { get; set; }
}

public sealed class CreateExchangeRequest
{
    public ExchangeType Type { get; set; } = ExchangeType.Direct;
}

public sealed class ErrorResponse
{
    public required string Code { get; init; }
    public required string Message { get; init; }
}

public sealed class NexusServerOptions
{
    /// <summary>Folder for journals and topology. Relative paths are resolved against the app's content root.</summary>
    public string DataDirectory { get; set; } = "data";
    /// <summary>false = in-memory only (nothing survives a restart).</summary>
    public bool Durable { get; set; } = true;
    /// <summary>Force an OS flush on every write (safer, slower).</summary>
    public bool FlushToDisk { get; set; }
    /// <summary>
    /// How journals and the Raft log are written. <c>Binary</c> (default from 1.13): raw message bodies, smaller and faster;
    /// such files cannot be read by older versions. <c>Json</c>: the JSON-lines format of earlier versions, to keep the option
    /// of going back. Files in the other format are read either way and converted.
    /// </summary>
    public NexusMQ.Core.Persistence.StorageFormat StorageFormat { get; set; } = NexusMQ.Core.Persistence.StorageFormat.Binary;
    /// <summary>Master key with all rights. When set (or when <see cref="AccessKeys"/> has entries), every /api call needs a key in header X-Api-Key.</summary>
    public string? ApiKey { get; set; }
    /// <summary>Named keys with Send/Listen/Manage rights on selected entities (like Azure SAS policies).</summary>
    public List<NexusAccessKey> AccessKeys { get; set; } = [];
    /// <summary>Upper bound for long-poll receive waits.</summary>
    public int MaxWaitSeconds { get; set; } = 60;
    /// <summary>Primary/standby high availability.</summary>
    public NexusReplicationOptions Replication { get; set; } = new();
    /// <summary>Raft cluster (3 or 5 servers). When configured, <see cref="Replication"/> is not used.</summary>
    public NexusClusterOptions Cluster { get; set; } = new();
}

public sealed class SessionRequest
{
    public required string SessionId { get; set; }
    public Guid SessionLockToken { get; set; }
}

public sealed class SessionStateRequest
{
    public required string SessionId { get; set; }
    public Guid SessionLockToken { get; set; }
    /// <summary>Null clears the state.</summary>
    public string? StateBase64 { get; set; }
}

public sealed class SessionStateResponse
{
    public string? StateBase64 { get; init; }
}

public sealed class CreateKeyRequest
{
    public required string Name { get; set; }
    public NexusRights Rights { get; set; }
    /// <summary>Entity patterns; empty = all entities.</summary>
    public List<string>? Entities { get; set; }
    /// <summary>Optional request limit for the key.</summary>
    public double? RequestsPerSecond { get; set; }
    public int? Burst { get; set; }
}

public sealed class KeyLimitsRequest
{
    /// <summary>Null removes the limit.</summary>
    public double? RequestsPerSecond { get; set; }
    public int? Burst { get; set; }
}

public sealed class KeySecretResponse
{
    public required AccessKeyInfo Key { get; init; }
    /// <summary>The secret. It is shown only in this response; only a hash is stored.</summary>
    public required string Secret { get; init; }
}
