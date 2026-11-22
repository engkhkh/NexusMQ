namespace NexusMQ.Core;

public enum NexusErrorCode
{
    BadRequest,
    EntityNotFound,
    EntityAlreadyExists,
    MessageLockLost,
    MessageNotFound,
    QuotaExceeded,
    Unauthorized,
    /// <summary>The access key is valid but lacks the right for this entity.</summary>
    Forbidden,
    /// <summary>The session is locked by another receiver.</summary>
    SessionCannotBeLocked,
    /// <summary>The session lock expired or the token is wrong.</summary>
    SessionLockLost,
    /// <summary>The access key exceeded its request rate; retry after <see cref="NexusException.RetryAfter"/>.</summary>
    RateLimited,
    /// <summary>This server is a standby; send requests to the primary.</summary>
    Standby,
    /// <summary>The change was not confirmed by a majority of the cluster in time (outcome unknown; retrying may duplicate).</summary>
    NotCommitted
}

public sealed class NexusException(NexusErrorCode code, string message) : Exception(message)
{
    public NexusErrorCode Code { get; } = code;
    /// <summary>For <see cref="NexusErrorCode.RateLimited"/>: how long to wait before retrying.</summary>
    public TimeSpan? RetryAfter { get; init; }
}
