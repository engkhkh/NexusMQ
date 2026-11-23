using NexusMQ.Core.Persistence;

namespace NexusMQ.Core;

/// <summary>
/// A single message container: a queue, a subscription, or a dead-letter queue.
/// All state is guarded by one lock; long-poll receivers wait on a signal that is completed on every enqueue.
/// </summary>
public sealed class MessageEntity
{
    private sealed class LockEntry(BrokerMessage message, DateTimeOffset lockedUntil)
    {
        public BrokerMessage Message { get; } = message;
        public DateTimeOffset LockedUntil { get; set; } = lockedUntil;
    }

    private readonly Lock _gate = new();
    private readonly IMessageStore _store;
    private readonly TimeProvider _time;
    private readonly BrokerMetrics _metrics;

    private readonly SortedDictionary<long, BrokerMessage> _active = new();
    private readonly Dictionary<Guid, LockEntry> _locked = new();
    private readonly SortedDictionary<long, BrokerMessage> _scheduled = new();
    private readonly SortedDictionary<long, BrokerMessage> _deferred = new();
    private readonly Dictionary<string, DateTimeOffset> _seenMessageIds = new(StringComparer.Ordinal);

    private sealed class SessionLock(Guid token, DateTimeOffset until)
    {
        public Guid Token { get; } = token;
        public DateTimeOffset Until { get; set; } = until;
    }

    private readonly Dictionary<string, SessionLock> _sessionLocks = new(StringComparer.Ordinal);
    private readonly Dictionary<string, BrokerMessage> _sessionState = new(StringComparer.Ordinal);
    private long _nextStateSequence = 1;

    private TaskCompletionSource _signal = NewSignal();
    private long _nextSequence = 1;
    private long _totalEnqueued;
    private long _totalCompleted;

    public string Path { get; }
    public string Kind { get; }
    public QueueOptions Options { get; set; }
    public bool IsDeadLetterQueue { get; }
    public MessageEntity? DeadLetterQueue { get; }

    /// <summary>Set by the broker: routes a dead-lettered message to <see cref="QueueOptions.ForwardDeadLetteredMessagesTo"/>. Returns false if it could not.</summary>
    internal Func<string, BrokerMessage, bool>? DeadLetterForwarder { get; set; }

    private readonly Func<long>? _committedIndex;

    internal MessageEntity(string path, string kind, QueueOptions options, IMessageStore store, TimeProvider time,
        BrokerMetrics? metrics = null, bool isDeadLetter = false, Func<long>? committedIndex = null)
    {
        _committedIndex = committedIndex;
        Path = path;
        Kind = kind;
        Options = options;
        _store = store;
        _time = time;
        _metrics = metrics ?? new BrokerMetrics();
        IsDeadLetterQueue = isDeadLetter;
        if (!isDeadLetter)
            DeadLetterQueue = new MessageEntity(path + "/$deadletter", "deadletter",
                new QueueOptions { MaxDeliveryCount = int.MaxValue, DeadLetterOnExpiration = false }, store, time, _metrics, isDeadLetter: true,
                committedIndex: committedIndex);
        Restore();
    }

    private static TaskCompletionSource NewSignal() => new(TaskCreationOptions.RunContinuationsAsynchronously);

    private void Restore()
    {
        foreach (var m in _store.Load(Path))
        {
            _nextSequence = Math.Max(_nextSequence, m.SequenceNumber + 1);
            switch (m.State)
            {
                case MessageState.Scheduled: _scheduled[m.SequenceNumber] = m; break;
                case MessageState.Deferred: _deferred[m.SequenceNumber] = m; break;
                default:
                    m.State = MessageState.Active;
                    _active[m.SequenceNumber] = m;
                    break;
            }
        }
        foreach (var st in _store.Load(StatePath))
        {
            _nextStateSequence = Math.Max(_nextStateSequence, st.SequenceNumber + 1);
            if (st.SessionId is not null) _sessionState[st.SessionId] = st;
        }
        // Duplicate detection must survive a restart or a cluster leader change: rebuild its history from the stored
        // messages still inside the window. Otherwise a client retrying a send after a failover would get a second copy
        // of a message the old leader had already stored.
        if (Options.DuplicateDetectionWindow is { } window)
        {
            var now = _time.GetUtcNow();
            foreach (var m in _active.Values.Concat(_scheduled.Values).Concat(_deferred.Values))
                if (!string.IsNullOrEmpty(m.MessageId) && now - m.EnqueuedTimeUtc < window)
                    _seenMessageIds[m.MessageId] = m.EnqueuedTimeUtc;
        }
    }

    private string StatePath => Path + "/$sessionstate";

    private void Signal()
    {
        var old = _signal;
        _signal = NewSignal();
        old.TrySetResult();
    }

    private int CountUnsafe() => _active.Count + _locked.Count + _scheduled.Count + _deferred.Count;

    // ---------------------------------------------------------------- send

    /// <summary>Enqueue a message. Returns its sequence number, or null when dropped as a duplicate.</summary>
    public long? Enqueue(OutgoingMessage outgoing) => EnqueueMessage(FromOutgoing(outgoing), _time.GetUtcNow());

    internal static BrokerMessage FromOutgoing(OutgoingMessage outgoing)
    {
        return new BrokerMessage
        {
            MessageId = string.IsNullOrEmpty(outgoing.MessageId) ? Guid.NewGuid().ToString("N") : outgoing.MessageId,
            CorrelationId = outgoing.CorrelationId,
            Subject = outgoing.Subject,
            ContentType = outgoing.ContentType,
            To = outgoing.To,
            ReplyTo = outgoing.ReplyTo,
            SessionId = outgoing.SessionId,
            ApplicationProperties = outgoing.ApplicationProperties is null ? new() : new(outgoing.ApplicationProperties),
            Body = outgoing.Body ?? [],
            ScheduledEnqueueTimeUtc = outgoing.ScheduledEnqueueTimeUtc,
            TimeToLive = outgoing.TimeToLive
        };
    }

    /// <summary>Enqueue an already-built message (used for fan-out copies and dead-lettering).</summary>
    internal long? EnqueueMessage(BrokerMessage m, DateTimeOffset now)
    {
        if (Options.RequiresSession && !IsDeadLetterQueue && string.IsNullOrEmpty(m.SessionId))
            throw new NexusException(NexusErrorCode.BadRequest, $"Entity '{Path}' requires sessions: set SessionId on every message.");

        lock (_gate)
        {
            if (Options.DuplicateDetectionWindow is { } window)
            {
                if (_seenMessageIds.TryGetValue(m.MessageId, out var seenAt) && now - seenAt < window)
                    return null;
                _seenMessageIds[m.MessageId] = now;
            }

            if (Options.MaxSizeInMessages > 0 && CountUnsafe() >= Options.MaxSizeInMessages)
                throw new NexusException(NexusErrorCode.QuotaExceeded, $"Entity '{Path}' is full ({Options.MaxSizeInMessages} messages).");

            m.SequenceNumber = _nextSequence++;
            m.EnqueuedTimeUtc = now;
            m.DeliveryCount = IsDeadLetterQueue ? m.DeliveryCount : 0;
            var ttl = m.TimeToLive ?? Options.DefaultTimeToLive;
            if (Options.DefaultTimeToLive is { } max && ttl > max) ttl = max;
            m.TimeToLive = ttl;
            m.ExpiresAtUtc = IsDeadLetterQueue || ttl is null ? null : (m.ScheduledEnqueueTimeUtc ?? now) + ttl.Value;

            if (m.ScheduledEnqueueTimeUtc is { } at && at > now)
            {
                m.State = MessageState.Scheduled;
                _scheduled[m.SequenceNumber] = m;
            }
            else
            {
                m.State = MessageState.Active;
                _active[m.SequenceNumber] = m;
            }

            _store.Put(Path, m);
            _totalEnqueued++;
            _metrics.Sent(Path);
            if (m.State == MessageState.Active) Signal();
            return m.SequenceNumber;
        }
    }

    public bool CancelScheduled(long sequenceNumber)
    {
        lock (_gate)
        {
            if (!_scheduled.Remove(sequenceNumber)) return false;
            _store.Remove(Path, sequenceNumber);
            return true;
        }
    }

    // ---------------------------------------------------------------- receive

    /// <summary>
    /// Take up to <paramref name="maxMessages"/> active messages. Also returns a task that completes when
    /// new messages may be available, so callers can long-poll without missing a wake-up.
    /// </summary>
    public (List<ReceivedMessage> Messages, Task WakeUp) TryReceive(int maxMessages, ReceiveMode mode)
    {
        if (Options.RequiresSession && !IsDeadLetterQueue)
            throw new NexusException(NexusErrorCode.BadRequest, $"Entity '{Path}' requires sessions: accept a session first.");
        return ReceiveCore(maxMessages, mode, sessionId: null);
    }

    private (List<ReceivedMessage> Messages, Task WakeUp) ReceiveCore(int maxMessages, ReceiveMode mode, string? sessionId)
    {
        var now = _time.GetUtcNow();
        var result = new List<ReceivedMessage>();
        var toDeadLetter = new List<BrokerMessage>();
        Task wake;

        lock (_gate)
        {
            // Committed-only delivery: stop at the first message whose write is not yet confirmed by the cluster.
            // Later messages were written later, so this keeps order and never delivers anything that could still be lost.
            var committed = _committedIndex?.Invoke() ?? long.MaxValue;
            if (sessionId is null)
            {
                while (result.Count < maxMessages && _active.Count > 0)
                {
                    var next = _active.First().Value;
                    if (next.RequiredCommitIndex > committed) break;
                    TakeForDelivery(next, mode, now, result, toDeadLetter);
                }
            }
            else
            {
                foreach (var m in _active.Values.Where(m => m.SessionId == sessionId).ToList())
                {
                    if (result.Count >= maxMessages || m.RequiredCommitIndex > committed) break;
                    TakeForDelivery(m, mode, now, result, toDeadLetter);
                }
            }
            wake = _signal.Task;
        }

        foreach (var m in toDeadLetter) MoveToDeadLetter(m, "TTLExpiredException", "The message expired and was dead lettered.", now);
        return (result, wake);
    }

    /// <summary>Remove <paramref name="m"/> from the active set and hand it out (caller holds the lock).</summary>
    private void TakeForDelivery(BrokerMessage m, ReceiveMode mode, DateTimeOffset now, List<ReceivedMessage> result, List<BrokerMessage> toDeadLetter)
    {
        _active.Remove(m.SequenceNumber);

        if (m.ExpiresAtUtc is { } exp && exp <= now)
        {
            _store.Remove(Path, m.SequenceNumber);
            _metrics.Expired(Path);
            if (Options.DeadLetterOnExpiration && DeadLetterQueue is not null) toDeadLetter.Add(m);
            return;
        }

        m.DeliveryCount++;
        _metrics.Delivered(Path);
        if (mode == ReceiveMode.ReceiveAndDelete)
        {
            _store.Remove(Path, m.SequenceNumber);
            _totalCompleted++;
            _metrics.Completed(Path);
            result.Add(new ReceivedMessage { Message = m.Clone() });
        }
        else
        {
            var token = Guid.NewGuid();
            var until = now + Options.LockDuration;
            m.State = MessageState.Locked;
            _locked[token] = new LockEntry(m, until);
            result.Add(new ReceivedMessage { Message = m.Clone(), LockToken = token, LockedUntilUtc = until });
        }
    }

    private async Task<T> LongPollAsync<T>(Func<(T Value, bool Done, Task WakeUp)> attempt, TimeSpan maxWait, CancellationToken ct)
    {
        var deadline = _time.GetUtcNow() + maxWait;
        while (true)
        {
            var (value, done, wake) = attempt();
            if (done) return value;
            var remaining = deadline - _time.GetUtcNow();
            if (remaining <= TimeSpan.Zero) return value;
            try
            {
                await wake.WaitAsync(remaining, _time, ct).ConfigureAwait(false);
            }
            catch (TimeoutException)
            {
                return value;
            }
        }
    }

    public Task<IReadOnlyList<ReceivedMessage>> ReceiveAsync(int maxMessages, TimeSpan maxWait, ReceiveMode mode, CancellationToken ct = default)
    {
        if (maxMessages < 1) throw new NexusException(NexusErrorCode.BadRequest, "maxMessages must be at least 1.");
        return LongPollAsync<IReadOnlyList<ReceivedMessage>>(() =>
        {
            var (messages, wake) = TryReceive(maxMessages, mode);
            return (messages, messages.Count > 0, wake);
        }, maxWait, ct);
    }

    // ---------------------------------------------------------------- sessions

    private void RequireSessions()
    {
        if (!Options.RequiresSession || IsDeadLetterQueue)
            throw new NexusException(NexusErrorCode.BadRequest, $"Entity '{Path}' is not session-enabled (set RequiresSession).");
    }

    private void ValidateSessionUnsafe(string sessionId, Guid token, DateTimeOffset now)
    {
        if (!_sessionLocks.TryGetValue(sessionId, out var l) || l.Token != token || l.Until < now)
            throw new NexusException(NexusErrorCode.SessionLockLost, $"The lock on session '{sessionId}' has expired or is invalid.");
    }

    /// <summary>
    /// Lock a session. With <paramref name="sessionId"/> null, picks the oldest session that has active messages
    /// and is not locked by someone else (Azure "AcceptNextSession").
    /// </summary>
    public (SessionHandle? Session, Task WakeUp) TryAcceptSession(string? sessionId)
    {
        RequireSessions();
        var now = _time.GetUtcNow();
        lock (_gate)
        {
            bool Free(string id) => !_sessionLocks.TryGetValue(id, out var l) || l.Until < now;

            if (sessionId is not null)
            {
                if (!Free(sessionId))
                    throw new NexusException(NexusErrorCode.SessionCannotBeLocked, $"Session '{sessionId}' is locked by another receiver.");
            }
            else
            {
                sessionId = _active.Values.Select(m => m.SessionId!).FirstOrDefault(id => id is not null && Free(id));
                if (sessionId is null) return (null, _signal.Task);
            }

            var handle = new SessionHandle { SessionId = sessionId, SessionLockToken = Guid.NewGuid(), LockedUntilUtc = now + Options.LockDuration };
            _sessionLocks[sessionId] = new SessionLock(handle.SessionLockToken, handle.LockedUntilUtc);
            return (handle, _signal.Task);
        }
    }

    public Task<SessionHandle?> AcceptSessionAsync(string? sessionId, TimeSpan maxWait, CancellationToken ct = default) =>
        LongPollAsync(() =>
        {
            var (session, wake) = TryAcceptSession(sessionId);
            return (session, session is not null, wake);
        }, maxWait, ct);

    /// <summary>Receive the next messages of a locked session, in order.</summary>
    public Task<IReadOnlyList<ReceivedMessage>> ReceiveFromSessionAsync(string sessionId, Guid sessionLockToken, int maxMessages,
        TimeSpan maxWait, ReceiveMode mode, CancellationToken ct = default)
    {
        RequireSessions();
        if (maxMessages < 1) throw new NexusException(NexusErrorCode.BadRequest, "maxMessages must be at least 1.");
        return LongPollAsync<IReadOnlyList<ReceivedMessage>>(() =>
        {
            lock (_gate) ValidateSessionUnsafe(sessionId, sessionLockToken, _time.GetUtcNow());
            var (messages, wake) = ReceiveCore(maxMessages, mode, sessionId);
            return (messages, messages.Count > 0, wake);
        }, maxWait, ct);
    }

    public DateTimeOffset RenewSessionLock(string sessionId, Guid sessionLockToken)
    {
        lock (_gate)
        {
            var now = _time.GetUtcNow();
            ValidateSessionUnsafe(sessionId, sessionLockToken, now);
            var l = _sessionLocks[sessionId];
            l.Until = now + Options.LockDuration;
            return l.Until;
        }
    }

    /// <summary>Give the session back so another receiver can accept it.</summary>
    public void ReleaseSession(string sessionId, Guid sessionLockToken)
    {
        lock (_gate)
        {
            if (_sessionLocks.TryGetValue(sessionId, out var l) && l.Token == sessionLockToken)
            {
                _sessionLocks.Remove(sessionId);
                Signal();
            }
        }
    }

    /// <summary>Per-session state (e.g. a workflow checkpoint), stored durably.</summary>
    public byte[]? GetSessionState(string sessionId, Guid sessionLockToken)
    {
        lock (_gate)
        {
            ValidateSessionUnsafe(sessionId, sessionLockToken, _time.GetUtcNow());
            return _sessionState.TryGetValue(sessionId, out var st) ? st.Body : null;
        }
    }

    public void SetSessionState(string sessionId, Guid sessionLockToken, byte[]? state)
    {
        lock (_gate)
        {
            ValidateSessionUnsafe(sessionId, sessionLockToken, _time.GetUtcNow());
            if (_sessionState.Remove(sessionId, out var old)) _store.Remove(StatePath, old.SequenceNumber);
            if (state is null) return;
            var record = new BrokerMessage { SequenceNumber = _nextStateSequence++, SessionId = sessionId, Body = state, MessageId = sessionId };
            _sessionState[sessionId] = record;
            _store.Put(StatePath, record);
        }
    }

    /// <summary>Receive deferred messages by sequence number (they become locked).</summary>
    public List<ReceivedMessage> ReceiveDeferred(IEnumerable<long> sequenceNumbers)
    {
        var now = _time.GetUtcNow();
        var result = new List<ReceivedMessage>();
        lock (_gate)
        {
            foreach (var seq in sequenceNumbers)
            {
                if (!_deferred.Remove(seq, out var m)) continue;
                m.DeliveryCount++;
                m.State = MessageState.Locked;
                var token = Guid.NewGuid();
                var until = now + Options.LockDuration;
                _locked[token] = new LockEntry(m, until);
                result.Add(new ReceivedMessage { Message = m.Clone(), LockToken = token, LockedUntilUtc = until });
            }
        }
        return result;
    }

    // ---------------------------------------------------------------- settle

    private LockEntry TakeLock(Guid token, DateTimeOffset now)
    {
        // An expired lock stays in the table so Sweep() returns the message to the queue.
        if (!_locked.TryGetValue(token, out var entry) || entry.LockedUntil < now)
            throw new NexusException(NexusErrorCode.MessageLockLost, $"The lock on the message in '{Path}' has expired or is invalid.");
        _locked.Remove(token);
        return entry;
    }

    /// <summary>Acknowledge: remove the message (RabbitMQ basic.ack).</summary>
    public void Complete(Guid lockToken)
    {
        lock (_gate)
        {
            var entry = TakeLock(lockToken, _time.GetUtcNow());
            _store.Remove(Path, entry.Message.SequenceNumber);
            _totalCompleted++;
            _metrics.Completed(Path);
        }
    }

    /// <summary>Release the lock so the message can be redelivered (RabbitMQ nack + requeue).</summary>
    public void Abandon(Guid lockToken, Dictionary<string, string>? propertiesToModify = null)
    {
        var now = _time.GetUtcNow();
        BrokerMessage m;
        lock (_gate)
        {
            m = TakeLock(lockToken, now).Message;
            if (propertiesToModify is not null)
                foreach (var (k, v) in propertiesToModify) m.ApplicationProperties[k] = v;
        }
        _metrics.Abandoned(Path);
        ReturnOrDeadLetter(m, now);
    }

    public void DeadLetter(Guid lockToken, string? reason, string? description, Dictionary<string, string>? propertiesToModify = null)
    {
        var now = _time.GetUtcNow();
        BrokerMessage m;
        lock (_gate)
        {
            m = TakeLock(lockToken, now).Message;
            if (propertiesToModify is not null)
                foreach (var (k, v) in propertiesToModify) m.ApplicationProperties[k] = v;
            _store.Remove(Path, m.SequenceNumber);
        }
        if (DeadLetterQueue is null)
            throw new NexusException(NexusErrorCode.BadRequest, "Messages in a dead-letter queue cannot be dead-lettered again.");
        MoveToDeadLetter(m, reason ?? "DeadLetteredByReceiver", description, now);
    }

    /// <summary>Set the message aside; it can only be received again by sequence number.</summary>
    public long Defer(Guid lockToken)
    {
        lock (_gate)
        {
            var m = TakeLock(lockToken, _time.GetUtcNow()).Message;
            m.State = MessageState.Deferred;
            _deferred[m.SequenceNumber] = m;
            _store.Put(Path, m);
            return m.SequenceNumber;
        }
    }

    public DateTimeOffset RenewLock(Guid lockToken)
    {
        lock (_gate)
        {
            var now = _time.GetUtcNow();
            if (!_locked.TryGetValue(lockToken, out var entry) || entry.LockedUntil < now)
                throw new NexusException(NexusErrorCode.MessageLockLost, $"The lock on the message in '{Path}' has expired or is invalid.");
            entry.LockedUntil = now + Options.LockDuration;
            return entry.LockedUntil;
        }
    }

    /// <summary>Put a message back to Active, or dead-letter it when it has exceeded MaxDeliveryCount.</summary>
    private void ReturnOrDeadLetter(BrokerMessage m, DateTimeOffset now)
    {
        if (DeadLetterQueue is not null && m.DeliveryCount >= Options.MaxDeliveryCount)
        {
            lock (_gate) _store.Remove(Path, m.SequenceNumber);
            MoveToDeadLetter(m, "MaxDeliveryCountExceeded",
                $"Message could not be consumed after {Options.MaxDeliveryCount} delivery attempts.", now);
            return;
        }

        lock (_gate)
        {
            m.State = MessageState.Active;
            _active[m.SequenceNumber] = m;
            _store.Put(Path, m);
            Signal();
        }
    }

    private void MoveToDeadLetter(BrokerMessage m, string reason, string? description, DateTimeOffset now)
    {
        if (DeadLetterQueue is null) return;
        var copy = m.Clone();
        copy.DeadLetterReason = reason;
        copy.DeadLetterErrorDescription = description;
        copy.DeadLetterSource = Path;
        copy.ScheduledEnqueueTimeUtc = null;
        _metrics.DeadLettered(Path);
        if (Options.ForwardDeadLetteredMessagesTo is { Length: > 0 } target && DeadLetterForwarder is not null)
        {
            copy.ExpiresAtUtc = null;
            copy.TimeToLive = null;
            try
            {
                if (DeadLetterForwarder(target, copy)) return;
            }
            catch (NexusException)
            {
                // Destination missing or full: fall back to our own dead-letter queue.
            }
        }
        DeadLetterQueue.EnqueueMessage(copy, now);
    }

    // ---------------------------------------------------------------- maintenance

    /// <summary>Expire locks, activate scheduled messages, expire TTL, prune duplicate-detection history, compact storage.</summary>
    public void Sweep()
    {
        var now = _time.GetUtcNow();
        var released = new List<BrokerMessage>();
        var expired = new List<BrokerMessage>();

        lock (_gate)
        {
            foreach (var (token, entry) in _locked.Where(kv => kv.Value.LockedUntil < now).ToList())
            {
                _locked.Remove(token);
                released.Add(entry.Message);
                _metrics.LockExpired(Path);
            }

            var due = _scheduled.Values.Where(m => m.ScheduledEnqueueTimeUtc <= now).ToList();
            foreach (var m in due)
            {
                _scheduled.Remove(m.SequenceNumber);
                m.State = MessageState.Active;
                _active[m.SequenceNumber] = m;
                _store.Put(Path, m);
            }

            foreach (var m in _active.Values.Where(m => m.ExpiresAtUtc is { } e && e <= now).ToList())
            {
                _active.Remove(m.SequenceNumber);
                _store.Remove(Path, m.SequenceNumber);
                expired.Add(m);
                _metrics.Expired(Path);
            }

            var expiredSessions = _sessionLocks.Where(kv => kv.Value.Until < now).Select(kv => kv.Key).ToList();
            foreach (var id in expiredSessions) _sessionLocks.Remove(id);

            if (Options.DuplicateDetectionWindow is { } window)
                foreach (var id in _seenMessageIds.Where(kv => now - kv.Value >= window).Select(kv => kv.Key).ToList())
                    _seenMessageIds.Remove(id);

            if (_store.NeedsCompaction(Path))
                _store.Compact(Path, SnapshotUnsafe());

            if (due.Count > 0 || expiredSessions.Count > 0) Signal();
        }

        foreach (var m in released) ReturnOrDeadLetter(m, now);
        if (Options.DeadLetterOnExpiration)
            foreach (var m in expired) MoveToDeadLetter(m, "TTLExpiredException", "The message expired and was dead lettered.", now);

        DeadLetterQueue?.Sweep();
    }

    /// <summary>The cluster committed more entries: wake receivers that may be waiting for them.</summary>
    internal void NotifyCommitted()
    {
        lock (_gate) Signal();
        DeadLetterQueue?.NotifyCommitted();
    }

    // ---------------------------------------------------------------- inspection

    private List<BrokerMessage> SnapshotUnsafe() =>
        _active.Values
            .Concat(_locked.Values.Select(l => l.Message))
            .Concat(_scheduled.Values)
            .Concat(_deferred.Values)
            .OrderBy(m => m.SequenceNumber)
            .ToList();

    /// <summary>Browse without locking (like the Azure "peek" or RabbitMQ management "get messages").</summary>
    public List<BrokerMessage> Peek(long fromSequenceNumber, int maxMessages)
    {
        lock (_gate)
        {
            return SnapshotUnsafe()
                .Where(m => m.SequenceNumber >= fromSequenceNumber)
                .Take(maxMessages)
                .Select(m => m.Clone())
                .ToList();
        }
    }

    /// <summary>A copy of one message by sequence number (any state), or null.</summary>
    public BrokerMessage? GetMessage(long sequenceNumber)
    {
        lock (_gate)
        {
            if (_active.TryGetValue(sequenceNumber, out var m) || _scheduled.TryGetValue(sequenceNumber, out m)
                || _deferred.TryGetValue(sequenceNumber, out m)) return m.Clone();
            return _locked.Values.FirstOrDefault(l => l.Message.SequenceNumber == sequenceNumber)?.Message.Clone();
        }
    }

    /// <summary>Remove one message (active, scheduled or deferred). Locked messages cannot be deleted until their lock ends.</summary>
    public bool DeleteMessage(long sequenceNumber)
    {
        lock (_gate)
        {
            if (_locked.Values.Any(l => l.Message.SequenceNumber == sequenceNumber))
                throw new NexusException(NexusErrorCode.BadRequest,
                    $"Message {sequenceNumber} in '{Path}' is locked by a receiver; wait for it to be settled or for the lock to expire.");
            if (!_active.Remove(sequenceNumber) && !_scheduled.Remove(sequenceNumber) && !_deferred.Remove(sequenceNumber)) return false;
            _store.Remove(Path, sequenceNumber);
            return true;
        }
    }

    /// <summary>Stored contents of this entity, its session state and its dead-letter queue, keyed by store path (for replication snapshots).</summary>
    internal IEnumerable<(string Path, List<BrokerMessage> Messages)> Export()
    {
        List<BrokerMessage> own, state;
        lock (_gate)
        {
            own = SnapshotUnsafe().Select(m =>
            {
                var c = m.Clone();
                if (c.State == MessageState.Locked) c.State = MessageState.Active;
                return c;
            }).ToList();
            state = _sessionState.Values.Select(m => m.Clone()).ToList();
        }
        yield return (Path, own);
        if (state.Count > 0) yield return (StatePath, state);
        if (DeadLetterQueue is not null)
            foreach (var x in DeadLetterQueue.Export()) yield return x;
    }

    public int Purge()
    {
        lock (_gate)
        {
            var count = CountUnsafe();
            foreach (var m in SnapshotUnsafe()) _store.Remove(Path, m.SequenceNumber);
            _active.Clear();
            _locked.Clear();
            _scheduled.Clear();
            _deferred.Clear();
            return count;
        }
    }

    public EntityStats GetStats()
    {
        lock (_gate)
        {
            return new EntityStats
            {
                Path = Path,
                Kind = Kind,
                Active = _active.Count,
                Locked = _locked.Count,
                Scheduled = _scheduled.Count,
                Deferred = _deferred.Count,
                DeadLetter = DeadLetterQueue?.GetStats().Active ?? 0,
                LockedSessions = _sessionLocks.Count,
                TotalEnqueued = _totalEnqueued,
                TotalCompleted = _totalCompleted
            };
        }
    }

    internal void DeleteStorage()
    {
        lock (_gate)
        {
            _store.DeleteEntity(Path);
            _store.DeleteEntity(StatePath);
            Signal();
        }
        DeadLetterQueue?.DeleteStorage();
    }
}
