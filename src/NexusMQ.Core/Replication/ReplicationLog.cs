using NexusMQ.Core.Persistence;

namespace NexusMQ.Core.Replication;

/// <summary>Kinds of state change a standby replays.</summary>
public enum ReplicationOpKind
{
    /// <summary>Insert or replace one message (by sequence number) in an entity.</summary>
    Put,
    /// <summary>Remove one message.</summary>
    Remove,
    /// <summary>Delete an entity's storage.</summary>
    DeleteEntity,
    /// <summary>Replace an entity's whole contents (journal compaction).</summary>
    Replace,
    /// <summary>Replace the topology (queues, topics, exchanges, bindings).</summary>
    Topology,
    /// <summary>Replace the runtime access keys (hashes only).</summary>
    Keys
}

/// <summary>
/// One state change. Every op is idempotent, so a standby can replay ops it already has (after a snapshot) without harm.
/// </summary>
public sealed class ReplicationOp
{
    public long Position { get; set; }
    public ReplicationOpKind Kind { get; set; }
    public string? Entity { get; set; }
    public long Seq { get; set; }
    public BrokerMessage? Message { get; set; }
    public List<BrokerMessage>? Messages { get; set; }
    public TopologySnapshot? Topology { get; set; }
    /// <summary>Opaque JSON for <see cref="ReplicationOpKind.Keys"/> (owned by the hosting layer).</summary>
    public string? Payload { get; set; }
}

/// <summary>Full state at a log position: what a new or far-behind standby starts from.</summary>
public sealed class ReplicationSnapshot
{
    /// <summary>Ops after this position must be replayed on top of the snapshot.</summary>
    public long Position { get; set; }
    public TopologySnapshot Topology { get; set; } = new();
    /// <summary>Stored contents of every entity path (queues, subscriptions, dead-letter queues, session state).</summary>
    public Dictionary<string, List<BrokerMessage>> Entities { get; set; } = new();
    public string? KeysPayload { get; set; }
}

/// <summary>
/// In-memory ring of recent state changes on the primary, numbered by position. Standbys read from a position; if they
/// fall further behind than the ring holds, they take a fresh snapshot.
/// </summary>
public sealed class ReplicationLog
{
    private readonly ReplicationOp?[] _ring;
    private readonly Lock _gate = new();
    private long _position;
    private TaskCompletionSource _signal = new(TaskCreationOptions.RunContinuationsAsynchronously);

    public ReplicationLog(int capacity = 200_000) => _ring = new ReplicationOp?[Math.Max(16, capacity)];

    /// <summary>Position of the latest op (0 = nothing yet).</summary>
    public long Position { get { lock (_gate) return _position; } }

    /// <summary>The latest keys payload, so snapshots include the keys even if no key changed recently.</summary>
    public string? LatestKeysPayload { get; private set; }

    public void Append(ReplicationOp op)
    {
        TaskCompletionSource toSignal;
        lock (_gate)
        {
            op.Position = ++_position;
            _ring[op.Position % _ring.Length] = op;
            if (op.Kind == ReplicationOpKind.Keys) LatestKeysPayload = op.Payload;
            toSignal = _signal;
            _signal = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        }
        toSignal.TrySetResult();
    }

    /// <summary>
    /// Ops after <paramref name="after"/> (up to <paramref name="max"/>). Returns null when <paramref name="after"/> is
    /// older than the ring keeps: the caller must send a snapshot instead.
    /// </summary>
    public List<ReplicationOp>? ReadAfter(long after, int max, out Task wakeUp)
    {
        lock (_gate)
        {
            wakeUp = _signal.Task;
            if (after > _position) after = _position; // a standby ahead of us (we restarted): resend from our view
            var oldest = Math.Max(1, _position - _ring.Length + 1);
            if (after + 1 < oldest) return null;
            var list = new List<ReplicationOp>();
            for (var p = after + 1; p <= _position && list.Count < max; p++) list.Add(_ring[p % _ring.Length]!);
            return list;
        }
    }
}

/// <summary>Store decorator: writes go to the inner store and are recorded in the <see cref="ReplicationLog"/>.</summary>
public sealed class ReplicatingMessageStore(IMessageStore inner, ReplicationLog log) : IMessageStore
{
    public IMessageStore Inner => inner;
    public ReplicationLog Log => log;

    public void Put(string entityPath, BrokerMessage message)
    {
        inner.Put(entityPath, message);
        var copy = message.Clone();
        if (copy.State == MessageState.Locked) copy.State = MessageState.Active;
        log.Append(new ReplicationOp { Kind = ReplicationOpKind.Put, Entity = entityPath, Seq = message.SequenceNumber, Message = copy });
    }

    public void Remove(string entityPath, long sequenceNumber)
    {
        inner.Remove(entityPath, sequenceNumber);
        log.Append(new ReplicationOp { Kind = ReplicationOpKind.Remove, Entity = entityPath, Seq = sequenceNumber });
    }

    public void DeleteEntity(string entityPath)
    {
        inner.DeleteEntity(entityPath);
        log.Append(new ReplicationOp { Kind = ReplicationOpKind.DeleteEntity, Entity = entityPath });
    }

    public void Compact(string entityPath, IEnumerable<BrokerMessage> liveMessages)
    {
        var list = liveMessages.Select(m =>
        {
            var c = m.Clone();
            if (c.State == MessageState.Locked) c.State = MessageState.Active;
            return c;
        }).ToList();
        inner.Compact(entityPath, list);
        log.Append(new ReplicationOp { Kind = ReplicationOpKind.Replace, Entity = entityPath, Messages = list });
    }

    public void SaveTopology(TopologySnapshot topology)
    {
        inner.SaveTopology(topology);
        log.Append(new ReplicationOp { Kind = ReplicationOpKind.Topology, Topology = topology });
    }

    public IEnumerable<BrokerMessage> Load(string entityPath) => inner.Load(entityPath);
    public bool NeedsCompaction(string entityPath) => inner.NeedsCompaction(entityPath);
    public TopologySnapshot? LoadTopology() => inner.LoadTopology();
    public void Clear() => inner.Clear();
    public IEnumerable<string> ListEntities() => inner.ListEntities();
    public void Dispose() => inner.Dispose();
}

/// <summary>Applies snapshots and ops to a standby's local store.</summary>
public static class ReplicationApplier
{
    /// <summary>The committed contents of a store as a snapshot (the caller must keep the store from changing meanwhile).</summary>
    public static ReplicationSnapshot ExportStore(IMessageStore store, long position) => new()
    {
        Position = position,
        Topology = store.LoadTopology() ?? new TopologySnapshot(),
        Entities = store.ListEntities().ToDictionary(p => p, p => store.Load(p).ToList())
    };

    public static void ApplySnapshot(IMessageStore store, ReplicationSnapshot snapshot)
    {
        store.Clear();
        foreach (var (path, messages) in snapshot.Entities) store.Compact(path, messages);
        store.SaveTopology(snapshot.Topology);
    }

    /// <summary>Apply one op. Returns the keys payload for <see cref="ReplicationOpKind.Keys"/> ops (the host stores keys), else null.</summary>
    public static string? Apply(IMessageStore store, ReplicationOp op)
    {
        switch (op.Kind)
        {
            case ReplicationOpKind.Put: store.Put(op.Entity!, op.Message!); break;
            case ReplicationOpKind.Remove: store.Remove(op.Entity!, op.Seq); break;
            case ReplicationOpKind.DeleteEntity: store.DeleteEntity(op.Entity!); break;
            case ReplicationOpKind.Replace: store.Compact(op.Entity!, op.Messages ?? []); break;
            case ReplicationOpKind.Topology: store.SaveTopology(op.Topology!); break;
            case ReplicationOpKind.Keys: return op.Payload;
        }
        return null;
    }
}
