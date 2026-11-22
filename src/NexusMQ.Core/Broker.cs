using System.Text.RegularExpressions;
using NexusMQ.Core.Persistence;

namespace NexusMQ.Core;

public sealed record SendResult(int Accepted, int Delivered, IReadOnlyList<long> SequenceNumbers);

/// <summary>
/// The broker: owns the topology (queues, topics/subscriptions, exchanges/bindings),
/// routes sends and resolves entity paths for receivers.
/// </summary>
public sealed partial class Broker : IDisposable
{
    public const string DeadLetterSuffix = "/$deadletter";
    public const string ExchangePrefix = "exchanges/";

    private sealed class TopicState(TopicDefinition definition)
    {
        public TopicDefinition Definition { get; } = definition;
        public Dictionary<string, (MessageEntity Entity, SubscriptionDefinition Definition)> Subscriptions { get; } = new(StringComparer.OrdinalIgnoreCase);
        public Dictionary<string, DateTimeOffset> SeenMessageIds { get; } = new(StringComparer.Ordinal);
    }

    private readonly Lock _topologyGate = new();
    private readonly IMessageStore _store;
    private readonly TimeProvider _time;
    private readonly ITimer? _sweepTimer;
    private const int MaxForwardHops = 8;

    /// <summary>Counters and gauges (meter "NexusMQ"), plus totals for the Prometheus endpoint.</summary>
    public BrokerMetrics Metrics { get; }
    private int _sweeping;

    private readonly Dictionary<string, MessageEntity> _queues = new(StringComparer.OrdinalIgnoreCase);
    private readonly Dictionary<string, TopicState> _topics = new(StringComparer.OrdinalIgnoreCase);
    private readonly Dictionary<string, ExchangeDefinition> _exchanges = new(StringComparer.OrdinalIgnoreCase);
    private readonly List<BindingDefinition> _bindings = [];

    private readonly Func<long>? _committedIndex;

    public Broker(IMessageStore? store = null, BrokerOptions? options = null, TimeProvider? time = null)
    {
        _store = store ?? new InMemoryMessageStore();
        _time = time ?? TimeProvider.System;
        options ??= new BrokerOptions();
        _committedIndex = options.CommittedIndex;
        Metrics = new BrokerMetrics(GetStats);

        if (_store.LoadTopology() is { } topology) ApplyTopology(topology);

        if (options.SweepInterval is { } interval)
            _sweepTimer = _time.CreateTimer(_ => Sweep(), null, interval, interval);
    }

    [GeneratedRegex(@"^[A-Za-z0-9][A-Za-z0-9._\-]{0,259}$")]
    private static partial Regex NameRegex();

    private static void ValidateName(string name, string what)
    {
        if (string.IsNullOrWhiteSpace(name) || !NameRegex().IsMatch(name))
            throw new NexusException(NexusErrorCode.BadRequest,
                $"Invalid {what} name '{name}'. Use letters, digits, '.', '-' or '_' (max 260 chars).");
    }

    private void ApplyTopology(TopologySnapshot t)
    {
        foreach (var q in t.Queues)
            _queues[q.Name] = NewEntity(q.Name, "queue", q.Options);
        foreach (var topic in t.Topics)
        {
            var state = new TopicState(topic);
            foreach (var s in topic.Subscriptions)
                state.Subscriptions[s.Name] = (NewEntity(SubscriptionPath(topic.Name, s.Name), "subscription", s.Options), s);
            _topics[topic.Name] = state;
        }
        foreach (var e in t.Exchanges) _exchanges[e.Name] = e;
        _bindings.AddRange(t.Bindings);
    }

    private MessageEntity NewEntity(string path, string kind, QueueOptions options)
    {
        var entity = new MessageEntity(path, kind, options, _store, _time, Metrics, committedIndex: _committedIndex);
        entity.DeadLetterForwarder = (target, message) =>
        {
            lock (_topologyGate)
                if (!DestinationExistsUnsafe(target)) return false;
            RouteTo(target, message, message.Subject, _time.GetUtcNow(), [], new HashSet<string>(StringComparer.OrdinalIgnoreCase), topLevel: false, hops: 1);
            return true;
        };
        return entity;
    }

    private bool DestinationExistsUnsafe(string path) =>
        path.StartsWith(ExchangePrefix, StringComparison.OrdinalIgnoreCase)
            ? _exchanges.ContainsKey(path[ExchangePrefix.Length..])
            : _queues.ContainsKey(path) || _topics.ContainsKey(path);

    private void ValidateForwardingUnsafe(string selfPath, QueueOptions? options)
    {
        if (options is null) return;
        foreach (var target in new[] { options.ForwardTo, options.ForwardDeadLetteredMessagesTo })
        {
            if (string.IsNullOrEmpty(target)) continue;
            if (target.Equals(selfPath, StringComparison.OrdinalIgnoreCase))
                throw new NexusException(NexusErrorCode.BadRequest, $"Entity '{selfPath}' cannot forward to itself.");
            if (!DestinationExistsUnsafe(target))
                throw new NexusException(NexusErrorCode.EntityNotFound, $"Forwarding destination '{target}' not found. Create it first.");
        }
    }

    public static string SubscriptionPath(string topic, string subscription) => $"{topic}/subscriptions/{subscription}";

    private void SaveTopologyUnsafe() => _store.SaveTopology(GetTopologyUnsafe());

    public TopologySnapshot GetTopology()
    {
        lock (_topologyGate) return GetTopologyUnsafe();
    }

    private TopologySnapshot GetTopologyUnsafe() => new()
    {
        Queues = _queues.Values.Select(q => new QueueDefinition { Name = q.Path, Options = q.Options }).OrderBy(q => q.Name).ToList(),
        Topics = _topics.Values.Select(t => new TopicDefinition
        {
            Name = t.Definition.Name,
            DuplicateDetectionWindow = t.Definition.DuplicateDetectionWindow,
            Subscriptions = t.Subscriptions.Values.Select(s => s.Definition).OrderBy(s => s.Name).ToList()
        }).OrderBy(t => t.Name).ToList(),
        Exchanges = _exchanges.Values.OrderBy(e => e.Name).ToList(),
        Bindings = _bindings.ToList()
    };

    // ================================================================ topology: queues

    public QueueDefinition CreateQueue(string name, QueueOptions? options = null)
    {
        ValidateName(name, "queue");
        lock (_topologyGate)
        {
            if (_topics.ContainsKey(name))
                throw new NexusException(NexusErrorCode.EntityAlreadyExists, $"A topic named '{name}' already exists.");
            ValidateForwardingUnsafe(name, options);
            if (_queues.TryGetValue(name, out var existing))
            {
                if (options is not null) existing.Options = options; // idempotent declare / update
            }
            else
            {
                _queues[name] = NewEntity(name, "queue", options ?? new QueueOptions());
            }
            SaveTopologyUnsafe();
            return new QueueDefinition { Name = name, Options = _queues[name].Options };
        }
    }

    public bool DeleteQueue(string name)
    {
        lock (_topologyGate)
        {
            if (!_queues.Remove(name, out var q)) return false;
            q.DeleteStorage();
            _bindings.RemoveAll(b => b.Destination.Equals(name, StringComparison.OrdinalIgnoreCase));
            SaveTopologyUnsafe();
            return true;
        }
    }

    // ================================================================ topology: topics

    public TopicDefinition CreateTopic(string name, TimeSpan? duplicateDetectionWindow = null)
    {
        ValidateName(name, "topic");
        lock (_topologyGate)
        {
            if (_queues.ContainsKey(name))
                throw new NexusException(NexusErrorCode.EntityAlreadyExists, $"A queue named '{name}' already exists.");
            if (!_topics.TryGetValue(name, out var state))
            {
                state = new TopicState(new TopicDefinition { Name = name });
                _topics[name] = state;
            }
            state.Definition.DuplicateDetectionWindow = duplicateDetectionWindow ?? state.Definition.DuplicateDetectionWindow;
            SaveTopologyUnsafe();
            return GetTopologyUnsafe().Topics.First(t => t.Name.Equals(name, StringComparison.OrdinalIgnoreCase));
        }
    }

    public bool DeleteTopic(string name)
    {
        lock (_topologyGate)
        {
            if (!_topics.Remove(name, out var t)) return false;
            foreach (var s in t.Subscriptions.Values) s.Entity.DeleteStorage();
            _bindings.RemoveAll(b => b.Destination.Equals(name, StringComparison.OrdinalIgnoreCase));
            SaveTopologyUnsafe();
            return true;
        }
    }

    public SubscriptionDefinition CreateSubscription(string topic, string name, QueueOptions? options = null, List<SubscriptionRule>? rules = null)
    {
        ValidateName(name, "subscription");
        foreach (var r in rules ?? [])
        {
            if (r.SqlFilter is not null) SqlFilter.Parse(r.SqlFilter); // fail fast on syntax errors
            if (r.SqlAction is not null) SqlRuleAction.Parse(r.SqlAction);
        }
        lock (_topologyGate)
        {
            if (!_topics.TryGetValue(topic, out var t))
                throw new NexusException(NexusErrorCode.EntityNotFound, $"Topic '{topic}' not found.");
            ValidateForwardingUnsafe(SubscriptionPath(topic, name), options);
            if (options?.ForwardTo is { } fwd && fwd.Equals(topic, StringComparison.OrdinalIgnoreCase))
                throw new NexusException(NexusErrorCode.BadRequest, "A subscription cannot forward to its own topic.");
            if (t.Subscriptions.TryGetValue(name, out var existing))
            {
                if (options is not null) existing.Entity.Options = existing.Definition.Options = options;
                if (rules is not null) existing.Definition.Rules = rules.Count == 0 ? [SubscriptionRule.MatchAll] : rules;
            }
            else
            {
                var def = new SubscriptionDefinition
                {
                    Name = name,
                    Options = options ?? new QueueOptions(),
                    Rules = rules is { Count: > 0 } ? rules : [SubscriptionRule.MatchAll]
                };
                t.Subscriptions[name] = (NewEntity(SubscriptionPath(t.Definition.Name, name), "subscription", def.Options), def);
            }
            SaveTopologyUnsafe();
            return t.Subscriptions[name].Definition;
        }
    }

    public bool DeleteSubscription(string topic, string name)
    {
        lock (_topologyGate)
        {
            if (!_topics.TryGetValue(topic, out var t) || !t.Subscriptions.Remove(name, out var s)) return false;
            s.Entity.DeleteStorage();
            SaveTopologyUnsafe();
            return true;
        }
    }

    // ================================================================ topology: exchanges

    public ExchangeDefinition CreateExchange(string name, ExchangeType type)
    {
        ValidateName(name, "exchange");
        lock (_topologyGate)
        {
            if (_exchanges.TryGetValue(name, out var existing) && existing.Type != type)
                throw new NexusException(NexusErrorCode.EntityAlreadyExists,
                    $"Exchange '{name}' already exists with type {existing.Type}.");
            var def = existing ?? new ExchangeDefinition { Name = name, Type = type };
            _exchanges[name] = def;
            SaveTopologyUnsafe();
            return def;
        }
    }

    public bool DeleteExchange(string name)
    {
        lock (_topologyGate)
        {
            if (!_exchanges.Remove(name)) return false;
            _bindings.RemoveAll(b => b.Exchange.Equals(name, StringComparison.OrdinalIgnoreCase)
                                     || b.Destination.Equals(ExchangePrefix + name, StringComparison.OrdinalIgnoreCase));
            SaveTopologyUnsafe();
            return true;
        }
    }

    public BindingDefinition Bind(BindingDefinition binding)
    {
        lock (_topologyGate)
        {
            if (!_exchanges.ContainsKey(binding.Exchange))
                throw new NexusException(NexusErrorCode.EntityNotFound, $"Exchange '{binding.Exchange}' not found.");
            var dest = binding.Destination;
            var destExists = dest.StartsWith(ExchangePrefix, StringComparison.OrdinalIgnoreCase)
                ? _exchanges.ContainsKey(dest[ExchangePrefix.Length..])
                : _queues.ContainsKey(dest) || _topics.ContainsKey(dest);
            if (!destExists)
                throw new NexusException(NexusErrorCode.EntityNotFound, $"Binding destination '{dest}' not found.");

            if (!_bindings.Any(b => SameBinding(b, binding))) _bindings.Add(binding);
            SaveTopologyUnsafe();
            return binding;
        }
    }

    public bool Unbind(BindingDefinition binding)
    {
        lock (_topologyGate)
        {
            var removed = _bindings.RemoveAll(b => SameBinding(b, binding)) > 0;
            if (removed) SaveTopologyUnsafe();
            return removed;
        }
    }

    private static bool SameBinding(BindingDefinition a, BindingDefinition b) =>
        a.Exchange.Equals(b.Exchange, StringComparison.OrdinalIgnoreCase)
        && a.Destination.Equals(b.Destination, StringComparison.OrdinalIgnoreCase)
        && a.RoutingKey == b.RoutingKey;

    // ================================================================ send / route

    /// <summary>
    /// Send messages to a queue name, a topic name, or "exchanges/{name}".
    /// For exchanges the routing key is <paramref name="routingKey"/> or each message's Subject.
    /// </summary>
    public SendResult Send(string entityPath, IReadOnlyList<OutgoingMessage> messages, string? routingKey = null)
    {
        if (messages.Count == 0) return new SendResult(0, 0, []);
        var now = _time.GetUtcNow();
        var seqs = new List<long>();
        var delivered = 0;

        foreach (var outgoing in messages)
        {
            var template = MessageEntity.FromOutgoing(outgoing);
            delivered += RouteTo(entityPath, template, routingKey ?? template.Subject, now, seqs, new HashSet<string>(StringComparer.OrdinalIgnoreCase), topLevel: true, hops: 0);
        }
        return new SendResult(messages.Count, delivered, seqs);
    }

    private int RouteTo(string destination, BrokerMessage template, string? routingKey, DateTimeOffset now,
        List<long> seqs, HashSet<string> visitedExchanges, bool topLevel, int hops)
    {
        MessageEntity? queue;
        TopicState? topic = null;
        ExchangeDefinition? exchange = null;
        List<BindingDefinition> bindings = [];
        List<(MessageEntity Entity, List<SubscriptionRule> Matched)> subscribers = [];

        lock (_topologyGate)
        {
            if (destination.StartsWith(ExchangePrefix, StringComparison.OrdinalIgnoreCase))
            {
                queue = null;
                var name = destination[ExchangePrefix.Length..];
                if (!_exchanges.TryGetValue(name, out exchange))
                {
                    if (topLevel) throw new NexusException(NexusErrorCode.EntityNotFound, $"Exchange '{name}' not found.");
                    return 0;
                }
                bindings = _bindings.Where(b => b.Exchange.Equals(name, StringComparison.OrdinalIgnoreCase)).ToList();
            }
            else if (_queues.TryGetValue(destination, out queue))
            {
            }
            else if (_topics.TryGetValue(destination, out topic))
            {
                if (topic.Definition.DuplicateDetectionWindow is { } window)
                {
                    if (topic.SeenMessageIds.TryGetValue(template.MessageId, out var seen) && now - seen < window) return 0;
                    topic.SeenMessageIds[template.MessageId] = now;
                }
                foreach (var s in topic.Subscriptions.Values)
                {
                    var matched = s.Definition.Rules.Where(r => Routing.RuleMatches(r, template)).ToList();
                    if (matched.Count > 0) subscribers.Add((s.Entity, matched));
                }
            }
            else
            {
                if (topLevel) throw new NexusException(NexusErrorCode.EntityNotFound, $"Entity '{destination}' not found.");
                return 0;
            }
        }

        if (queue is not null)
        {
            if (TryForward(queue, template, now, seqs, hops, out var forwarded)) return forwarded;
            var seq = queue.EnqueueMessage(template.Clone(), now);
            if (seq is null) return 0;
            if (topLevel) seqs.Add(seq.Value);
            return 1;
        }

        if (topic is not null)
        {
            var n = 0;
            foreach (var (sub, matched) in subscribers)
            {
                var copy = template.Clone();
                foreach (var rule in matched)
                    if (rule.SqlAction is not null) SqlRuleAction.Parse(rule.SqlAction).Apply(copy);
                if (TryForward(sub, copy, now, seqs, hops, out var forwarded)) { n += forwarded; continue; }
                if (sub.EnqueueMessage(copy, now) is not null) n++;
            }
            return n;
        }

        // Exchange routing (RabbitMQ semantics).
        if (!visitedExchanges.Add(exchange!.Name)) return 0; // break exchange-to-exchange cycles
        var delivered = 0;
        foreach (var b in bindings)
        {
            var match = exchange.Type switch
            {
                ExchangeType.Fanout => true,
                ExchangeType.Direct => string.Equals(b.RoutingKey, routingKey ?? "", StringComparison.Ordinal),
                ExchangeType.Topic => Routing.TopicMatches(b.RoutingKey, routingKey),
                ExchangeType.Headers => Routing.HeadersMatch(b.Arguments, template.ApplicationProperties),
                _ => false
            };
            if (match) delivered += RouteTo(b.Destination, template, routingKey, now, seqs, visitedExchanges, topLevel: false, hops);
        }
        return delivered;
    }

    /// <summary>Auto-forwarding: pass the message on instead of storing it. Falls back to storing when the target is gone or hops run out.</summary>
    private bool TryForward(MessageEntity source, BrokerMessage message, DateTimeOffset now, List<long> seqs, int hops, out int delivered)
    {
        delivered = 0;
        var target = source.Options.ForwardTo;
        if (string.IsNullOrEmpty(target) || hops >= MaxForwardHops) return false;
        lock (_topologyGate)
            if (!DestinationExistsUnsafe(target)) return false;
        delivered = RouteTo(target, message, message.Subject, now, seqs,
            new HashSet<string>(StringComparer.OrdinalIgnoreCase), topLevel: false, hops: hops + 1);
        return true;
    }

    // ================================================================ receive / settle

    /// <summary>Resolve a receivable path: "queue", "queue/$deadletter", "topic/subscriptions/sub", ".../$deadletter".</summary>
    public MessageEntity GetEntity(string path)
    {
        var trimmed = path.Trim('/');
        var deadLetter = trimmed.EndsWith(DeadLetterSuffix, StringComparison.OrdinalIgnoreCase);
        if (deadLetter) trimmed = trimmed[..^DeadLetterSuffix.Length];

        MessageEntity? entity = null;
        lock (_topologyGate)
        {
            var parts = trimmed.Split('/');
            if (parts.Length == 1)
                _queues.TryGetValue(parts[0], out entity);
            else if (parts.Length == 3 && parts[1].Equals("subscriptions", StringComparison.OrdinalIgnoreCase)
                     && _topics.TryGetValue(parts[0], out var t) && t.Subscriptions.TryGetValue(parts[2], out var s))
                entity = s.Entity;
        }

        if (entity is null) throw new NexusException(NexusErrorCode.EntityNotFound, $"Entity '{path}' not found.");
        return deadLetter ? entity.DeadLetterQueue! : entity;
    }

    // ---------------------------------------------------------------- sessions

    public Task<SessionHandle?> AcceptSessionAsync(string path, string? sessionId, TimeSpan maxWait, CancellationToken ct = default) =>
        GetEntity(path).AcceptSessionAsync(sessionId, maxWait, ct);

    public Task<IReadOnlyList<ReceivedMessage>> ReceiveFromSessionAsync(string path, string sessionId, Guid sessionLockToken,
        int maxMessages, TimeSpan maxWait, ReceiveMode mode = ReceiveMode.PeekLock, CancellationToken ct = default) =>
        GetEntity(path).ReceiveFromSessionAsync(sessionId, sessionLockToken, maxMessages, maxWait, mode, ct);

    public DateTimeOffset RenewSessionLock(string path, string sessionId, Guid token) => GetEntity(path).RenewSessionLock(sessionId, token);
    public void ReleaseSession(string path, string sessionId, Guid token) => GetEntity(path).ReleaseSession(sessionId, token);
    public byte[]? GetSessionState(string path, string sessionId, Guid token) => GetEntity(path).GetSessionState(sessionId, token);
    public void SetSessionState(string path, string sessionId, Guid token, byte[]? state) => GetEntity(path).SetSessionState(sessionId, token, state);

    public Task<IReadOnlyList<ReceivedMessage>> ReceiveAsync(string path, int maxMessages, TimeSpan maxWait,
        ReceiveMode mode = ReceiveMode.PeekLock, CancellationToken ct = default) =>
        GetEntity(path).ReceiveAsync(maxMessages, maxWait, mode, ct);

    public void Complete(string path, Guid lockToken) => GetEntity(path).Complete(lockToken);
    public void Abandon(string path, Guid lockToken, Dictionary<string, string>? props = null) => GetEntity(path).Abandon(lockToken, props);
    public void DeadLetter(string path, Guid lockToken, string? reason, string? description, Dictionary<string, string>? props = null) =>
        GetEntity(path).DeadLetter(lockToken, reason, description, props);
    public long Defer(string path, Guid lockToken) => GetEntity(path).Defer(lockToken);
    public DateTimeOffset RenewLock(string path, Guid lockToken) => GetEntity(path).RenewLock(lockToken);
    public List<ReceivedMessage> ReceiveDeferred(string path, IEnumerable<long> seqs) => GetEntity(path).ReceiveDeferred(seqs);
    public List<BrokerMessage> Peek(string path, long fromSequence, int max) => GetEntity(path).Peek(fromSequence, max);
    public int Purge(string path) => GetEntity(path).Purge();
    public bool CancelScheduled(string queue, long sequenceNumber) => GetEntity(queue).CancelScheduled(sequenceNumber);

    /// <summary>Delete a single message by sequence number. Throws MessageNotFound if it is not there.</summary>
    public void DeleteMessage(string path, long sequenceNumber)
    {
        if (!GetEntity(path).DeleteMessage(sequenceNumber))
            throw new NexusException(NexusErrorCode.MessageNotFound, $"Message {sequenceNumber} not found in '{path}'.");
    }

    /// <summary>
    /// Send a copy of one message again. <paramref name="destination"/> defaults to where it came from: for a dead-letter
    /// queue that is the entity that dead-lettered it, otherwise the same entity. The destination can be a queue, subscription,
    /// topic or "exchanges/name". <paramref name="removeOriginal"/> defaults to true for dead-letter queues (a "move") and
    /// false elsewhere (a "duplicate"). Returns the number of entities that received the copy.
    /// </summary>
    public int ResendMessage(string path, long sequenceNumber, string? destination = null, bool? removeOriginal = null)
    {
        var source = GetEntity(path);
        var original = source.GetMessage(sequenceNumber)
                       ?? throw new NexusException(NexusErrorCode.MessageNotFound, $"Message {sequenceNumber} not found in '{path}'.");
        destination ??= source.IsDeadLetterQueue ? original.DeadLetterSource ?? path[..^DeadLetterSuffix.Length] : path;
        var move = removeOriginal ?? source.IsDeadLetterQueue;
        if (move && original.State == MessageState.Locked)
            throw new NexusException(NexusErrorCode.BadRequest, $"Message {sequenceNumber} is locked by a receiver and cannot be moved now.");

        var copy = original.Clone();
        copy.DeadLetterReason = copy.DeadLetterErrorDescription = copy.DeadLetterSource = null;
        copy.ScheduledEnqueueTimeUtc = null;
        copy.ExpiresAtUtc = null;
        copy.DeliveryCount = 0;

        var now = _time.GetUtcNow();
        int delivered;
        MessageEntity? target = null;
        try { target = GetEntity(destination); } catch (NexusException) { /* not a receivable entity: route it */ }
        if (target is not null)
            delivered = target.EnqueueMessage(copy, now) is null ? 0 : 1;
        else
            delivered = RouteTo(destination, copy, copy.Subject, now, [], new HashSet<string>(StringComparer.OrdinalIgnoreCase), topLevel: true, hops: 0);

        if (move && delivered == 0)
            throw new NexusException(NexusErrorCode.BadRequest,
                $"The copy was not accepted by '{destination}' (duplicate detection or no matching route); the original was kept.");
        if (move) source.DeleteMessage(sequenceNumber);
        return delivered;
    }

    /// <summary>Move every message from a dead-letter queue back to its parent entity.</summary>
    public int ResubmitDeadLetters(string path)
    {
        var parentPath = path.EndsWith(DeadLetterSuffix, StringComparison.OrdinalIgnoreCase) ? path[..^DeadLetterSuffix.Length] : path;
        var parent = GetEntity(parentPath);
        var dlq = parent.DeadLetterQueue!;
        var now = _time.GetUtcNow();
        var moved = 0;
        while (true)
        {
            var (batch, _) = dlq.TryReceive(100, ReceiveMode.PeekLock);
            if (batch.Count == 0) return moved;
            foreach (var r in batch)
            {
                var m = r.Message.Clone();
                m.DeadLetterReason = m.DeadLetterErrorDescription = m.DeadLetterSource = null;
                m.ExpiresAtUtc = null;
                parent.EnqueueMessage(m, now);
                dlq.Complete(r.LockToken!.Value);
                moved++;
            }
        }
    }

    /// <summary>Cluster mode: the committed index moved; wake receivers waiting for newly confirmed messages.</summary>
    public void NotifyCommitted()
    {
        List<MessageEntity> entities;
        lock (_topologyGate)
            entities = _queues.Values.Concat(_topics.Values.SelectMany(t => t.Subscriptions.Values.Select(s => s.Entity))).ToList();
        foreach (var e in entities) e.NotifyCommitted();
    }

    // ================================================================ replication

    /// <summary>Full state for a standby. <paramref name="position"/> must be read from the replication log before calling.</summary>
    public Replication.ReplicationSnapshot ExportState(long position)
    {
        List<MessageEntity> entities;
        TopologySnapshot topology;
        lock (_topologyGate)
        {
            topology = GetTopologyUnsafe();
            entities = _queues.Values.Concat(_topics.Values.SelectMany(t => t.Subscriptions.Values.Select(s => s.Entity))).ToList();
        }
        var snapshot = new Replication.ReplicationSnapshot { Position = position, Topology = topology };
        foreach (var e in entities)
            foreach (var (path, messages) in e.Export())
                snapshot.Entities[path] = messages;
        return snapshot;
    }

    // ================================================================ maintenance & stats

    public List<EntityStats> GetStats()
    {
        List<MessageEntity> entities;
        lock (_topologyGate)
            entities = _queues.Values.Concat(_topics.Values.SelectMany(t => t.Subscriptions.Values.Select(s => s.Entity))).ToList();
        return entities.Select(e => e.GetStats()).OrderBy(s => s.Path).ToList();
    }

    /// <summary>Process lock expiry, scheduled messages, TTL and duplicate-detection windows on every entity.</summary>
    public void Sweep()
    {
        if (Interlocked.Exchange(ref _sweeping, 1) == 1) return;
        try
        {
            List<MessageEntity> entities;
            var now = _time.GetUtcNow();
            lock (_topologyGate)
            {
                entities = _queues.Values.Concat(_topics.Values.SelectMany(t => t.Subscriptions.Values.Select(s => s.Entity))).ToList();
                foreach (var t in _topics.Values)
                    if (t.Definition.DuplicateDetectionWindow is { } w)
                        foreach (var id in t.SeenMessageIds.Where(kv => now - kv.Value >= w).Select(kv => kv.Key).ToList())
                            t.SeenMessageIds.Remove(id);
            }
            foreach (var e in entities)
            {
                try { e.Sweep(); }
                catch { /* keep sweeping other entities; errors surface on the next client operation */ }
            }
        }
        finally
        {
            Volatile.Write(ref _sweeping, 0);
        }
    }

    public void Dispose()
    {
        _sweepTimer?.Dispose();
        Metrics.Dispose();
        _store.Dispose();
    }
}
