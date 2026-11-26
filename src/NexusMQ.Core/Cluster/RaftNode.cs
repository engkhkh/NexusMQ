using System.Diagnostics;
using NexusMQ.Core.Persistence;
using NexusMQ.Core.Replication;

namespace NexusMQ.Core.Cluster;

public enum RaftRole { Follower, Candidate, Leader }

public sealed class ClusterMember
{
    public string Id { get; set; } = "";
    public string Url { get; set; } = "";
}

/// <summary>
/// A vote request. <c>Transfer</c>: the candidate was asked to take over by the current leader (leadership transfer), so
/// voters skip the "I still hear from a leader" check for it, as etcd, HashiCorp Raft and Ratis do. Older servers ignore it.
/// </summary>
public sealed record VoteRequest(long Term, string CandidateId, long LastLogIndex, long LastLogTerm, bool Transfer = false);
/// <summary>Leader → chosen follower: "you are caught up, start an election now" (Raft thesis §3.10).</summary>
public sealed record TimeoutNowRequest(long Term, string LeaderId, long LastLogIndex);
public sealed record TimeoutNowResponse(long Term, bool Accepted, string? Reason = null);
public sealed record VoteResponse(long Term, bool Granted);
public sealed record AppendRequest(long Term, string LeaderId, long PrevLogIndex, long PrevLogTerm, List<RaftEntry> Entries, long LeaderCommit);
public sealed record AppendResponse(long Term, bool Success, long MatchIndex, long ConflictIndex);
public sealed record SnapshotRequest(long Term, string LeaderId, long LastIncludedIndex, long LastIncludedTerm, ReplicationSnapshot Snapshot,
    List<ClusterMember>? Config = null);
public sealed record SnapshotResponse(long Term);

/// <summary>How a Raft node talks to its peers. Returns null when the peer cannot be reached.</summary>
public interface IRaftTransport
{
    Task<VoteResponse?> RequestVoteAsync(ClusterMember peer, VoteRequest request, CancellationToken ct);
    /// <summary>
    /// PreVote: "would you vote for me at <c>request.Term</c>?", asked before an election without changing any term or vote.
    /// The default (a transport, or a peer, without PreVote) answers yes without asking, which is how elections worked
    /// before: the real vote that follows still decides.
    /// </summary>
    Task<VoteResponse?> RequestPreVoteAsync(ClusterMember peer, VoteRequest request, CancellationToken ct) =>
        Task.FromResult<VoteResponse?>(new VoteResponse(request.Term - 1, true));
    /// <summary>Leadership transfer. <c>null</c>: not reachable, or the peer does not support it (an older version).</summary>
    Task<TimeoutNowResponse?> TimeoutNowAsync(ClusterMember peer, TimeoutNowRequest request, CancellationToken ct) =>
        Task.FromResult<TimeoutNowResponse?>(null);
    Task<AppendResponse?> AppendEntriesAsync(ClusterMember peer, AppendRequest request, CancellationToken ct);
    Task<SnapshotResponse?> InstallSnapshotAsync(ClusterMember peer, SnapshotRequest request, CancellationToken ct);
}

public sealed class RaftOptions
{
    public TimeSpan ElectionTimeout { get; set; } = TimeSpan.FromMilliseconds(1500);
    public TimeSpan HeartbeatInterval { get; set; } = TimeSpan.FromMilliseconds(250);
    public int MaxEntriesPerAppend { get; set; } = 500;
    /// <summary>
    /// AppendEntries calls that may be in flight to one peer at once (pipelining). 1 (default) waits for each answer, which
    /// lets entries pile up into bigger batches; measured faster whenever the servers are CPU-bound. Raise it for links with
    /// a long round trip (other region) and spare CPU.
    /// </summary>
    public int MaxInflightAppends { get; set; } = 1;
    /// <summary>Compact the log once it holds more applied entries than this.</summary>
    public int MaxLogEntries { get; set; } = 50_000;
    /// <summary>
    /// Start as a new server that will be added to an existing cluster: no elections and no membership of its own until
    /// the leader replicates the cluster's configuration to it.
    /// </summary>
    public bool JoinExisting { get; set; }
    /// <summary>How long a new server may take to catch up (as a non-voting learner) before it is added.</summary>
    public TimeSpan CatchUpTimeout { get; set; } = TimeSpan.FromSeconds(60);
    /// <summary>
    /// Ask "would you vote for me?" before each election (default on; etcd, HashiCorp Raft, Ratis and Kafka KRaft do the same).
    /// Off: elections raise the term straight away, as before 1.18.
    /// </summary>
    public bool PreVote { get; set; } = true;
}

public sealed class RaftStatus
{
    public string NodeId { get; init; } = "";
    public RaftRole Role { get; init; }
    public long Term { get; init; }
    public string? LeaderId { get; init; }
    public bool LeaderReady { get; init; }
    public bool IsVoter { get; init; }
    public long LastLogIndex { get; init; }
    public long CommitIndex { get; init; }
    public long LastApplied { get; init; }
    public long SnapshotIndex { get; init; }
    public List<ClusterMember> Members { get; init; } = [];
    public List<ClusterMember> Learners { get; init; } = [];
    public Dictionary<string, long> PeerMatchIndex { get; init; } = new();
    /// <summary>Leader only: milliseconds since each peer last answered (null = never since this node became leader).</summary>
    public Dictionary<string, long?> PeerLastContactMs { get; init; } = new();
    /// <summary>AppendEntries calls this node has sent as leader (all terms, including heartbeats).</summary>
    public long AppendCallsSent { get; init; }
    /// <summary>Log entries carried by those calls; entries per call shows how well replication batches.</summary>
    public long AppendEntriesSent { get; init; }
    /// <summary>PreVote rounds this node ran (its election timer expired).</summary>
    public long PreVoteRounds { get; init; }
    /// <summary>Real elections this node started (a PreVote majority said yes): each one raises the term.</summary>
    public long ElectionsStarted { get; init; }
    /// <summary>Leadership transfers this node completed as leader (it handed over) and as target (it took over).</summary>
    public long TransfersOut { get; init; }
    public long TransfersIn { get; init; }
    /// <summary>Set while this leader is handing over: the server it is handing over to.</summary>
    public string? TransferringTo { get; init; }
}

/// <summary>
/// A Raft consensus node. The leader turns store writes into log entries (<see cref="Propose"/>); an entry is committed
/// once a majority of voters has it, and then every node (leader included) applies it to its local message store. The store
/// therefore always holds exactly the committed state, which is also what snapshots are made of.
/// Membership changes one server at a time (<see cref="AddServerAsync"/>, <see cref="RemoveServerAsync"/>); a configuration
/// takes effect as soon as it is in a node's log.
/// </summary>
public sealed class RaftNode : IDisposable
{
    private readonly string _id;
    private readonly RaftLog _log;
    private readonly IMessageStore _store;
    private readonly IRaftTransport _transport;
    private readonly RaftOptions _options;
    private readonly Lock _gate = new();
    private readonly CancellationTokenSource _stop = new();
    private readonly Random _random = new();
    private readonly Dictionary<string, long> _nextIndex = new();
    private readonly Dictionary<string, long> _matchIndex = new();
    private readonly Dictionary<string, DateTime> _lastAck = new();
    private readonly Dictionary<string, ClusterMember> _learners = new();
    private readonly HashSet<string> _replicating = new();

    private List<ClusterMember> _members;   // current voting configuration (latest in log, or the static one)
    private long _configIndex;              // log index of that configuration (0 = static / not in the log)
    private RaftRole _role = RaftRole.Follower;
    private string? _leaderId;
    private long _commitIndex;
    private long _noopIndex = long.MaxValue;
    private bool _leaderReady;
    private DateTime _leaderSince;
    private DateTime _lastHeard = DateTime.UtcNow;
    private TimeSpan _electionDeadline;
    private long _appendCalls, _appendEntriesSent;
    private long _preVotes, _elections, _transfersOut, _transfersIn;
    /// <summary>Leader: handing over to this server; new writes are refused meanwhile (they would delay the hand-over).</summary>
    private string? _transferTarget;
    /// <summary>Follower: the leader asked us to take over; the election loop starts an election at once, skipping PreVote.</summary>
    private bool _campaignNow;
    private TaskCompletionSource _commitSignal = NewSignal();
    private TaskCompletionSource _appendSignal = NewSignal();
    private readonly SemaphoreSlim _applyWake = new(0);
    /// <summary>Wakes the leader's group-commit flusher when entries are appended.</summary>
    private readonly SemaphoreSlim _flushWake = new(0);
    private Task? _flushLoop;
    /// <summary>Test hook: awaited before each group-commit flush (lets tests hold the leader's disk back).</summary>
    internal Func<Task>? BeforeFlushForTests { get; set; }
    private Task? _electionLoop, _applyLoop;

    /// <summary>Serializes store changes (applying entries, exporting and installing snapshots).</summary>
    public SemaphoreSlim ApplyLock { get; } = new(1, 1);

    /// <summary>Raised (outside locks) with true when this node becomes a leader that has applied everything committed, false when it stops leading.</summary>
    public event Action<bool>? LeadershipChanged;
    /// <summary>Raised when a replicated keys payload is applied (runtime access keys).</summary>
    public Action<string?>? KeysApplied { get; set; }
    /// <summary>Supplies the keys payload for snapshots.</summary>
    public Func<string?>? ExportKeys { get; set; }

    /// <summary>Create a node. <paramref name="members"/> is the initial (static) membership, used until the log holds one; ignored with <see cref="RaftOptions.JoinExisting"/>.</summary>
    public RaftNode(string id, IEnumerable<ClusterMember> members, RaftLog log, IMessageStore store, IRaftTransport transport, RaftOptions? options = null)
    {
        _id = id;
        _log = log;
        _store = store;
        _transport = transport;
        _options = options ?? new RaftOptions();
        List<ClusterMember> initial = _options.JoinExisting ? [] : members.ToList();
        var (config, index) = _log.LatestConfig();
        if (!_options.JoinExisting && config is null && initial.All(m => m.Id != id))
            throw new ArgumentException($"Node id '{id}' is not in the member list (use JoinExisting for a server being added).");
        (_members, _configIndex) = config is not null ? (config, index) : (initial, 0);
        _commitIndex = _log.LastApplied; // applied entries were committed
        ResetElectionTimer();
    }

    private static TaskCompletionSource NewSignal() => new(TaskCreationOptions.RunContinuationsAsynchronously);
    private int Majority => _members.Count / 2 + 1;
    private bool IsVoterUnsafe => _members.Any(m => m.Id == _id);
    private IEnumerable<ClusterMember> PeersUnsafe => _members.Where(m => m.Id != _id);

    public string Id => _id;
    public IReadOnlyList<ClusterMember> Members { get { lock (_gate) return [.. _members]; } }
    public RaftRole Role { get { lock (_gate) return _role; } }
    public string? LeaderId { get { lock (_gate) return _leaderId; } }
    public bool IsReadyLeader { get { lock (_gate) return _role == RaftRole.Leader && _leaderReady; } }
    public long CurrentTerm { get { lock (_gate) return _log.CurrentTerm; } }
    public long CommitIndex { get { lock (_gate) return _commitIndex; } }

    /// <summary>Completes the next time the commit index moves (or leadership changes).</summary>
    public Task CommitChanged() { lock (_gate) return _commitSignal.Task; }

    public void Start()
    {
        _electionLoop = Task.Run(ElectionLoopAsync);
        _applyLoop = Task.Run(ApplyLoopAsync);
        _flushLoop = Task.Run(FlushLoopAsync);
        _applyWake.Release(); // apply anything committed but not yet applied (restart)
    }

    private void ResetElectionTimer()
    {
        _lastHeard = DateTime.UtcNow;
        var ms = _options.ElectionTimeout.TotalMilliseconds;
        _electionDeadline = TimeSpan.FromMilliseconds(ms + _random.NextDouble() * ms);
    }

    /// <summary>Re-read the configuration from the log (after appends, truncation or a snapshot). Caller holds the lock.</summary>
    private void RefreshConfigUnsafe()
    {
        var (config, index) = _log.LatestConfig();
        if (config is null) return; // keep the static one
        (_members, _configIndex) = (config, index);
        if (_role != RaftRole.Leader) return;
        foreach (var m in _members.Where(m => m.Id != _id))
        {
            _learners.Remove(m.Id);
            if (!_nextIndex.ContainsKey(m.Id))
            {
                _nextIndex[m.Id] = _log.LastIndex + 1;
                _matchIndex[m.Id] = 0;
                _lastAck[m.Id] = DateTime.UtcNow;
            }
            StartReplicationUnsafe(m, _log.CurrentTerm);
        }
        foreach (var gone in _matchIndex.Keys.Where(k => _members.All(m => m.Id != k) && !_learners.ContainsKey(k)).ToList())
        {
            _matchIndex.Remove(gone);
            _nextIndex.Remove(gone);
            _lastAck.Remove(gone);
        }
    }

    private void StartReplicationUnsafe(ClusterMember peer, long term)
    {
        if (!_replicating.Add(peer.Id)) return;
        _ = Task.Run(async () =>
        {
            try { await ReplicateLoopAsync(peer, term); }
            finally { lock (_gate) _replicating.Remove(peer.Id); }
        });
    }

    // ================================================================== elections

    private async Task ElectionLoopAsync()
    {
        while (!_stop.IsCancellationRequested)
        {
            try { await Task.Delay(50, _stop.Token); } catch (OperationCanceledException) { return; }
            bool start, transfer;
            lock (_gate)
            {
                transfer = _campaignNow && _role != RaftRole.Leader && IsVoterUnsafe;
                _campaignNow = false;
                // Only voters run elections: a joining server waits to be added, a removed server stays quiet.
                start = transfer || (_role != RaftRole.Leader && IsVoterUnsafe && DateTime.UtcNow - _lastHeard > _electionDeadline);
                // Check quorum: a leader that cannot reach a majority steps down, so a cut-off leader stops serving clients.
                if (_role == RaftRole.Leader && _members.Count > 1 && DateTime.UtcNow - _leaderSince > _options.ElectionTimeout)
                {
                    var cutoff = DateTime.UtcNow - _options.ElectionTimeout;
                    var reachable = (IsVoterUnsafe ? 1 : 0) + PeersUnsafe.Count(p => _lastAck.TryGetValue(p.Id, out var t) && t > cutoff);
                    if (reachable < Majority) StepDownUnsafe(_log.CurrentTerm);
                }
            }
            if (start) await RunElectionAsync(transfer);
        }
    }

    /// <summary>
    /// PreVote round (Raft thesis §9.6): before raising its term, a server asks whether a majority would vote for it. A
    /// server cut off from the others gets no majority, so its term stays where it was and, when the network comes back,
    /// it simply follows the leader again instead of forcing an election with its higher term.
    /// </summary>
    private async Task<bool> PreVoteAsync()
    {
        VoteRequest request;
        long term;
        List<ClusterMember> peers;
        int majority;
        lock (_gate)
        {
            term = _log.CurrentTerm;
            _leaderId = null; // we lost the leader; don't send clients to it
            ResetElectionTimer();
            request = new VoteRequest(term + 1, _id, _log.LastIndex, _log.LastTerm);
            peers = [.. PeersUnsafe];
            majority = Majority;
        }
        Interlocked.Increment(ref _preVotes);

        var votes = 1;
        if (votes < majority)
        {
            using var timeout = CancellationTokenSource.CreateLinkedTokenSource(_stop.Token);
            timeout.CancelAfter(_options.ElectionTimeout);
            var pending = peers.Select(p => _transport.RequestPreVoteAsync(p, request, timeout.Token)).ToList();
            while (pending.Count > 0 && votes < majority)
            {
                var done = await Task.WhenAny(pending);
                pending.Remove(done);
                VoteResponse? r;
                try { r = await done; } catch { r = null; }
                if (r is null) continue;
                if (r.Granted) { votes++; continue; }
                lock (_gate)
                    if (r.Term > _log.CurrentTerm) { StepDownUnsafe(r.Term); return false; } // we were behind: catch up
            }
            if (votes < majority) return false;
        }
        lock (_gate)
            // Still worth an election? Not if a leader reached us or our term moved while we were asking.
            // (A candidate whose election was split asks again: it is no leader and has none.)
            return _role != RaftRole.Leader && _leaderId is null && _log.CurrentTerm == term && IsVoterUnsafe;
    }

    // ================================================================== leadership transfer (Raft thesis §3.10)

    /// <summary>
    /// Hand the leader role to another voter, e.g. before restarting this server: new writes are refused (clients retry),
    /// the target is brought up to date, then told to start an election at once. It wins without waiting for a timeout, so
    /// clients see a pause of one round trip instead of an election timeout. If it has not taken over within an election
    /// timeout, the transfer is abandoned and this server keeps leading (etcd, HashiCorp Raft and Ratis work the same way).
    /// </summary>
    /// <param name="targetId">The server to hand over to; <c>null</c> picks the most up-to-date voter.</param>
    /// <param name="ct">Stops waiting (the transfer is then abandoned).</param>
    /// <returns>The id of the new leader.</returns>
    public async Task<string> TransferLeadershipAsync(string? targetId, CancellationToken ct = default)
    {
        ClusterMember target;
        long term;
        lock (_gate)
        {
            if (_role != RaftRole.Leader || !_leaderReady)
                throw new NexusException(NexusErrorCode.Standby, "Leadership can only be handed over by the cluster leader.");
            if (_transferTarget is not null)
                throw new NexusException(NexusErrorCode.BadRequest, $"A hand-over to {_transferTarget} is already in progress.");
            var peers = PeersUnsafe.ToList();
            if (targetId is null)
                target = peers.OrderByDescending(p => _matchIndex.GetValueOrDefault(p.Id))
                              .ThenByDescending(p => _lastAck.GetValueOrDefault(p.Id)).FirstOrDefault()
                         ?? throw new NexusException(NexusErrorCode.BadRequest, "There is no other voting server to hand over to.");
            else if (targetId == _id)
                throw new NexusException(NexusErrorCode.BadRequest, $"{_id} is already the leader.");
            else
                target = peers.FirstOrDefault(p => p.Id == targetId)
                         ?? throw new NexusException(NexusErrorCode.BadRequest, $"'{targetId}' is not a voting member of the cluster.");
            _transferTarget = target.Id;
            term = _log.CurrentTerm;
            SignalAppendUnsafe();
        }

        var deadline = DateTime.UtcNow + _options.ElectionTimeout + _options.ElectionTimeout; // catch up, then one election
        var sent = false;
        string? failure = null;
        try
        {
            while (DateTime.UtcNow < deadline && !ct.IsCancellationRequested)
            {
                TimeoutNowRequest? request = null;
                lock (_gate)
                {
                    if (_role != RaftRole.Leader || _log.CurrentTerm != term) break; // someone took over
                    if (!sent && _log.DurableIndex >= _log.LastIndex && _matchIndex.GetValueOrDefault(target.Id) >= _log.LastIndex)
                        request = new TimeoutNowRequest(term, _id, _log.LastIndex);
                }
                if (request is not null)
                {
                    sent = true;
                    var answer = await SendTimeoutNowAsync(target, request);
                    if (answer is not { Accepted: true })
                    {
                        failure = answer is null
                            ? $"{target.Id} did not answer or runs a version without leadership transfer."
                            : $"{target.Id} declined: {answer.Reason}";
                        break;
                    }
                }
                await Task.Delay(10, ct).ConfigureAwait(false);
            }
        }
        catch (OperationCanceledException) { }
        finally
        {
            lock (_gate) if (_role == RaftRole.Leader && _transferTarget == target.Id) _transferTarget = null;
        }

        lock (_gate)
        {
            if (_role == RaftRole.Leader && _log.CurrentTerm == term)
                throw new NexusException(NexusErrorCode.NotCommitted,
                    failure ?? $"{target.Id} did not take over in time; {_id} stays the leader.");
            Interlocked.Increment(ref _transfersOut);
        }
        // Report who leads now: normally the target, which tells us within a heartbeat or two. (A server that is shutting down
        // may no longer hear from it, so don't wait long.)
        var until = DateTime.UtcNow + TimeSpan.FromTicks(Math.Min(_options.ElectionTimeout.Ticks, _options.HeartbeatInterval.Ticks * 3));
        while (DateTime.UtcNow < until)
        {
            lock (_gate) if (_leaderId is not null) return _leaderId;
            await Task.Delay(10, CancellationToken.None);
        }
        return target.Id;
    }

    private async Task<TimeoutNowResponse?> SendTimeoutNowAsync(ClusterMember target, TimeoutNowRequest request)
    {
        try
        {
            using var timeout = CancellationTokenSource.CreateLinkedTokenSource(_stop.Token);
            timeout.CancelAfter(_options.ElectionTimeout);
            return await _transport.TimeoutNowAsync(target, request, timeout.Token);
        }
        catch { return null; }
    }

    /// <summary>
    /// The leader asks us to take over. Only accepted from our current leader, in its term, once our log has everything the
    /// leader had when it asked: a late or stray request (raft-rs issue 234) cannot start an election out of turn.
    /// </summary>
    public TimeoutNowResponse HandleTimeoutNow(TimeoutNowRequest req)
    {
        lock (_gate)
        {
            string? reason = null;
            if (_stop.IsCancellationRequested) reason = "this server is shutting down";
            else if (req.Term != _log.CurrentTerm) reason = $"term {req.Term} is not the current term {_log.CurrentTerm}";
            else if (_role == RaftRole.Leader) reason = "this server is already the leader";
            else if (!IsVoterUnsafe) reason = "this server is not a voting member";
            else if (_log.LastIndex < req.LastLogIndex) reason = $"this server's log is behind ({_log.LastIndex} < {req.LastLogIndex})";
            else if (_leaderId is not null && _leaderId != req.LeaderId) reason = $"this server follows {_leaderId}, not {req.LeaderId}";
            if (reason is not null) return new TimeoutNowResponse(_log.CurrentTerm, false, reason);
            _campaignNow = true;
            return new TimeoutNowResponse(_log.CurrentTerm, true);
        }
    }

    /// <summary>Answer a PreVote: the same checks as a real vote, but nothing is changed or recorded.</summary>
    public VoteResponse HandlePreVote(VoteRequest req)
    {
        lock (_gate)
        {
            var inTouchWithLeader = _role == RaftRole.Leader
                                    || (_leaderId is not null && DateTime.UtcNow - _lastHeard < _options.ElectionTimeout);
            var upToDate = req.LastLogTerm > _log.LastTerm || (req.LastLogTerm == _log.LastTerm && req.LastLogIndex >= _log.LastIndex);
            return new VoteResponse(_log.CurrentTerm,
                !_stop.IsCancellationRequested && !inTouchWithLeader && req.Term > _log.CurrentTerm && upToDate);
        }
    }

    /// <param name="transfer">The leader handed over to us: no PreVote, and voters skip their leader check.</param>
    private async Task RunElectionAsync(bool transfer = false)
    {
        if (!transfer && _options.PreVote && !await PreVoteAsync()) return;
        Interlocked.Increment(ref _elections);
        VoteRequest request;
        long term;
        List<ClusterMember> peers;
        int majority;
        lock (_gate)
        {
            term = _log.CurrentTerm + 1;
            _log.SetTerm(term, _id);
            _role = RaftRole.Candidate;
            _leaderId = null;
            ResetElectionTimer();
            request = new VoteRequest(term, _id, _log.LastIndex, _log.LastTerm, transfer);
            peers = [.. PeersUnsafe];
            majority = Majority;
        }

        var votes = 1;
        if (votes >= majority) { BecomeLeader(term); return; }
        using var timeout = CancellationTokenSource.CreateLinkedTokenSource(_stop.Token);
        timeout.CancelAfter(_options.ElectionTimeout);
        var pending = peers.Select(p => _transport.RequestVoteAsync(p, request, timeout.Token)).ToList();
        while (pending.Count > 0)
        {
            var done = await Task.WhenAny(pending);
            pending.Remove(done);
            VoteResponse? r;
            try { r = await done; } catch { r = null; }
            if (r is null) continue;
            lock (_gate)
            {
                if (r.Term > _log.CurrentTerm) { StepDownUnsafe(r.Term); return; }
                if (_role != RaftRole.Candidate || _log.CurrentTerm != term) return;
            }
            if (r.Granted && ++votes >= majority)
            {
                BecomeLeader(term);
                if (transfer) Interlocked.Increment(ref _transfersIn);
                return;
            }
        }
    }

    public VoteResponse HandleVote(VoteRequest req)
    {
        lock (_gate)
        {
            // Leader stickiness: while we are in touch with a live leader, ignore candidates (e.g. a server that was
            // removed from the cluster and keeps timing out) instead of letting them bump the term and disrupt everyone.
            var inTouchWithLeader = _role == RaftRole.Leader
                                    || (_leaderId is not null && DateTime.UtcNow - _lastHeard < _options.ElectionTimeout);
            // A transfer candidate was sent by the leader itself, so the leader check does not apply to it. The leader
            // only lets through the server it is handing over to.
            var handOver = req.Transfer && (_role != RaftRole.Leader || _transferTarget == req.CandidateId);
            // A server that has stopped (shutting down) takes no part, as if it were already gone.
            if (_stop.IsCancellationRequested) return new VoteResponse(_log.CurrentTerm, false);
            if (req.Term > _log.CurrentTerm && inTouchWithLeader && !handOver) return new VoteResponse(_log.CurrentTerm, false);

            if (req.Term < _log.CurrentTerm) return new VoteResponse(_log.CurrentTerm, false);
            if (req.Term > _log.CurrentTerm) StepDownUnsafe(req.Term);
            var upToDate = req.LastLogTerm > _log.LastTerm || (req.LastLogTerm == _log.LastTerm && req.LastLogIndex >= _log.LastIndex);
            if ((_log.VotedFor is null || _log.VotedFor == req.CandidateId) && upToDate)
            {
                _log.SetTerm(_log.CurrentTerm, req.CandidateId);
                ResetElectionTimer();
                return new VoteResponse(_log.CurrentTerm, true);
            }
            return new VoteResponse(_log.CurrentTerm, false);
        }
    }

    private void BecomeLeader(long term)
    {
        lock (_gate)
        {
            if (_log.CurrentTerm != term || _role == RaftRole.Leader) return;
            _role = RaftRole.Leader;
            _leaderId = _id;
            _leaderReady = false;
            _leaderSince = DateTime.UtcNow;
            _learners.Clear();
            foreach (var p in PeersUnsafe)
            {
                _nextIndex[p.Id] = _log.LastIndex + 1;
                _matchIndex[p.Id] = 0;
                _lastAck[p.Id] = DateTime.MinValue;
            }
            // A no-op in our term: once it commits, everything before it is committed too. The very first leader also
            // records the static membership in the log, so later membership changes have a base to work from.
            var noop = new RaftEntry
            {
                Index = _log.LastIndex + 1,
                Term = term,
                Config = _log.LatestConfig().Config is null ? [.. _members] : null
            };
            _log.Append(noop);
            _noopIndex = noop.Index;
            RefreshConfigUnsafe();
            foreach (var p in PeersUnsafe) StartReplicationUnsafe(p, term);
            SignalAppendUnsafe();
            AdvanceCommitUnsafe();
        }
    }

    /// <summary>Become a follower (caller holds the lock). Raises LeadershipChanged(false) if we were leading.</summary>
    private void StepDownUnsafe(long newTerm)
    {
        var wasReadyLeader = _role == RaftRole.Leader && _leaderReady;
        if (newTerm > _log.CurrentTerm) _log.SetTerm(newTerm, null);
        if (_role == RaftRole.Leader) _leaderId = null;
        _role = RaftRole.Follower;
        _leaderReady = false;
        _transferTarget = null;
        _noopIndex = long.MaxValue;
        _learners.Clear();
        ResetElectionTimer();
        SignalCommitUnsafe(); // wake commit waiters so they notice
        if (wasReadyLeader) _ = Task.Run(() => LeadershipChanged?.Invoke(false));
    }

    // ================================================================== replication (leader)

    /// <summary>Leader: append a store change to the log. Returns (index, term) for <see cref="WaitCommittedAsync"/>.</summary>
    public (long Index, long Term) Propose(ReplicationOp op)
    {
        lock (_gate)
        {
            if (_role != RaftRole.Leader)
                throw new NexusException(NexusErrorCode.Standby, "This node is no longer the cluster leader.");
            if (_transferTarget is not null)
                throw new NexusException(NexusErrorCode.Standby, "The leader is handing over to another server; retry in a moment.");
            var entry = new RaftEntry { Index = _log.LastIndex + 1, Term = _log.CurrentTerm, Op = op };
            _log.Append(entry);
            SignalAppendUnsafe();
            AdvanceCommitUnsafe(); // single-node clusters commit at once
            return (entry.Index, entry.Term);
        }
    }

    private void SignalAppendUnsafe()
    {
        if (_log.HasPending && _flushWake.CurrentCount == 0) _flushWake.Release();
        var old = _appendSignal;
        _appendSignal = NewSignal();
        old.TrySetResult();
    }

    private void SignalCommitUnsafe()
    {
        var old = _commitSignal;
        _commitSignal = NewSignal();
        old.TrySetResult();
    }

    private bool ShouldReplicateUnsafe(string peerId, long term) =>
        _role == RaftRole.Leader && _log.CurrentTerm == term
        && (_members.Any(m => m.Id == peerId) || _learners.ContainsKey(peerId));

    /// <summary>
    /// Leader → one peer. Up to <see cref="RaftOptions.MaxInflightAppends"/> AppendEntries calls are in flight at once: the
    /// next batch goes out (optimistically following the previous one) without waiting for the answer, so the round trip
    /// no longer limits throughput. Answers are handled here, in this one loop per peer. A rejected or lost call
    /// drops everything sent after it and resends from the last confirmed point.
    /// </summary>
    private async Task ReplicateLoopAsync(ClusterMember peer, long term)
    {
        var inflight = new List<InflightAppend>();
        long sent; // highest index handed to the peer (confirmed or not)
        lock (_gate) sent = _nextIndex.TryGetValue(peer.Id, out var n0) ? n0 - 1 : 0;
        var lastSend = 0L;
        var backOff = false;
        try
        {
            while (!_stop.IsCancellationRequested)
            {
                // 1. Answers that have arrived, oldest first.
                for (var i = 0; i < inflight.Count; i++)
                {
                    var f = inflight[i];
                    if (!f.Call.IsCompleted) continue;
                    inflight.RemoveAt(i--);
                    f.Drop();
                    var r = f.Call.IsCompletedSuccessfully ? f.Call.Result : null;
                    lock (_gate)
                    {
                        if (r is not null) _lastAck[peer.Id] = DateTime.UtcNow;
                        if (r is not null && r.Term > _log.CurrentTerm) { StepDownUnsafe(r.Term); return; }
                        if (!ShouldReplicateUnsafe(peer.Id, term)) return;
                        if (r is { Success: true })
                        {
                            _matchIndex[peer.Id] = Math.Max(_matchIndex[peer.Id], r.MatchIndex);
                            _nextIndex[peer.Id] = _matchIndex[peer.Id] + 1;
                            sent = Math.Max(sent, _matchIndex[peer.Id]);
                            AdvanceCommitUnsafe();
                            continue;
                        }
                        // Lost (unreachable / timed out) or rejected (log mismatch): later calls built on it are useless.
                        if (r is null) backOff = true;
                        else _nextIndex[peer.Id] = Math.Max(1, Math.Min(r.ConflictIndex, _nextIndex[peer.Id] - 1));
                        sent = _nextIndex[peer.Id] - 1;
                    }
                    foreach (var other in inflight) other.Drop();
                    inflight.Clear();
                    break;
                }

                // 2. Send what we can.
                Task wake;
                var snapshotNeeded = false;
                if (!backOff)
                {
                    lock (_gate)
                    {
                        if (!ShouldReplicateUnsafe(peer.Id, term)) return;
                        if (sent < _log.SnapshotIndex)
                        {
                            snapshotNeeded = inflight.Count == 0; // the peer needs entries we no longer have
                        }
                        else
                        {
                            var heartbeatDue = Stopwatch.GetElapsedTime(lastSend) >= _options.HeartbeatInterval;
                            while (inflight.Count < Math.Max(1, _options.MaxInflightAppends)
                                   && (sent < _log.LastIndex || (heartbeatDue && inflight.Count == 0)))
                            {
                                var entries = _log.Slice(sent + 1, _options.MaxEntriesPerAppend);
                                var append = new AppendRequest(term, _id, sent, _log.TermAt(sent), entries, _commitIndex);
                                inflight.Add(Send(peer, append));
                                sent += entries.Count;
                                lastSend = Stopwatch.GetTimestamp();
                                heartbeatDue = false;
                            }
                        }
                    }
                }
                if (snapshotNeeded)
                {
                    try { await SendSnapshotAsync(peer, term); }
                    catch (OperationCanceledException) when (_stop.IsCancellationRequested) { return; }
                    catch { backOff = true; }
                    lock (_gate) sent = _nextIndex.TryGetValue(peer.Id, out var n1) ? n1 - 1 : sent;
                    continue;
                }

                // 3. Wait for an answer, new entries, or the next heartbeat. After a failure, only for the heartbeat interval.
                lock (_gate) wake = _appendSignal.Task;
                var untilHeartbeat = _options.HeartbeatInterval - Stopwatch.GetElapsedTime(lastSend);
                if (backOff || untilHeartbeat < TimeSpan.FromMilliseconds(1))
                    untilHeartbeat = backOff ? _options.HeartbeatInterval : TimeSpan.FromMilliseconds(1);
                var tick = Task.Delay(untilHeartbeat, _stop.Token);
                if (backOff)
                {
                    await Task.WhenAny(tick).ConfigureAwait(false);
                    backOff = false;
                    lock (_gate) sent = _nextIndex.TryGetValue(peer.Id, out var n2) ? n2 - 1 : sent;
                }
                else
                {
                    await Task.WhenAny([tick, wake, .. inflight.Select(f => (Task)f.Call)]).ConfigureAwait(false);
                }
            }
        }
        catch (OperationCanceledException) when (_stop.IsCancellationRequested) { }
        finally
        {
            foreach (var f in inflight) f.Drop();
        }
    }

    private sealed record InflightAppend(Task<AppendResponse?> Call, CancellationTokenSource Cancel)
    {
        /// <summary>Stop waiting for this call (if still running) and release its timer.</summary>
        public void Drop()
        {
            try { Cancel.Cancel(); } catch (ObjectDisposedException) { }
            Cancel.Dispose();
        }
    }

    private InflightAppend Send(ClusterMember peer, AppendRequest append)
    {
        Interlocked.Increment(ref _appendCalls);
        Interlocked.Add(ref _appendEntriesSent, append.Entries.Count);
        var cts = CancellationTokenSource.CreateLinkedTokenSource(_stop.Token);
        cts.CancelAfter(TimeSpan.FromSeconds(5));
        async Task<AppendResponse?> Call()
        {
            try { return await _transport.AppendEntriesAsync(peer, append, cts.Token).ConfigureAwait(false); }
            catch { return null; }
        }
        return new InflightAppend(Call(), cts);
    }

    private async Task SendSnapshotAsync(ClusterMember peer, long term)
    {
        ReplicationSnapshot snapshot;
        long index, snapTerm;
        List<ClusterMember>? config;
        await ApplyLock.WaitAsync(_stop.Token);
        try
        {
            lock (_gate)
            {
                index = _log.LastApplied;
                snapTerm = _log.TermAt(index);
                config = _log.ConfigAt(index).Config ?? [.. _members];
            }
            snapshot = ReplicationApplier.ExportStore(_store, index);
            snapshot.KeysPayload = ExportKeys?.Invoke();
        }
        finally { ApplyLock.Release(); }

        using var timeout = CancellationTokenSource.CreateLinkedTokenSource(_stop.Token);
        timeout.CancelAfter(TimeSpan.FromSeconds(120));
        var r = await _transport.InstallSnapshotAsync(peer, new SnapshotRequest(term, _id, index, snapTerm, snapshot, config), timeout.Token);
        lock (_gate)
        {
            if (r is null) return;
            _lastAck[peer.Id] = DateTime.UtcNow;
            if (r.Term > _log.CurrentTerm) { StepDownUnsafe(r.Term); return; }
            if (!_matchIndex.ContainsKey(peer.Id)) return;
            _matchIndex[peer.Id] = Math.Max(_matchIndex[peer.Id], index);
            _nextIndex[peer.Id] = _matchIndex[peer.Id] + 1;
            AdvanceCommitUnsafe();
        }
    }

    private void AdvanceCommitUnsafe()
    {
        if (_role != RaftRole.Leader) return;
        for (var n = _log.LastIndex; n > _commitIndex; n--)
        {
            if (_log.TermAt(n) != _log.CurrentTerm) break; // only entries of our own term are committed by counting
            // The leader counts itself only for entries already in its own file (group commit, see FlushLoopAsync).
            var count = (IsVoterUnsafe && _log.DurableIndex >= n ? 1 : 0) + PeersUnsafe.Count(p => _matchIndex.TryGetValue(p.Id, out var m) && m >= n);
            if (count >= Majority)
            {
                _commitIndex = n;
                SignalCommitUnsafe();
                _applyWake.Release();
                // A leader removed from the membership leads until that change commits, then steps aside.
                if (!IsVoterUnsafe && _configIndex <= _commitIndex)
                {
                    // Hand over to the most caught-up member straight away instead of leaving them to time out.
                    var next = PeersUnsafe.OrderByDescending(p => _matchIndex.GetValueOrDefault(p.Id)).FirstOrDefault();
                    var req = new TimeoutNowRequest(_log.CurrentTerm, _id, _matchIndex.GetValueOrDefault(next?.Id ?? ""));
                    StepDownUnsafe(_log.CurrentTerm);
                    _leaderId = null;
                    if (next is not null) _ = SendTimeoutNowAsync(next, req);
                }
                break;
            }
        }
    }

    /// <summary>
    /// Group commit on the leader: everything appended since the last pass is written (and fsynced) at once, outside the
    /// Raft lock, so proposals keep flowing while the disk works. Replication to followers does not wait for it.
    /// </summary>
    private async Task FlushLoopAsync()
    {
        while (!_stop.IsCancellationRequested)
        {
            try { await _flushWake.WaitAsync(TimeSpan.FromMilliseconds(200), _stop.Token); }
            catch (OperationCanceledException) { return; }

            RaftLog.PendingWrite pending;
            lock (_gate)
            {
                if (!_log.HasPending) continue;
                pending = _log.TakePending();
            }
            try
            {
                if (BeforeFlushForTests is { } hook) await hook();
                _log.Flush(pending);
            }
            catch (Exception)
            {
                // The write failed (disk full, …): rewrite the whole file from memory so no entry is skipped. If that fails
                // too, this node simply never counts itself; a majority of followers can still commit.
                try { lock (_gate) _log.RewriteAll(); } catch { }
            }
            lock (_gate) AdvanceCommitUnsafe();
        }
    }

    /// <summary>Wait until the entry is committed by a majority. False if leadership was lost or the timeout passed (outcome unknown).</summary>
    public async Task<bool> WaitCommittedAsync(long index, long term, TimeSpan timeout, CancellationToken ct = default)
    {
        var deadline = DateTime.UtcNow + timeout;
        while (true)
        {
            Task wake;
            lock (_gate)
            {
                if (_log.TermAt(index) != term && index > _log.SnapshotIndex) return false; // overwritten
                if (_commitIndex >= index) return true;
                if (_role != RaftRole.Leader || _log.CurrentTerm != term) return false;
                wake = _commitSignal.Task;
            }
            var left = deadline - DateTime.UtcNow;
            if (left <= TimeSpan.Zero) return false;
            try { await wake.WaitAsync(left, ct); } catch (TimeoutException) { return false; }
        }
    }

    // ================================================================== membership changes (leader)

    /// <summary>
    /// Add a server: it first catches up as a non-voting learner, then a configuration entry makes it a voter.
    /// One change at a time. Returns once the new configuration is committed.
    /// </summary>
    public async Task AddServerAsync(ClusterMember member, CancellationToken ct = default)
    {
        long term;
        lock (_gate)
        {
            RequireLeaderForChangeUnsafe();
            if (_members.Any(m => m.Id == member.Id)) throw new NexusException(NexusErrorCode.EntityAlreadyExists, $"Server '{member.Id}' is already a member.");
            if (_members.Any(m => m.Url.TrimEnd('/').Equals(member.Url.TrimEnd('/'), StringComparison.OrdinalIgnoreCase)))
                throw new NexusException(NexusErrorCode.EntityAlreadyExists, $"A member already uses {member.Url}.");
            term = _log.CurrentTerm;
            _learners[member.Id] = member;
            _nextIndex[member.Id] = _log.LastIndex + 1;
            _matchIndex[member.Id] = 0;
            _lastAck[member.Id] = DateTime.MinValue;
            StartReplicationUnsafe(member, term);
        }

        // Catch-up: wait until the learner is close to our log, so adding it does not stall commits.
        var deadline = DateTime.UtcNow + _options.CatchUpTimeout;
        while (true)
        {
            lock (_gate)
            {
                if (_role != RaftRole.Leader || _log.CurrentTerm != term)
                    throw new NexusException(NexusErrorCode.Standby, "Leadership changed while adding the server; retry on the new leader.");
                if (_matchIndex.TryGetValue(member.Id, out var match) && match > 0 && match >= _log.LastIndex - _options.MaxEntriesPerAppend) break;
            }
            if (DateTime.UtcNow > deadline)
            {
                lock (_gate) _learners.Remove(member.Id);
                throw new NexusException(NexusErrorCode.BadRequest,
                    $"Server '{member.Id}' at {member.Url} did not catch up within {_options.CatchUpTimeout.TotalSeconds:0}s. Is it running with JoinExisting and the cluster secret?");
            }
            await Task.Delay(50, ct);
        }

        await AppendConfigAsync(m => [.. m, member], ct);
    }

    /// <summary>Remove a server (possibly this leader, which then steps down once the change commits).</summary>
    public async Task RemoveServerAsync(string id, CancellationToken ct = default)
    {
        lock (_gate)
        {
            RequireLeaderForChangeUnsafe();
            if (_members.All(m => m.Id != id)) throw new NexusException(NexusErrorCode.EntityNotFound, $"Server '{id}' is not a member.");
            if (_members.Count == 1) throw new NexusException(NexusErrorCode.BadRequest, "Cannot remove the last member.");
        }
        await AppendConfigAsync(m => m.Where(x => x.Id != id).ToList(), ct);
    }

    private void RequireLeaderForChangeUnsafe()
    {
        if (_role != RaftRole.Leader || !_leaderReady)
            throw new NexusException(NexusErrorCode.Standby, "Membership changes must be made on the cluster leader.");
        if (_configIndex > _commitIndex)
            throw new NexusException(NexusErrorCode.BadRequest, "Another membership change is still in progress; try again shortly.");
        if (_transferTarget is not null)
            throw new NexusException(NexusErrorCode.Standby, "The leader is handing over to another server; retry in a moment.");
    }

    private async Task AppendConfigAsync(Func<List<ClusterMember>, List<ClusterMember>> change, CancellationToken ct)
    {
        long index, term;
        lock (_gate)
        {
            RequireLeaderForChangeUnsafe();
            var entry = new RaftEntry { Index = _log.LastIndex + 1, Term = _log.CurrentTerm, Config = change([.. _members]) };
            _log.Append(entry);
            (index, term) = (entry.Index, entry.Term);
            RefreshConfigUnsafe(); // in effect immediately on the leader
            SignalAppendUnsafe();
            AdvanceCommitUnsafe();
        }
        if (!await WaitCommittedAsync(index, term, TimeSpan.FromSeconds(30), ct))
        {
            // A leader that removed itself steps down as the change commits, which also ends the wait: check the outcome.
            lock (_gate) if (_commitIndex >= index && _log.TermAt(index) == term) return;
            throw new NexusException(NexusErrorCode.NotCommitted, "The membership change was not confirmed by the cluster in time.");
        }
    }

    // ================================================================== follower RPC handlers

    public AppendResponse HandleAppend(AppendRequest req)
    {
        lock (_gate)
        {
            if (req.Term < _log.CurrentTerm) return new AppendResponse(_log.CurrentTerm, false, 0, 0);
            if (req.Term > _log.CurrentTerm || _role != RaftRole.Follower) StepDownUnsafe(req.Term);
            _leaderId = req.LeaderId;
            ResetElectionTimer();

            if (req.PrevLogIndex > _log.LastIndex)
                return new AppendResponse(_log.CurrentTerm, false, 0, _log.LastIndex + 1);
            if (req.PrevLogIndex > _log.SnapshotIndex && _log.TermAt(req.PrevLogIndex) != req.PrevLogTerm)
            {
                var conflictTerm = _log.TermAt(req.PrevLogIndex);
                var i = req.PrevLogIndex;
                while (i - 1 > _log.SnapshotIndex && _log.TermAt(i - 1) == conflictTerm) i--;
                return new AppendResponse(_log.CurrentTerm, false, 0, i);
            }

            var changedConfig = false;
            foreach (var e in req.Entries)
            {
                if (e.Index <= _log.SnapshotIndex) continue;
                if (e.Index <= _log.LastIndex)
                {
                    if (_log.TermAt(e.Index) == e.Term) continue;
                    if (_configIndex >= e.Index) changedConfig = true; // a configuration may be truncated away
                    _log.TruncateFrom(e.Index);
                }
                _log.Append(e);
                if (e.Config is not null) changedConfig = true;
            }
            if (changedConfig) RefreshConfigUnsafe();

            // Acknowledge only what is durable: one write (and fsync) per call for all its entries.
            if (_log.HasPending) _log.FlushNow();

            var lastNew = req.PrevLogIndex + req.Entries.Count;
            if (req.LeaderCommit > _commitIndex)
            {
                _commitIndex = Math.Min(req.LeaderCommit, Math.Max(lastNew, req.PrevLogIndex));
                _applyWake.Release();
            }
            return new AppendResponse(_log.CurrentTerm, true, lastNew, 0);
        }
    }

    public async Task<SnapshotResponse> HandleSnapshotAsync(SnapshotRequest req)
    {
        lock (_gate)
        {
            if (req.Term < _log.CurrentTerm) return new SnapshotResponse(_log.CurrentTerm);
            if (req.Term > _log.CurrentTerm || _role != RaftRole.Follower) StepDownUnsafe(req.Term);
            _leaderId = req.LeaderId;
            ResetElectionTimer();
            if (req.LastIncludedIndex <= _log.LastApplied) return new SnapshotResponse(_log.CurrentTerm);
        }

        await ApplyLock.WaitAsync();
        try
        {
            ReplicationApplier.ApplySnapshot(_store, req.Snapshot);
            KeysApplied?.Invoke(req.Snapshot.KeysPayload);
            lock (_gate)
            {
                _log.ResetToSnapshot(req.LastIncludedIndex, req.LastIncludedTerm, req.Config);
                if (req.Config is not null) (_members, _configIndex) = (req.Config, req.LastIncludedIndex);
                _commitIndex = Math.Max(_commitIndex, req.LastIncludedIndex);
                ResetElectionTimer();
                return new SnapshotResponse(_log.CurrentTerm);
            }
        }
        finally { ApplyLock.Release(); }
    }

    // ================================================================== applying committed entries

    private async Task ApplyLoopAsync()
    {
        var sinceSave = 0;
        var lastSave = Stopwatch.GetTimestamp();
        while (!_stop.IsCancellationRequested)
        {
            try { await _applyWake.WaitAsync(TimeSpan.FromMilliseconds(200), _stop.Token); }
            catch (OperationCanceledException) { return; }

            var becameReady = false;
            await ApplyLock.WaitAsync();
            // One store flush per batch of applied entries (always before LastApplied is saved or the log compacted).
            var batch = (_store as FileMessageStore)?.DeferFlush();
            try
            {
                while (true)
                {
                    RaftEntry entry;
                    lock (_gate)
                    {
                        if (_log.LastApplied >= _commitIndex || _log.LastApplied >= _log.LastIndex) break;
                        entry = _log.Get(_log.LastApplied + 1);
                    }
                    if (entry.Op is { } op)
                    {
                        if (ReplicationApplier.Apply(_store, op) is { } keys) KeysApplied?.Invoke(keys);
                        if (op.Entity is { } e && op.Kind == ReplicationOpKind.Put && _store.NeedsCompaction(e))
                            _store.Compact(e, _store.Load(e).ToList());
                    }
                    if (++sinceSave >= 500)
                    {
                        (_store as FileMessageStore)?.Flush(); // the store must hold what LastApplied claims
                        sinceSave = 0;
                        lastSave = Stopwatch.GetTimestamp();
                        lock (_gate) _log.SetLastApplied(entry.Index, persist: true);
                    }
                    else lock (_gate) _log.SetLastApplied(entry.Index, persist: false);
                }
                batch?.Dispose(); // flush before saving LastApplied / compacting below

                lock (_gate)
                {
                    // LastApplied is only a restart hint (replaying applied entries in order is harmless), so it is saved
                    // every 500 entries or once a second, not after every batch: that rewrite used to cost more than the apply.
                    if (sinceSave > 0 && Stopwatch.GetElapsedTime(lastSave) >= TimeSpan.FromSeconds(1))
                    {
                        _log.SaveState();
                        sinceSave = 0;
                        lastSave = Stopwatch.GetTimestamp();
                    }
                    if (_role == RaftRole.Leader && !_leaderReady && _log.LastApplied >= _noopIndex)
                    {
                        _leaderReady = true;
                        becameReady = true;
                    }
                    if (_log.Count > _options.MaxLogEntries)
                    {
                        var keep = _options.MaxLogEntries / 10;
                        var upTo = _log.LastApplied - keep;
                        if (_role == RaftRole.Leader && _matchIndex.Count > 0)
                            upTo = Math.Min(upTo, Math.Max(_matchIndex.Values.Min(), _log.LastApplied - _options.MaxLogEntries));
                        if (upTo > _log.SnapshotIndex) _log.CompactTo(upTo);
                    }
                }
            }
            catch (Exception)
            {
                // A failing apply (disk full, …) is retried on the next wake-up; the log still holds the entries.
            }
            finally
            {
                try { batch?.Dispose(); } catch { /* flush failed: LastApplied was not saved past it, so it is replayed */ }
                ApplyLock.Release();
            }

            if (becameReady) LeadershipChanged?.Invoke(true);
        }
    }

    public RaftStatus Status()
    {
        lock (_gate)
        {
            return new RaftStatus
            {
                NodeId = _id,
                Role = _role,
                Term = _log.CurrentTerm,
                LeaderId = _leaderId,
                LeaderReady = _role == RaftRole.Leader && _leaderReady,
                IsVoter = IsVoterUnsafe,
                LastLogIndex = _log.LastIndex,
                CommitIndex = _commitIndex,
                LastApplied = _log.LastApplied,
                SnapshotIndex = _log.SnapshotIndex,
                Members = [.. _members],
                Learners = [.. _learners.Values],
                PeerMatchIndex = _role == RaftRole.Leader ? new Dictionary<string, long>(_matchIndex) : new(),
                PeerLastContactMs = _role == RaftRole.Leader
                    ? _lastAck.ToDictionary(kv => kv.Key, kv => kv.Value == DateTime.MinValue ? (long?)null : (long)(DateTime.UtcNow - kv.Value).TotalMilliseconds)
                    : new(),
                AppendCallsSent = Interlocked.Read(ref _appendCalls),
                AppendEntriesSent = Interlocked.Read(ref _appendEntriesSent),
                PreVoteRounds = Interlocked.Read(ref _preVotes),
                ElectionsStarted = Interlocked.Read(ref _elections),
                TransfersOut = Interlocked.Read(ref _transfersOut),
                TransfersIn = Interlocked.Read(ref _transfersIn),
                TransferringTo = _transferTarget
            };
        }
    }

    /// <summary>
    /// Stop taking part (no heartbeats, votes, elections or replication) without closing the log, e.g. while the host shuts
    /// down. Requests from other servers are still answered from the log until <see cref="Dispose"/>.
    /// </summary>
    public void Stop()
    {
        lock (_gate)
        {
            if (_stop.IsCancellationRequested) return;
            if (_role == RaftRole.Leader) StepDownUnsafe(_log.CurrentTerm);
        }
        _stop.Cancel();
    }

    public void Dispose()
    {
        _stop.Cancel();
        try { Task.WaitAll([.. new[] { _electionLoop, _applyLoop, _flushLoop }.OfType<Task>()], TimeSpan.FromSeconds(2)); } catch { }
        lock (_gate) _log.SaveState();
        _log.Dispose();
    }
}

/// <summary>
/// The leader's broker writes through this store: every change becomes a Raft log entry instead of a local write.
/// The change reaches every node's store (including this one) only once committed. Reads (at broker start-up) come
/// from the local store, which holds the committed state.
/// </summary>
public sealed class RaftStore(IMessageStore committed, RaftNode raft) : IMessageStore
{
    /// <summary>The highest log entry proposed in the current async flow (one HTTP request or WebSocket operation).</summary>
    private static readonly AsyncLocal<WriteTracker?> Current = new();

    public sealed class WriteTracker
    {
        public long Index { get; internal set; }
        public long Term { get; internal set; }
    }

    /// <summary>Start tracking the writes of the current request; await the tracker after the request's work.</summary>
    public static WriteTracker Track() => Current.Value = new WriteTracker();

    /// <summary>The tracker of the current async flow, if any.</summary>
    public static WriteTracker? CurrentTracker => Current.Value;

    private long Propose(ReplicationOp op)
    {
        var (index, term) = raft.Propose(op);
        if (Current.Value is { } t && index > t.Index) { t.Index = index; t.Term = term; }
        return index;
    }

    public void Put(string entityPath, BrokerMessage message)
    {
        var copy = message.Clone();
        if (copy.State == MessageState.Locked) copy.State = MessageState.Active;
        // Remember which log entry must commit before this message may be delivered (committed-only delivery).
        message.RequiredCommitIndex = Propose(new ReplicationOp { Kind = ReplicationOpKind.Put, Entity = entityPath, Seq = message.SequenceNumber, Message = copy });
    }

    public void Remove(string entityPath, long sequenceNumber) =>
        Propose(new ReplicationOp { Kind = ReplicationOpKind.Remove, Entity = entityPath, Seq = sequenceNumber });

    public void DeleteEntity(string entityPath) =>
        Propose(new ReplicationOp { Kind = ReplicationOpKind.DeleteEntity, Entity = entityPath });

    public void SaveTopology(TopologySnapshot topology) =>
        Propose(new ReplicationOp { Kind = ReplicationOpKind.Topology, Topology = topology });

    public void ProposeKeys(string json) => Propose(new ReplicationOp { Kind = ReplicationOpKind.Keys, Payload = json });

    // Journal compaction happens on apply (every node compacts its own store); the broker never needs to.
    public bool NeedsCompaction(string entityPath) => false;
    public void Compact(string entityPath, IEnumerable<BrokerMessage> liveMessages) { }

    public IEnumerable<BrokerMessage> Load(string entityPath) => committed.Load(entityPath);
    public TopologySnapshot? LoadTopology() => committed.LoadTopology();
    public IEnumerable<string> ListEntities() => committed.ListEntities();
    public void Clear() { }
    public void Dispose() { } // the committed store is owned by the host
}
