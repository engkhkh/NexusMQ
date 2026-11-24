using System.Collections.Concurrent;
using NexusMQ.Core;
using NexusMQ.Core.Cluster;
using NexusMQ.Core.Persistence;
using NexusMQ.Core.Replication;

namespace NexusMQ.Tests;

/// <summary>An in-memory network between Raft nodes that can cut nodes off.</summary>
internal sealed class FakeNetwork : IRaftTransport
{
    public readonly ConcurrentDictionary<string, RaftNode> Nodes = new();
    public readonly ConcurrentDictionary<string, bool> Down = new();
    private readonly string _from;
    private readonly FakeNetwork? _root;

    public FakeNetwork() => _from = "";
    private FakeNetwork(FakeNetwork root, string from) { _root = root; _from = from; }
    public FakeNetwork For(string from) => new(this, from);

    private FakeNetwork Root => _root ?? this;
    private bool Reachable(string to) => !Root.Down.ContainsKey(to) && !Root.Down.ContainsKey(_from) && Root.Nodes.ContainsKey(to);

    public async Task<VoteResponse?> RequestVoteAsync(ClusterMember peer, VoteRequest r, CancellationToken ct)
    {
        await Task.Yield();
        return Reachable(peer.Id) ? Root.Nodes[peer.Id].HandleVote(r) : null;
    }

    /// <summary>Peers that answer PreVote like a version without it (yes, without being asked).</summary>
    public readonly ConcurrentDictionary<string, bool> NoPreVote = new();

    public async Task<VoteResponse?> RequestPreVoteAsync(ClusterMember peer, VoteRequest r, CancellationToken ct)
    {
        await Task.Yield();
        if (!Reachable(peer.Id)) return null;
        return Root.NoPreVote.ContainsKey(peer.Id) ? new VoteResponse(r.Term - 1, true) : Root.Nodes[peer.Id].HandlePreVote(r);
    }

    public async Task<AppendResponse?> AppendEntriesAsync(ClusterMember peer, AppendRequest r, CancellationToken ct)
    {
        await Task.Yield();
        // Entries are passed by reference in-process; copy the list like a real network would.
        return Reachable(peer.Id) ? Root.Nodes[peer.Id].HandleAppend(r with { Entries = [.. r.Entries] }) : null;
    }

    public async Task<SnapshotResponse?> InstallSnapshotAsync(ClusterMember peer, SnapshotRequest r, CancellationToken ct) =>
        Reachable(peer.Id) ? await Root.Nodes[peer.Id].HandleSnapshotAsync(r) : null;
}

public sealed class RaftTests : IDisposable
{
    private readonly string _root = Path.Combine(Path.GetTempPath(), "nexusmq-raft-" + Guid.NewGuid().ToString("N"));
    private readonly FakeNetwork _net = new();
    private readonly List<ClusterMember> _members = [new() { Id = "a" }, new() { Id = "b" }, new() { Id = "c" }];
    private readonly Dictionary<string, FileMessageStore> _stores = new();
    private static readonly RaftOptions Fast = new()
    {
        ElectionTimeout = TimeSpan.FromMilliseconds(150),
        HeartbeatInterval = TimeSpan.FromMilliseconds(30),
        MaxLogEntries = 1_000_000
    };

    private RaftNode Start(string id, RaftOptions? options = null)
    {
        var dir = Path.Combine(_root, id);
        var store = _stores[id] = new FileMessageStore(Path.Combine(dir, "store"));
        var node = new RaftNode(id, _members, new RaftLog(Path.Combine(dir, "raft")), store, _net.For(id), options ?? Fast);
        _net.Nodes[id] = node;
        node.Start();
        return node;
    }

    private void StartAll(RaftOptions? options = null) { foreach (var m in _members) Start(m.Id, options); }

    private async Task<RaftNode> Leader(params string[] excluding)
    {
        for (var i = 0; i < 200; i++)
        {
            var leaders = _net.Nodes.Values.Where(n => !excluding.Contains(n.Id) && !_net.Down.ContainsKey(n.Id) && n.IsReadyLeader).ToList();
            if (leaders.Count == 1) return leaders[0];
            await Task.Delay(25);
        }
        throw new TimeoutException("No single leader elected.");
    }

    private static ReplicationOp Put(string entity, long seq, string body) => new()
    {
        Kind = ReplicationOpKind.Put, Entity = entity, Seq = seq,
        Message = new BrokerMessage { SequenceNumber = seq, Body = System.Text.Encoding.UTF8.GetBytes(body) }
    };

    private async Task Eventually(Func<bool> condition, int ms = 5000)
    {
        for (var waited = 0; waited < ms; waited += 25)
        {
            if (condition()) return;
            await Task.Delay(25);
        }
        throw new TimeoutException("Condition not met.");
    }

    private int Count(string node, string entity) { lock (_stores[node]) return _stores[node].Load(entity).Count(); }

    [Fact]
    public async Task Elects_one_leader_and_commits_to_every_store()
    {
        StartAll();
        var leader = await Leader();
        for (var i = 1; i <= 20; i++)
        {
            var (index, term) = leader.Propose(Put("orders", i, $"m{i}"));
            Assert.True(await leader.WaitCommittedAsync(index, term, TimeSpan.FromSeconds(5)));
        }
        foreach (var id in new[] { "a", "b", "c" }) await Eventually(() => Count(id, "orders") == 20);
        Assert.Equal(1, _net.Nodes.Values.Count(n => n.Role == RaftRole.Leader));
    }

    [Fact]
    public async Task Leader_cut_off_cannot_commit_and_the_majority_moves_on()
    {
        StartAll();
        var old = await Leader();
        var (i1, t1) = old.Propose(Put("q", 1, "committed"));
        Assert.True(await old.WaitCommittedAsync(i1, t1, TimeSpan.FromSeconds(5)));

        _net.Down[old.Id] = true; // network partition: the old leader is alone
        var (i2, t2) = old.Propose(Put("q", 2, "lost"));
        Assert.False(await old.WaitCommittedAsync(i2, t2, TimeSpan.FromMilliseconds(600))); // no majority, no confirmation

        var fresh = await Leader(excluding: old.Id);
        Assert.NotEqual(old.Id, fresh.Id);
        var (i3, t3) = fresh.Propose(Put("q", 3, "new-leader"));
        Assert.True(await fresh.WaitCommittedAsync(i3, t3, TimeSpan.FromSeconds(5)));

        _net.Down.TryRemove(old.Id, out _); // heal: the old leader steps down and drops its unconfirmed write
        await Eventually(() => old.Role == RaftRole.Follower);
        await Eventually(() => Count(old.Id, "q") == 2);
        lock (_stores[old.Id])
            Assert.Equal([1L, 3L], _stores[old.Id].Load("q").Select(m => m.SequenceNumber).OrderBy(x => x).ToArray());
    }

    [Fact]
    public async Task Minority_cannot_elect_a_leader_and_a_cut_off_leader_steps_down()
    {
        StartAll();
        var leader = await Leader();
        foreach (var other in _members.Where(m => m.Id != leader.Id)) _net.Down[other.Id] = true; // leader alone
        await Eventually(() => !leader.IsReadyLeader, ms: 2000); // check quorum: it stops leading
        await Task.Delay(600);
        Assert.False(leader.IsReadyLeader); // and cannot get re-elected without a majority
    }

    [Fact]
    public async Task Lagging_follower_catches_up_through_a_snapshot()
    {
        var small = new RaftOptions { ElectionTimeout = Fast.ElectionTimeout, HeartbeatInterval = Fast.HeartbeatInterval, MaxLogEntries = 20 };
        StartAll(small);
        var leader = await Leader();
        var lagger = _net.Nodes.Values.First(n => n.Id != leader.Id);
        _net.Down[lagger.Id] = true;

        for (var i = 1; i <= 100; i++)
        {
            var (index, term) = leader.Propose(Put("big", i, $"m{i}"));
            Assert.True(await leader.WaitCommittedAsync(index, term, TimeSpan.FromSeconds(5)));
        }
        await Eventually(() => leader.Status().SnapshotIndex > 0); // the leader compacted its log

        _net.Down.TryRemove(lagger.Id, out _);
        // Wait for both together: the store is filled from the snapshot just before the log records it (both under the
        // apply lock), so checking the snapshot index right after seeing 100 messages could catch the moment in between.
        await Eventually(() => Count(lagger.Id, "big") == 100 && lagger.Status().SnapshotIndex > 0, ms: 10000);
    }

    [Fact]
    public async Task Term_vote_and_log_survive_a_restart()
    {
        StartAll();
        var leader = await Leader();
        var (index, term) = leader.Propose(Put("q", 1, "x"));
        Assert.True(await leader.WaitCommittedAsync(index, term, TimeSpan.FromSeconds(5)));
        var id = leader.Id;

        _net.Nodes.TryRemove(id, out _);
        leader.Dispose();
        _stores[id].Dispose();

        using var log = new RaftLog(Path.Combine(_root, id, "raft"));
        Assert.True(log.CurrentTerm >= term);
        Assert.True(log.LastIndex >= index);
        Assert.Equal(term, log.TermAt(index));
    }

    [Fact]
    public async Task A_follower_cut_off_and_reconnected_does_not_disturb_the_leader()
    {
        StartAll();
        var leader = await Leader();
        var term = leader.CurrentTerm;
        var follower = _net.Nodes.Values.First(n => n.Id != leader.Id);
        var other = _net.Nodes.Values.First(n => n.Id != leader.Id && n.Id != follower.Id);

        await Eventually(() => follower.Status().LeaderId == leader.Id && follower.CurrentTerm == term);
        var (rounds, elections) = (follower.Status().PreVoteRounds, follower.Status().ElectionsStarted); // (it may have run in the first election)

        _net.Down[follower.Id] = true;
        await Eventually(() => follower.Status().PreVoteRounds >= rounds + 3, ms: 5000); // its timer keeps expiring...
        Assert.Equal(term, follower.CurrentTerm);                                        // ...but it never raises its term
        Assert.Equal(elections, follower.Status().ElectionsStarted);
        Assert.Null(follower.Status().LeaderId);                                // and no longer points clients at the leader

        _net.Down.TryRemove(follower.Id, out _);
        await Eventually(() => follower.Status().LeaderId == leader.Id);
        var (index, t) = leader.Propose(Put("q", 1, "after"));
        Assert.True(await leader.WaitCommittedAsync(index, t, TimeSpan.FromSeconds(5)));
        await Eventually(() => Count(follower.Id, "q") == 1);
        Assert.True(leader.IsReadyLeader);                 // same leader,
        Assert.Equal(term, leader.CurrentTerm);            // same term: no election happened
        Assert.Equal(term, other.CurrentTerm);
    }

    [Fact]
    public async Task PreVote_is_refused_while_the_leader_is_alive_and_changes_nothing()
    {
        StartAll();
        var leader = await Leader();
        var follower = _net.Nodes.Values.First(n => n.Id != leader.Id);
        var before = follower.Status();
        var probe = new VoteRequest(before.Term + 5, "x", long.MaxValue, long.MaxValue);
        Assert.False(follower.HandlePreVote(probe).Granted); // hears from the leader: no
        Assert.False(leader.HandlePreVote(probe).Granted);   // is the leader: no
        Assert.Equal(before.Term, follower.CurrentTerm);    // and nothing was recorded
        Assert.Equal(leader.Id, follower.Status().LeaderId);

        // Once the leader is gone, the remaining two agree and elect one of themselves.
        _net.Down[leader.Id] = true;
        var fresh = await Leader(excluding: leader.Id);
        Assert.NotEqual(leader.Id, fresh.Id);
        Assert.True(fresh.CurrentTerm > before.Term);
    }

    [Fact]
    public async Task PreVote_refuses_a_server_whose_log_is_behind()
    {
        StartAll();
        var leader = await Leader();
        var (index, t) = leader.Propose(Put("q", 1, "x"));
        Assert.True(await leader.WaitCommittedAsync(index, t, TimeSpan.FromSeconds(5)));
        var follower = _net.Nodes.Values.First(n => n.Id != leader.Id);
        await Eventually(() => Count(follower.Id, "q") == 1);
        foreach (var n in _net.Nodes.Keys.Where(id => id != follower.Id)) _net.Down[n] = true; // alone: no leader, no election
        await Eventually(() => follower.Status().LeaderId is null && follower.Status().PreVoteRounds > 0);
        var term = follower.CurrentTerm;
        Assert.False(follower.HandlePreVote(new VoteRequest(term + 1, "x", 0, 0)).Granted);                  // log behind: no
        var mine = follower.Status();
        Assert.True(follower.HandlePreVote(new VoteRequest(term + 1, "x", mine.LastLogIndex, leader.CurrentTerm)).Granted); // same log: yes
        Assert.Equal(term, follower.CurrentTerm);
    }

    [Fact]
    public async Task Mixed_cluster_with_peers_without_PreVote_still_elects()
    {
        foreach (var m in _members.Skip(1)) _net.NoPreVote[m.Id] = true; // a rolling upgrade: only "a" has PreVote
        StartAll();
        var leader = await Leader();
        var (index, t) = leader.Propose(Put("q", 1, "x"));
        Assert.True(await leader.WaitCommittedAsync(index, t, TimeSpan.FromSeconds(5)));
        _net.Down[leader.Id] = true;
        var fresh = await Leader(excluding: leader.Id);
        Assert.NotEqual(leader.Id, fresh.Id);
    }

    public void Dispose()
    {
        foreach (var n in _net.Nodes.Values) n.Dispose();
        foreach (var s in _stores.Values) s.Dispose();
        try { Directory.Delete(_root, true); } catch { }
    }
}
