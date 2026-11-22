using System.Text;
using NexusMQ.Core;
using NexusMQ.Core.Cluster;
using NexusMQ.Core.Persistence;
using NexusMQ.Core.Replication;

namespace NexusMQ.Tests;

public sealed class RaftMembershipTests : IDisposable
{
    private readonly string _root = Path.Combine(Path.GetTempPath(), "nexusmq-raftm-" + Guid.NewGuid().ToString("N"));
    private readonly FakeNetwork _net = new();
    private readonly List<ClusterMember> _initial = [new() { Id = "a", Url = "a" }, new() { Id = "b", Url = "b" }, new() { Id = "c", Url = "c" }];
    private readonly Dictionary<string, FileMessageStore> _stores = new();

    private static RaftOptions Opts(bool join = false, TimeSpan? catchUp = null) => new()
    {
        ElectionTimeout = TimeSpan.FromMilliseconds(150),
        HeartbeatInterval = TimeSpan.FromMilliseconds(30),
        JoinExisting = join,
        CatchUpTimeout = catchUp ?? TimeSpan.FromSeconds(10)
    };

    private RaftNode Start(string id, bool join = false, TimeSpan? catchUp = null)
    {
        var dir = Path.Combine(_root, id);
        var store = _stores[id] = new FileMessageStore(Path.Combine(dir, "store"));
        var node = new RaftNode(id, _initial, new RaftLog(Path.Combine(dir, "raft")), store, _net.For(id), Opts(join, catchUp));
        _net.Nodes[id] = node;
        node.Start();
        return node;
    }

    private async Task<RaftNode> Leader(params string[] excluding)
    {
        for (var i = 0; i < 300; i++)
        {
            var leaders = _net.Nodes.Values.Where(n => !excluding.Contains(n.Id) && !_net.Down.ContainsKey(n.Id) && n.IsReadyLeader).ToList();
            if (leaders.Count == 1) return leaders[0];
            await Task.Delay(25);
        }
        throw new TimeoutException("No single leader elected.");
    }

    private static async Task Eventually(Func<bool> condition, int ms = 5000)
    {
        for (var waited = 0; waited < ms; waited += 25)
        {
            if (condition()) return;
            await Task.Delay(25);
        }
        throw new TimeoutException("Condition not met.");
    }

    private static ReplicationOp Put(string entity, long seq) => new()
    {
        Kind = ReplicationOpKind.Put, Entity = entity, Seq = seq, Message = new BrokerMessage { SequenceNumber = seq, Body = [1] }
    };

    private async Task Commit(RaftNode leader, string entity, long seq)
    {
        var (i, t) = leader.Propose(Put(entity, seq));
        Assert.True(await leader.WaitCommittedAsync(i, t, TimeSpan.FromSeconds(5)));
    }

    private int Count(string node, string entity) => _stores[node].Load(entity).Count();

    [Fact]
    public async Task New_server_joins_catches_up_and_votes()
    {
        foreach (var m in _initial) Start(m.Id);
        var leader = await Leader();
        for (var i = 1; i <= 30; i++) await Commit(leader, "q", i);

        var d = Start("d", join: true);
        await Task.Delay(400);
        Assert.Equal(RaftRole.Follower, d.Role); // a joining server never starts elections on its own

        await leader.AddServerAsync(new ClusterMember { Id = "d", Url = "d" });
        Assert.Equal(["a", "b", "c", "d"], leader.Members.Select(m => m.Id).OrderBy(x => x).ToArray());
        await Eventually(() => d.Members.Count == 4 && Count("d", "q") == 30);

        // 4 members: a majority is 3, so with one original server down commits still work thanks to d.
        var downId = _initial.Select(m => m.Id).First(id => id != leader.Id);
        _net.Down[downId] = true;
        await Commit(leader, "q", 31);
        await Eventually(() => Count("d", "q") == 31);
    }

    [Fact]
    public async Task Removed_follower_leaves_quietly_and_the_cluster_keeps_committing()
    {
        foreach (var m in _initial) Start(m.Id);
        var leader = await Leader();
        var victim = _initial.Select(m => m.Id).First(id => id != leader.Id);

        await leader.RemoveServerAsync(victim);
        Assert.DoesNotContain(leader.Members, m => m.Id == victim);
        var term = leader.CurrentTerm;

        await Task.Delay(1000); // the removed server times out repeatedly; leader stickiness keeps it from disrupting
        Assert.True(leader.IsReadyLeader);
        Assert.Equal(term, leader.CurrentTerm);
        await Commit(leader, "q", 1);
    }

    [Fact]
    public async Task Leader_can_remove_itself_and_a_new_leader_takes_over()
    {
        foreach (var m in _initial) Start(m.Id);
        var leader = await Leader();
        await leader.RemoveServerAsync(leader.Id);
        await Eventually(() => !leader.IsReadyLeader);
        var next = await Leader(excluding: leader.Id);
        Assert.Equal(2, next.Members.Count);
        await Commit(next, "q", 1);
    }

    [Fact]
    public async Task Adding_an_unreachable_server_fails_without_changing_membership()
    {
        foreach (var m in _initial) Start(m.Id);
        var leader = await Leader();
        var ex = await Assert.ThrowsAsync<NexusException>(() => leader.AddServerAsync(new ClusterMember { Id = "ghost", Url = "ghost" }));
        Assert.Contains("did not catch up", ex.Message);
        Assert.Equal(3, leader.Members.Count);
        await Commit(leader, "q", 1);
    }

    [Fact]
    public async Task Membership_survives_a_restart()
    {
        foreach (var m in _initial) Start(m.Id);
        var leader = await Leader();
        var victim = _initial.Select(m => m.Id).First(id => id != leader.Id);
        await leader.RemoveServerAsync(victim);
        var id = leader.Id;

        _net.Nodes.TryRemove(id, out _);
        leader.Dispose();
        _stores[id].Dispose();

        using var log = new RaftLog(Path.Combine(_root, id, "raft"));
        Assert.Equal(2, log.LatestConfig().Config!.Count);
        Assert.DoesNotContain(log.LatestConfig().Config!, m => m.Id == victim);
    }

    [Fact]
    public async Task Messages_are_delivered_only_after_the_cluster_commits_them()
    {
        foreach (var m in _initial) Start(m.Id);
        var leader = await Leader();
        using var broker = new Broker(new RaftStore(_stores[leader.Id], leader),
            new BrokerOptions { SweepInterval = null, CommittedIndex = () => leader.CommitIndex });
        broker.CreateQueue("orders");
        await Eventually(() => leader.CommitIndex >= leader.Status().LastLogIndex);

        // Cut both followers off: the send is accepted by the leader's memory but cannot commit.
        foreach (var f in _initial.Where(m => m.Id != leader.Id)) _net.Down[f.Id] = true;
        broker.Send("orders", [new OutgoingMessage { Body = Encoding.UTF8.GetBytes("pending") }]);
        Assert.Empty(broker.GetEntity("orders").TryReceive(1, ReceiveMode.PeekLock).Messages); // not committed: not delivered

        var receive = broker.ReceiveAsync("orders", 1, TimeSpan.FromSeconds(10));
        foreach (var f in _initial.Where(m => m.Id != leader.Id)) _net.Down.TryRemove(f.Id, out _);
        await Eventually(() => leader.CommitIndex >= leader.Status().LastLogIndex);
        broker.NotifyCommitted(); // what the server does whenever the commit index moves
        var got = await receive.WaitAsync(TimeSpan.FromSeconds(5));
        Assert.Equal("pending", Encoding.UTF8.GetString(Assert.Single(got).Message.Body));
    }

    public void Dispose()
    {
        foreach (var n in _net.Nodes.Values) n.Dispose();
        foreach (var s in _stores.Values) s.Dispose();
        try { Directory.Delete(_root, true); } catch { }
    }
}
