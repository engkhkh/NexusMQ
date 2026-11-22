using System.Text.Json;
using NexusMQ.Core;
using NexusMQ.Core.Cluster;
using NexusMQ.Core.Persistence;
using NexusMQ.Core.Replication;

namespace NexusMQ.Tests;

/// <summary>Phase 11: group commit in the Raft log, batched store flushes, and the durability rules they must keep.</summary>
public sealed class Phase11Tests : IDisposable
{
    private readonly string _root = Path.Combine(Path.GetTempPath(), "nexusmq-p11-" + Guid.NewGuid().ToString("N"));

    public void Dispose()
    {
        try { Directory.Delete(_root, true); } catch { }
    }

    private static RaftEntry Entry(long index, long term = 1) => new()
    {
        Index = index, Term = term,
        Op = new ReplicationOp { Kind = ReplicationOpKind.Remove, Entity = "q", Seq = index }
    };

    /// <summary>Indexes of the entries in a log file, read straight from disk (skipping the header record).</summary>
    private static List<long> IndexesOnDisk(string raftDir) =>
        NexusMQ.Core.Persistence.BinaryRecords.Read(Path.Combine(raftDir, "raft-log.bin")).Skip(1)
            .Select(r => JsonDocument.Parse(r.Json).RootElement.GetProperty("index").GetInt64()).ToList();

    [Fact]
    public void Raft_log_entries_reach_the_file_only_when_flushed()
    {
        var dir = Path.Combine(_root, "log");
        var log = new RaftLog(dir);
        for (var i = 1; i <= 3; i++) log.Append(Entry(i));

        Assert.Equal(3, log.LastIndex);
        Assert.Equal(0, log.DurableIndex);
        Assert.True(log.HasPending);
        Assert.Empty(IndexesOnDisk(dir));

        log.FlushNow(); // one write for all three
        Assert.Equal(3, log.DurableIndex);
        Assert.False(log.HasPending);
        Assert.Equal([1L, 2L, 3L], IndexesOnDisk(dir));

        log.Append(Entry(4));
        log.Dispose(); // shutting down flushes what is left
        using var reopened = new RaftLog(dir);
        Assert.Equal(4, reopened.LastIndex);
        Assert.Equal(4, reopened.DurableIndex);
    }

    [Fact]
    public void Flush_taken_before_a_rewrite_does_not_write_stale_entries()
    {
        var dir = Path.Combine(_root, "log");
        using (var log = new RaftLog(dir))
        {
            for (var i = 1; i <= 3; i++) log.Append(Entry(i));
            var pending = log.TakePending();  // the flusher picked up entries 1-3 …
            log.TruncateFrom(3);              // … then a conflicting entry 3 was dropped (rewrites the file)
            log.Append(Entry(3, term: 2));
            log.Flush(pending);               // the late flush must not append the old 1-3 again
            log.FlushNow();
            Assert.Equal([1L, 2L, 3L], IndexesOnDisk(dir));
        }
        using var reopened = new RaftLog(dir);
        Assert.Equal(3, reopened.LastIndex);
        Assert.Equal(2, reopened.TermAt(3));
    }

    [Fact]
    public void Store_writes_inside_a_deferred_scope_are_flushed_once_when_it_ends()
    {
        var dir = Path.Combine(_root, "store");
        using var store = new FileMessageStore(dir);
        var journal = Path.Combine(dir, "entities", "q.journal");
        long Length() => File.Exists(journal) ? new FileInfo(journal).Length : 0;

        using (store.DeferFlush())
        {
            using (store.DeferFlush()) // nested scopes: only the outermost one flushes
            {
                store.Put("q", new BrokerMessage { SequenceNumber = 1, Body = [1] });
                store.Put("q", new BrokerMessage { SequenceNumber = 2, Body = [2] });
                store.Remove("q", 1);
            }
            Assert.Equal(0, Length());
        }
        Assert.True(Length() > 0);

        using var other = new FileMessageStore(dir); // what a restart would see
        Assert.Equal([2L], other.Load("q").Select(m => m.SequenceNumber));
    }

    [Fact]
    public void Store_without_a_deferred_scope_still_writes_every_change_at_once()
    {
        var dir = Path.Combine(_root, "store");
        using var store = new FileMessageStore(dir);
        store.Put("q", new BrokerMessage { SequenceNumber = 7, Body = [7] });
        using var other = new FileMessageStore(dir);
        Assert.Single(other.Load("q"));
    }

    [Fact]
    public async Task Leader_does_not_count_itself_for_entries_it_has_not_written()
    {
        var net = new FakeNetwork();
        List<ClusterMember> members = [new() { Id = "a" }, new() { Id = "b" }, new() { Id = "c" }];
        var options = new RaftOptions { ElectionTimeout = TimeSpan.FromMilliseconds(150), HeartbeatInterval = TimeSpan.FromMilliseconds(30) };
        var nodes = new List<RaftNode>();
        var stores = new List<FileMessageStore>();
        var diskHeld = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        diskHeld.SetResult(); // open until the test closes it
        try
        {
            foreach (var m in members)
            {
                var store = new FileMessageStore(Path.Combine(_root, m.Id, "store"));
                stores.Add(store);
                var node = new RaftNode(m.Id, members, new RaftLog(Path.Combine(_root, m.Id, "raft")), store, net.For(m.Id), options);
                node.BeforeFlushForTests = () => diskHeld.Task;
                net.Nodes[m.Id] = node;
                nodes.Add(node);
                node.Start();
            }
            RaftNode? leader = null;
            for (var i = 0; i < 200 && leader is null; i++)
            {
                leader = nodes.SingleOrDefault(n => n.IsReadyLeader);
                if (leader is null) await Task.Delay(25);
            }
            Assert.NotNull(leader);

            // Only the leader and one follower remain, and the leader's disk is stuck.
            net.Down[members.First(m => m.Id != leader.Id).Id] = true;
            diskHeld = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
            var (index, term) = leader.Propose(new ReplicationOp { Kind = ReplicationOpKind.Remove, Entity = "q", Seq = 1 });

            // One follower has it, but the leader has not written it: that is not a majority yet.
            Assert.False(await leader.WaitCommittedAsync(index, term, TimeSpan.FromMilliseconds(600)));

            diskHeld.SetResult(); // the disk catches up → now it commits
            Assert.True(await leader.WaitCommittedAsync(index, term, TimeSpan.FromSeconds(5)));
        }
        finally
        {
            diskHeld.TrySetResult();
            foreach (var n in nodes) n.Dispose();
            foreach (var s in stores) s.Dispose();
        }
    }

    [Fact]
    public async Task Leader_status_reports_how_long_ago_each_peer_answered()
    {
        var net = new FakeNetwork();
        List<ClusterMember> members = [new() { Id = "a" }, new() { Id = "b" }, new() { Id = "c" }];
        var options = new RaftOptions { ElectionTimeout = TimeSpan.FromMilliseconds(150), HeartbeatInterval = TimeSpan.FromMilliseconds(30) };
        var nodes = new List<RaftNode>();
        var stores = new List<FileMessageStore>();
        try
        {
            foreach (var m in members)
            {
                var store = new FileMessageStore(Path.Combine(_root, m.Id, "store"));
                stores.Add(store);
                var node = new RaftNode(m.Id, members, new RaftLog(Path.Combine(_root, m.Id, "raft")), store, net.For(m.Id), options);
                net.Nodes[m.Id] = node;
                nodes.Add(node);
                node.Start();
            }
            RaftNode? leader = null;
            for (var i = 0; i < 200 && leader is null; i++)
            {
                leader = nodes.SingleOrDefault(n => n.IsReadyLeader);
                if (leader is null) await Task.Delay(25);
            }
            Assert.NotNull(leader);
            await Task.Delay(200);

            var peers = members.Where(m => m.Id != leader.Id).Select(m => m.Id).ToList();
            var st = leader.Status();
            Assert.Equal(peers.OrderBy(x => x), st.PeerLastContactMs.Keys.OrderBy(x => x));
            Assert.All(peers, id => Assert.InRange(st.PeerLastContactMs[id] ?? long.MaxValue, 0, 500)); // heartbeats every 30 ms

            net.Down[peers[0]] = true; // this peer stops answering; the other keeps the majority
            await Task.Delay(700);
            st = leader.Status();
            Assert.True(st.PeerLastContactMs[peers[0]] >= 500, $"silent peer last answered {st.PeerLastContactMs[peers[0]]} ms ago");
            Assert.InRange(st.PeerLastContactMs[peers[1]] ?? long.MaxValue, 0, 500);

            // Followers do not report peers (only the leader talks to everyone).
            Assert.Empty(nodes.First(n => n.Id == peers[1]).Status().PeerLastContactMs);
        }
        finally
        {
            foreach (var n in nodes) n.Dispose();
            foreach (var s in stores) s.Dispose();
        }
    }

    [Fact]
    public async Task Every_committed_entry_is_already_in_the_log_files_of_a_majority()
    {
        var net = new FakeNetwork();
        List<ClusterMember> members = [new() { Id = "a" }, new() { Id = "b" }, new() { Id = "c" }];
        var options = new RaftOptions { ElectionTimeout = TimeSpan.FromMilliseconds(150), HeartbeatInterval = TimeSpan.FromMilliseconds(30) };
        var nodes = new List<RaftNode>();
        var stores = new List<FileMessageStore>();
        try
        {
            foreach (var m in members)
            {
                var store = new FileMessageStore(Path.Combine(_root, m.Id, "store"));
                stores.Add(store);
                var node = new RaftNode(m.Id, members, new RaftLog(Path.Combine(_root, m.Id, "raft")), store, net.For(m.Id), options);
                net.Nodes[m.Id] = node;
                nodes.Add(node);
                node.Start();
            }
            RaftNode? leader = null;
            for (var i = 0; i < 200 && leader is null; i++)
            {
                leader = nodes.SingleOrDefault(n => n.IsReadyLeader);
                if (leader is null) await Task.Delay(25);
            }
            Assert.NotNull(leader);
            // One follower offline: every majority now includes the leader, so its own file must hold each commit.
            var offline = members.First(m => m.Id != leader.Id).Id;
            net.Down[offline] = true;

            // Many proposals at once, so the flusher and the replication batch them.
            var proposals = Enumerable.Range(1, 200).Select(i => leader.Propose(new ReplicationOp
            {
                Kind = ReplicationOpKind.Put, Entity = "orders", Seq = i,
                Message = new BrokerMessage { SequenceNumber = i, Body = [(byte)i] }
            })).ToList();
            var last = proposals[^1];
            Assert.True(await leader.WaitCommittedAsync(last.Index, last.Term, TimeSpan.FromSeconds(10)));

            // Right at commit time (before anything shuts down) at least two of the three files hold the entry.
            var holders = members.Count(m => IndexesOnDisk(Path.Combine(_root, m.Id, "raft")).Contains(last.Index));
            Assert.True(IndexesOnDisk(Path.Combine(_root, leader.Id, "raft")).Contains(last.Index), "leader counted an entry it had not written");
            Assert.True(holders >= 2, $"committed entry {last.Index} was on disk on only {holders} node(s)");
        }
        finally
        {
            foreach (var n in nodes) n.Dispose();
            foreach (var s in stores) s.Dispose();
        }
    }
}
