using System.Collections.Concurrent;
using NexusMQ.AspNetCore;
using NexusMQ.Client;
using NexusMQ.Core.Cluster;

namespace NexusMQ.Tests.Integration;

/// <summary>Three real servers forming a Raft cluster.</summary>
public sealed class ClusterTests : IDisposable
{
    private readonly string _root = Path.Combine(Path.GetTempPath(), "nexusmq-cluster-" + Guid.NewGuid().ToString("N"));
    private readonly Dictionary<string, int> _ports = new() { ["a"] = LiveServer.FreePort(), ["b"] = LiveServer.FreePort(), ["c"] = LiveServer.FreePort() };
    private readonly Dictionary<string, LiveServer?> _servers = new();

    public ClusterTests()
    {
        foreach (var id in _ports.Keys) Start(id);
    }

    private string Url(string id) => $"http://localhost:{_ports[id]}";
    private string AllUrls => string.Join(",", _ports.Keys.Select(Url));

    private void Start(string id) => _servers[id] = LiveServer.Start(o =>
    {
        o.ApiKey = "master";
        o.Durable = true;
        o.DataDirectory = Path.Combine(_root, id);
        o.Cluster.NodeId = id;
        o.Cluster.Secret = "cluster-secret";
        o.Cluster.ElectionTimeoutMs = 400;
        o.Cluster.HeartbeatMs = 80;
        o.Cluster.Nodes = _ports.Select(kv => new ClusterMember { Id = kv.Key, Url = Url(kv.Key) }).ToList();
        o.Cluster.Transport = _transport;
        // Kill() stands for a crash in these tests, so no hand-over on the way down unless a test asks for it.
        o.Cluster.TransferLeadershipOnShutdown = _handOverOnShutdown;
    }, port: _ports[id]);
    private string _transport = "WebSocket";
    private bool _handOverOnShutdown;

    private async Task RestartAll(string transport, bool handOver)
    {
        foreach (var id in _ports.Keys.ToList()) Kill(id);
        (_transport, _handOverOnShutdown) = (transport, handOver);
        foreach (var id in _ports.Keys.ToList()) Start(id);
        await LeaderId();
    }

    private void StartJoiner(string id, int port)
    {
        _ports[id] = port;
        _servers[id] = LiveServer.Start(o =>
        {
            o.ApiKey = "master";
            o.Durable = true;
            o.DataDirectory = Path.Combine(_root, id);
            o.Cluster.NodeId = id;
            o.Cluster.Secret = "cluster-secret";
            o.Cluster.Join = true;
            o.Cluster.ElectionTimeoutMs = 400;
            o.Cluster.HeartbeatMs = 80;
        }, port: port);
    }

    private void Kill(string id)
    {
        _servers[id]?.Dispose();
        _servers[id] = null;
    }

    private async Task<string> LeaderId(int seconds = 15)
    {
        var until = DateTime.UtcNow.AddSeconds(seconds);
        while (DateTime.UtcNow < until)
        {
            foreach (var (id, server) in _servers)
            {
                if (server is null) continue;
                var node = server.Services.GetService(typeof(NexusNode)) as NexusNode;
                if (node?.Raft?.IsReadyLeader == true) return id;
            }
            await Task.Delay(50);
        }
        throw new TimeoutException("No leader elected.");
    }

    private RaftStatus Status(string id) => ((NexusNode)_servers[id]!.Services.GetService(typeof(NexusNode))!).Raft!.Status();

    [Fact]
    public async Task Writes_are_confirmed_by_a_majority_and_followers_point_to_the_leader()
    {
        var leader = await LeaderId();
        using var client = new NexusClient(AllUrls, "master");
        await client.Admin.CreateQueueAsync("orders");
        await client.CreateSender("orders").SendMessagesAsync(Enumerable.Range(0, 20).Select(i => new NexusMessage($"m{i}")));
        Assert.Equal(Url(leader) + "/", client.CurrentEndpoint.ToString());

        // A confirmed write is on a majority: at least one follower already has it committed.
        var leaderCommit = Status(leader).CommitIndex;
        Assert.Contains(_ports.Keys.Where(id => id != leader), id => Status(id).LastLogIndex >= leaderCommit);

        var follower = _ports.Keys.First(id => id != leader);
        using var direct = new NexusClient(Url(follower), "master");
        var refused = await Assert.ThrowsAsync<NexusClientException>(() => direct.Admin.GetStatsAsync());
        Assert.Equal("Standby", refused.Code);

        using var viaFollower = new NexusClient($"{Url(follower)},{Url(leader)}", "master"); // follower first: hint leads to the leader
        Assert.Equal(20, (await viaFollower.Admin.GetStatsAsync()).Single(s => s.Path == "orders").Active);
    }

    [Fact]
    public async Task Leader_failure_elects_a_new_leader_without_losing_confirmed_messages()
    {
        var leader = await LeaderId();
        using var client = new NexusClient(AllUrls, "master");
        await client.Admin.CreateQueueAsync("ha");
        var received = new ConcurrentDictionary<string, bool>();
        await using var consumer = client.CreateProcessor("ha");
        consumer.ProcessMessageAsync += a => { received[a.Message.BodyAsString] = true; return Task.CompletedTask; };
        consumer.ProcessErrorAsync += _ => Task.CompletedTask;
        await consumer.StartProcessingAsync();

        var sender = client.CreateSender("ha");
        for (var i = 0; i < 10; i++) await sender.SendMessageAsync(new NexusMessage($"a{i}"));

        Kill(leader);
        var next = await LeaderId();
        Assert.NotEqual(leader, next);

        for (var i = 0; i < 10; i++) await sender.SendMessageAsync(new NexusMessage($"b{i}"));
        var until = DateTime.UtcNow.AddSeconds(20);
        while (received.Count < 20 && DateTime.UtcNow < until) await Task.Delay(100);
        Assert.Equal(20, received.Count);
        await consumer.StopProcessingAsync();
    }

    [Theory]
    [InlineData("socket")]
    [InlineData("http")]
    public async Task PreVote_reaches_real_servers_and_changes_nothing(string transport)
    {
        var leader = await LeaderId();
        var follower = _ports.Keys.First(id => id != leader);
        var options = new NexusClusterOptions { Secret = "cluster-secret" };
        using var socket = new WebSocketRaftTransport(options);
        using var http = new HttpRaftTransport(options);
        IRaftTransport t = transport == "socket" ? socket : http;

        var before = Status(follower);
        var probe = new VoteRequest(before.Term + 10, "intruder", long.MaxValue, long.MaxValue);
        foreach (var id in new[] { follower, leader })
        {
            var answer = await t.RequestPreVoteAsync(new ClusterMember { Id = id, Url = Url(id) }, probe, CancellationToken.None);
            Assert.NotNull(answer);
            Assert.False(answer!.Granted); // a real answer (an old server's stand-in would say yes): there is a live leader
        }
        Assert.Equal(before.Term, Status(follower).Term);
        Assert.Equal(leader, await LeaderId());
        Assert.Equal(before.Term, Status(leader).Term);
    }

    [Theory]
    [InlineData("WebSocket")]
    [InlineData("Http")]
    public async Task Leadership_is_handed_over_between_real_servers(string transport)
    {
        await RestartAll(transport, handOver: false);
        using var client = new NexusClient(AllUrls, "master");
        await client.Admin.CreateQueueAsync("handover");
        await client.CreateSender("handover").SendMessageAsync(new NexusMessage("before"));
        var leader = await LeaderId();
        var target = _ports.Keys.First(id => id != leader);
        var term = Status(leader).Term;

        var result = await client.Admin.TransferLeadershipAsync(target);

        Assert.Equal(target, result.GetProperty("leader").GetString());
        Assert.Equal(Url(target), result.GetProperty("url").GetString());
        Assert.Equal(target, await LeaderId());
        Assert.Equal(term + 1, Status(target).Term); // one election, nothing else
        Assert.Equal(1, Status(target).TransfersIn);
        await client.CreateSender("handover").SendMessageAsync(new NexusMessage("after")); // the client follows by itself
        Assert.Equal(2, (await client.Admin.GetStatsAsync()).Single(s => s.Path == "handover").Active);
        var unknown = await Assert.ThrowsAsync<NexusClientException>(() => client.Admin.TransferLeadershipAsync("nobody"));
        Assert.Contains("not a voting member", unknown.Message);
    }

    [Fact]
    public async Task A_leader_shut_down_cleanly_hands_over_first()
    {
        await RestartAll("WebSocket", handOver: true);
        var leader = await LeaderId();
        var stopping = System.Diagnostics.Stopwatch.StartNew();
        Kill(leader); // a clean stop (service stop, Ctrl+C, container stop)
        // Before 1.19 this took 30 s: the host waited for the other servers' Raft connections to close, and kept the
        // cluster without a working leader meanwhile.
        Assert.True(stopping.Elapsed < TimeSpan.FromSeconds(5), $"stopping the leader took {stopping.Elapsed.TotalSeconds:0.0} s");
        var next = await LeaderId();
        Assert.NotEqual(leader, next);
        Assert.Equal(1, Status(next).TransfersIn); // handed over, not elected after a timeout
    }

    [Fact]
    public async Task A_single_surviving_node_refuses_writes()
    {
        var leader = await LeaderId();
        var term = Status(leader).Term;
        foreach (var id in _ports.Keys.Where(id => id != leader).Take(1).Append(leader).ToList()) Kill(id);
        var survivor = _servers.Single(kv => kv.Value is not null).Key;
        await Task.Delay(1500); // let it notice
        Assert.True(Status(survivor).PreVoteRounds > 0);
        Assert.Equal(term, Status(survivor).Term); // PreVote: alone, it never raises its term

        using var c = new NexusClient(Url(survivor), "master");
        var ex = await Assert.ThrowsAsync<NexusClientException>(() => c.Admin.CreateQueueAsync("x"));
        Assert.Equal("Standby", ex.Code);
        Assert.NotEqual(RaftRole.Leader, Status(survivor).Role);
    }

    [Fact]
    public async Task Restarted_node_catches_up()
    {
        var leader = await LeaderId();
        var follower = _ports.Keys.First(id => id != leader);
        Kill(follower);

        using var client = new NexusClient(AllUrls, "master");
        await client.Admin.CreateQueueAsync("q");
        await client.CreateSender("q").SendMessagesAsync(Enumerable.Range(0, 30).Select(i => new NexusMessage($"{i}")));
        var target = Status(leader).CommitIndex;

        Start(follower);
        var until = DateTime.UtcNow.AddSeconds(15);
        while (Status(follower).LastApplied < target && DateTime.UtcNow < until) await Task.Delay(100);
        Assert.True(Status(follower).LastApplied >= target);
    }

    [Fact]
    public async Task Messages_sent_one_at_a_time_are_each_delivered_promptly()
    {
        // Regression: with committed-only delivery, a commit that happened while receivers were being woken for an
        // earlier one was missed, and the message waited for the next commit (or a 20 s receive timeout).
        await LeaderId();
        using var client = new NexusClient(AllUrls, "master");
        await client.Admin.CreateQueueAsync("one-by-one");
        TaskCompletionSource<string>? waiting = null;
        await using var p = client.CreateProcessor("one-by-one");
        p.ProcessMessageAsync += a => { waiting?.TrySetResult(a.Message.BodyAsString); return Task.CompletedTask; };
        p.ProcessErrorAsync += _ => Task.CompletedTask;
        await p.StartProcessingAsync();
        await Task.Delay(300);

        var sender = client.CreateSender("one-by-one");
        var slowest = TimeSpan.Zero;
        for (var i = 0; i < 40; i++)
        {
            waiting = new TaskCompletionSource<string>(TaskCreationOptions.RunContinuationsAsynchronously);
            var sw = System.Diagnostics.Stopwatch.StartNew();
            await sender.SendMessageAsync(new NexusMessage($"m{i}"));
            Assert.Equal($"m{i}", await waiting.Task.WaitAsync(TimeSpan.FromSeconds(5)));
            if (sw.Elapsed > slowest) slowest = sw.Elapsed;
        }
        await p.StopProcessingAsync();
        Assert.True(slowest < TimeSpan.FromSeconds(2), $"slowest delivery took {slowest.TotalMilliseconds:0} ms");
    }

    [Fact]
    public async Task Servers_can_be_added_and_removed_at_runtime_over_http()
    {
        await LeaderId();
        using var client = new NexusClient(AllUrls, "master");
        // Duplicate detection: a send retried across a leader change (at-least-once) must not leave a second copy, so the
        // exact counts below hold even on a slow machine where an election happens right after start-up.
        await client.Admin.CreateQueueAsync("orders", new NexusQueueOptions { DuplicateDetectionWindow = TimeSpan.FromMinutes(10) });
        await client.CreateSender("orders").SendMessagesAsync(Enumerable.Range(0, 10).Select(i => new NexusMessage($"m{i}")));

        // Add a fourth server (started in join mode), then retire one of the original three.
        StartJoiner("d", LiveServer.FreePort());
        var members = await client.Admin.AddClusterMemberAsync("d", Url("d"));
        Assert.Equal(4, members.GetArrayLength());
        var retire = _ports.Keys.First(id => id != "d");
        await client.Admin.RemoveClusterMemberAsync(retire);
        Kill(retire);

        // The cluster is now three servers including d; it keeps working, and d has all the data.
        using var rest = new NexusClient(string.Join(",", _ports.Keys.Where(id => id != retire).Select(Url)), "master");
        await rest.CreateSender("orders").SendMessageAsync(new NexusMessage("after-change"));
        Assert.Equal(11, (await rest.Admin.GetStatsAsync()).Single(s => s.Path == "orders").Active);
        var until = DateTime.UtcNow.AddSeconds(10);
        var commit = (await LeaderId()) is var l ? Status(l).CommitIndex : 0;
        while (Status("d").LastApplied < commit && DateTime.UtcNow < until) await Task.Delay(50);
        Assert.True(Status("d").LastApplied >= commit);
        Assert.Equal(3, Status("d").Members.Count);
    }

    public void Dispose()
    {
        foreach (var s in _servers.Values) s?.Dispose();
        try { Directory.Delete(_root, true); } catch { }
    }
}
