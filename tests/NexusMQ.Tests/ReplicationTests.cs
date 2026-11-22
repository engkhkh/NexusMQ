using System.Text;
using NexusMQ.Core;
using NexusMQ.Core.Persistence;
using NexusMQ.Core.Replication;

namespace NexusMQ.Tests;

public sealed class ReplicationTests : IDisposable
{
    private readonly string _dir = Path.Combine(Path.GetTempPath(), "nexusmq-repl-" + Guid.NewGuid().ToString("N"));
    private static readonly BrokerOptions NoTimer = new() { SweepInterval = null };

    private static string Body(BrokerMessage m) => Encoding.UTF8.GetString(m.Body);

    private static void Traffic(Broker b)
    {
        b.CreateQueue("orders", new QueueOptions { MaxDeliveryCount = 1 });
        b.CreateQueue("ship", new QueueOptions { RequiresSession = true });
        b.CreateTopic("events");
        b.CreateSubscription("events", "audit");
        b.Send("orders", [new OutgoingMessage { Body = "a"u8.ToArray() }, new OutgoingMessage { Body = "b"u8.ToArray() }, new OutgoingMessage { Body = "c"u8.ToArray() }]);
        var batch = b.GetEntity("orders").TryReceive(2, ReceiveMode.PeekLock).Messages;
        b.Complete("orders", batch[0].LockToken!.Value);   // a gone
        b.Abandon("orders", batch[1].LockToken!.Value);    // b dead-lettered (MaxDeliveryCount 1)
        b.Send("events", [new OutgoingMessage { Body = "e"u8.ToArray() }]);
        b.Send("ship", [new OutgoingMessage { SessionId = "s1", Body = "p"u8.ToArray() }]);
        var s = b.GetEntity("ship").TryAcceptSession("s1").Session!;
        b.SetSessionState("ship", "s1", s.SessionLockToken, "step-1"u8.ToArray());
    }

    private static void AssertSameState(Broker standby)
    {
        Assert.Equal(["c"], standby.Peek("orders", 0, 10).Select(Body).ToArray());
        Assert.Equal(["b"], standby.Peek("orders/$deadletter", 0, 10).Select(Body).ToArray());
        Assert.Equal(["e"], standby.Peek("events/subscriptions/audit", 0, 10).Select(Body).ToArray());
        var session = standby.GetEntity("ship").TryAcceptSession("s1").Session!;
        Assert.Equal("step-1", Encoding.UTF8.GetString(standby.GetSessionState("ship", "s1", session.SessionLockToken)!));
        Assert.True(standby.GetTopology().Queues.Single(q => q.Name == "ship").Options.RequiresSession);
    }

    [Fact]
    public void Replaying_the_log_from_the_start_reproduces_the_state()
    {
        var log = new ReplicationLog();
        using var primary = new Broker(new ReplicatingMessageStore(new InMemoryMessageStore(), log), NoTimer);
        Traffic(primary);

        using (var standbyStore = new FileMessageStore(_dir))
            foreach (var op in log.ReadAfter(0, int.MaxValue, out _)!) ReplicationApplier.Apply(standbyStore, op);

        using var promoted = new Broker(new FileMessageStore(_dir), NoTimer);
        AssertSameState(promoted);
    }

    [Fact]
    public void Snapshot_plus_later_ops_reproduces_the_state()
    {
        var log = new ReplicationLog();
        using var primary = new Broker(new ReplicatingMessageStore(new InMemoryMessageStore(), log), NoTimer);
        primary.CreateQueue("orders", new QueueOptions { MaxDeliveryCount = 1 });
        primary.Send("orders", [new OutgoingMessage { Body = "old"u8.ToArray() }]);

        var position = log.Position;
        var snapshot = primary.ExportState(position);
        primary.DeleteMessage("orders", primary.Peek("orders", 0, 1).Single().SequenceNumber);
        Traffic(primary); // CreateQueue("orders") again is an idempotent update

        using (var standbyStore = new FileMessageStore(_dir))
        {
            ReplicationApplier.ApplySnapshot(standbyStore, snapshot);
            foreach (var op in log.ReadAfter(position, int.MaxValue, out _)!) ReplicationApplier.Apply(standbyStore, op);
        }

        using var promoted = new Broker(new FileMessageStore(_dir), NoTimer);
        AssertSameState(promoted);
    }

    [Fact]
    public void Standby_too_far_behind_must_resync_from_a_snapshot()
    {
        var log = new ReplicationLog(capacity: 16);
        using var primary = new Broker(new ReplicatingMessageStore(new InMemoryMessageStore(), log), NoTimer);
        primary.CreateQueue("q");
        for (var i = 0; i < 40; i++) primary.Send("q", [new OutgoingMessage()]);

        Assert.Null(log.ReadAfter(0, 100, out _));
        Assert.Equal(5, log.ReadAfter(log.Position - 5, 100, out _)!.Count);
    }

    [Fact]
    public void Snapshot_apply_clears_stale_entities()
    {
        using (var stale = new Broker(new FileMessageStore(_dir), NoTimer))
        {
            stale.CreateQueue("old-queue");
            stale.Send("old-queue", [new OutgoingMessage()]);
        }
        var log = new ReplicationLog();
        using var primary = new Broker(new ReplicatingMessageStore(new InMemoryMessageStore(), log), NoTimer);
        primary.CreateQueue("fresh");

        using (var store = new FileMessageStore(_dir)) ReplicationApplier.ApplySnapshot(store, primary.ExportState(log.Position));

        using var promoted = new Broker(new FileMessageStore(_dir), NoTimer);
        Assert.Equal(["fresh"], promoted.GetTopology().Queues.Select(q => q.Name).ToArray());
    }

    public void Dispose()
    {
        try { Directory.Delete(_dir, true); } catch { }
    }
}
