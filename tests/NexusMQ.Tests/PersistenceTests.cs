using System.Text;
using NexusMQ.Core;
using NexusMQ.Core.Persistence;

namespace NexusMQ.Tests;

public sealed class PersistenceTests : IDisposable
{
    private readonly string _dir = Path.Combine(Path.GetTempPath(), "nexusmq-tests-" + Guid.NewGuid().ToString("N"));
    private static readonly BrokerOptions NoTimer = new() { SweepInterval = null };

    private Broker Open(int compactionThreshold = 10_000) => new(new FileMessageStore(_dir, compactionThreshold: compactionThreshold), NoTimer);

    [Fact]
    public void Topology_and_messages_survive_restart()
    {
        using (var broker = Open())
        {
            broker.CreateQueue("orders", new QueueOptions { MaxDeliveryCount = 7 });
            broker.CreateTopic("events");
            broker.CreateSubscription("events", "audit");
            broker.CreateExchange("logs", ExchangeType.Topic);
            broker.Bind(new BindingDefinition { Exchange = "logs", Destination = "orders", RoutingKey = "#" });

            broker.Send("orders", [new OutgoingMessage { Body = Encoding.UTF8.GetBytes("one") }, new OutgoingMessage { Body = Encoding.UTF8.GetBytes("two") }]);
            broker.Send("events", [new OutgoingMessage { Body = Encoding.UTF8.GetBytes("evt") }]);

            var first = broker.GetEntity("orders").TryReceive(1, ReceiveMode.PeekLock).Messages.Single();
            broker.Complete("orders", first.LockToken!.Value);
        }

        using (var broker = Open())
        {
            var topo = broker.GetTopology();
            Assert.Equal(7, topo.Queues.Single().Options.MaxDeliveryCount);
            Assert.Single(topo.Bindings);

            var remaining = broker.GetEntity("orders").TryReceive(10, ReceiveMode.PeekLock).Messages;
            Assert.Equal("two", Encoding.UTF8.GetString(remaining.Single().Message.Body));
            Assert.Single(broker.Peek("events/subscriptions/audit", 0, 10));

            // New sequence numbers continue after the restored ones.
            var seq = broker.Send("orders", [new OutgoingMessage()]).SequenceNumbers.Single();
            Assert.True(seq > remaining.Single().Message.SequenceNumber);
        }
    }

    [Fact]
    public void Locked_messages_become_available_again_after_restart()
    {
        using (var broker = Open())
        {
            broker.CreateQueue("q");
            broker.Send("q", [new OutgoingMessage()]);
            Assert.Single(broker.GetEntity("q").TryReceive(1, ReceiveMode.PeekLock).Messages);
        }

        using (var broker = Open())
            Assert.Single(broker.GetEntity("q").TryReceive(1, ReceiveMode.PeekLock).Messages);
    }

    [Fact]
    public void Dead_letters_and_deferred_state_survive_restart()
    {
        long deferredSeq;
        using (var broker = Open())
        {
            broker.CreateQueue("q");
            broker.Send("q", [new OutgoingMessage(), new OutgoingMessage()]);
            var batch = broker.GetEntity("q").TryReceive(2, ReceiveMode.PeekLock).Messages;
            broker.DeadLetter("q", batch[0].LockToken!.Value, "Bad", null);
            deferredSeq = broker.Defer("q", batch[1].LockToken!.Value);
        }

        using (var broker = Open())
        {
            Assert.Equal("Bad", broker.Peek("q/$deadletter", 0, 10).Single().DeadLetterReason);
            Assert.Empty(broker.GetEntity("q").TryReceive(1, ReceiveMode.PeekLock).Messages);
            Assert.Single(broker.ReceiveDeferred("q", [deferredSeq]));
        }
    }

    [Fact]
    public void Journal_is_compacted()
    {
        using (var broker = Open(compactionThreshold: 50))
        {
            broker.CreateQueue("q");
            for (var i = 0; i < 100; i++)
            {
                broker.Send("q", [new OutgoingMessage()]);
                var m = broker.GetEntity("q").TryReceive(1, ReceiveMode.PeekLock).Messages.Single();
                broker.Complete("q", m.LockToken!.Value);
            }
            broker.Send("q", [new OutgoingMessage { Body = [42] }]);
            broker.Sweep();
        }

        var journal = Directory.GetFiles(Path.Combine(_dir, "entities"), "q.journal").Single();
        Assert.Single(NexusMQ.Core.Persistence.BinaryRecords.Read(journal)); // compacted down to the one live message

        using (var broker = Open())
            Assert.Equal(42, broker.GetEntity("q").TryReceive(1, ReceiveMode.PeekLock).Messages.Single().Message.Body[0]);
    }

    [Fact]
    public void Deleting_a_queue_deletes_its_journal()
    {
        using var broker = Open();
        broker.CreateQueue("q");
        broker.Send("q", [new OutgoingMessage()]);
        broker.DeleteQueue("q");
        Assert.Empty(Directory.GetFiles(Path.Combine(_dir, "entities"), "q.journal"));
    }

    public void Dispose()
    {
        try { Directory.Delete(_dir, recursive: true); } catch { /* best effort */ }
    }
}
