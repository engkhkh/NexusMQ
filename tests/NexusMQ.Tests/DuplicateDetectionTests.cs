using NexusMQ.Client;
using NexusMQ.Core;
using NexusMQ.Core.Persistence;

namespace NexusMQ.Tests;

/// <summary>
/// Duplicate detection across restarts and cluster leader changes. Its history used to live only in memory, so a server that
/// restored a queue from disk (a restart, or a new cluster leader) forgot it, and a client retrying a send after a failover got a
/// second copy. Found by an intermittent Windows CI failure (a batch of 10 delivered twice after an election).
/// </summary>
public sealed class DuplicateDetectionTests : IDisposable
{
    private readonly string _dir = Path.Combine(Path.GetTempPath(), "nexusmq-dedup-" + Guid.NewGuid().ToString("N"));

    public void Dispose()
    {
        try { Directory.Delete(_dir, true); } catch { }
    }

    private static OutgoingMessage Msg(string id) => new() { MessageId = id, Body = [1] };

    private long Active(Broker b) => b.GetStats().Single(s => s.Path == "orders").Active;

    [Fact]
    public void A_resent_message_is_still_recognised_after_the_queue_is_restored_from_disk()
    {
        var time = new ManualTimeProvider();
        using (var store = new FileMessageStore(_dir))
        {
            var broker = new Broker(store, time: time);
            broker.CreateQueue("orders", new QueueOptions { DuplicateDetectionWindow = TimeSpan.FromMinutes(10) });
            broker.Send("orders", [Msg("order-1"), Msg("order-2")]);
        }

        time.Advance(TimeSpan.FromMinutes(1));
        using (var store = new FileMessageStore(_dir)) // a restart, or a new leader building its broker from the store
        {
            var broker = new Broker(store, time: time);
            var again = broker.Send("orders", [Msg("order-1"), Msg("order-3")]);
            Assert.Equal(1, again.Delivered); // order-1 is a duplicate (not stored), order-3 is new
            Assert.Equal(3, Active(broker));
        }
    }

    [Fact]
    public void Ids_older_than_the_window_are_not_remembered_after_a_restore()
    {
        var time = new ManualTimeProvider();
        using (var store = new FileMessageStore(_dir))
        {
            var broker = new Broker(store, time: time);
            broker.CreateQueue("orders", new QueueOptions { DuplicateDetectionWindow = TimeSpan.FromMinutes(10) });
            broker.Send("orders", [Msg("old")]);
        }

        time.Advance(TimeSpan.FromMinutes(11));
        using (var store = new FileMessageStore(_dir))
        {
            var broker = new Broker(store, time: time);
            Assert.Equal(1, broker.Send("orders", [Msg("old")]).Delivered); // outside the window: a new message, as before a restart
        }
    }

    [Fact]
    public void Client_messages_get_an_id_when_created_so_a_retried_send_carries_the_same_one()
    {
        var a = new NexusMessage("x");
        var b = new NexusMessage("x");
        Assert.False(string.IsNullOrEmpty(a.MessageId));
        Assert.NotEqual(a.MessageId, b.MessageId);
        Assert.Equal(a.MessageId, a.MessageId); // stable for the life of the object, i.e. across retries
    }
}
