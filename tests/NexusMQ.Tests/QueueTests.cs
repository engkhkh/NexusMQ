using System.Text;
using NexusMQ.Core;

namespace NexusMQ.Tests;

public class QueueTests
{
    private readonly ManualTimeProvider _time = new();
    private readonly Broker _broker;

    public QueueTests()
    {
        _broker = new Broker(options: new BrokerOptions { SweepInterval = null }, time: _time);
    }

    private static OutgoingMessage Msg(string body, string? id = null) => new() { Body = Encoding.UTF8.GetBytes(body), MessageId = id };

    private static string Text(ReceivedMessage r) => Encoding.UTF8.GetString(r.Message.Body);

    [Fact]
    public void Send_receive_complete_is_fifo()
    {
        _broker.CreateQueue("orders");
        _broker.Send("orders", [Msg("a"), Msg("b"), Msg("c")]);

        var (batch, _) = _broker.GetEntity("orders").TryReceive(10, ReceiveMode.PeekLock);

        Assert.Equal(["a", "b", "c"], batch.Select(Text).ToArray());
        foreach (var m in batch) _broker.Complete("orders", m.LockToken!.Value);
        Assert.Equal(0, _broker.GetEntity("orders").GetStats().Active);
        Assert.Equal(0, _broker.GetEntity("orders").GetStats().Locked);
    }

    [Fact]
    public void ReceiveAndDelete_removes_immediately()
    {
        _broker.CreateQueue("q");
        _broker.Send("q", [Msg("x")]);

        var (batch, _) = _broker.GetEntity("q").TryReceive(1, ReceiveMode.ReceiveAndDelete);

        Assert.Single(batch);
        Assert.Null(batch[0].LockToken);
        Assert.Empty(_broker.Peek("q", 0, 10));
    }

    [Fact]
    public void Abandon_redelivers_then_dead_letters_after_max_delivery_count()
    {
        _broker.CreateQueue("q", new QueueOptions { MaxDeliveryCount = 3 });
        _broker.Send("q", [Msg("poison")]);
        var entity = _broker.GetEntity("q");

        for (var attempt = 1; attempt <= 3; attempt++)
        {
            var (batch, _) = entity.TryReceive(1, ReceiveMode.PeekLock);
            Assert.Single(batch);
            Assert.Equal(attempt, batch[0].Message.DeliveryCount);
            entity.Abandon(batch[0].LockToken!.Value);
        }

        Assert.Empty(entity.TryReceive(1, ReceiveMode.PeekLock).Messages);
        var dead = _broker.Peek("q/$deadletter", 0, 10);
        Assert.Single(dead);
        Assert.Equal("MaxDeliveryCountExceeded", dead[0].DeadLetterReason);
        Assert.Equal("q", dead[0].DeadLetterSource);
    }

    [Fact]
    public void Expired_lock_returns_message_and_old_token_is_rejected()
    {
        _broker.CreateQueue("q", new QueueOptions { LockDuration = TimeSpan.FromSeconds(10) });
        _broker.Send("q", [Msg("x")]);
        var entity = _broker.GetEntity("q");
        var first = entity.TryReceive(1, ReceiveMode.PeekLock).Messages.Single();

        _time.Advance(TimeSpan.FromSeconds(11));
        var ex = Assert.Throws<NexusException>(() => entity.Complete(first.LockToken!.Value));
        Assert.Equal(NexusErrorCode.MessageLockLost, ex.Code);

        _broker.Sweep();
        var second = entity.TryReceive(1, ReceiveMode.PeekLock).Messages.Single();
        Assert.Equal(2, second.Message.DeliveryCount);
        entity.Complete(second.LockToken!.Value);
    }

    [Fact]
    public void Renew_lock_extends_lock()
    {
        _broker.CreateQueue("q", new QueueOptions { LockDuration = TimeSpan.FromSeconds(10) });
        _broker.Send("q", [Msg("x")]);
        var entity = _broker.GetEntity("q");
        var m = entity.TryReceive(1, ReceiveMode.PeekLock).Messages.Single();

        _time.Advance(TimeSpan.FromSeconds(8));
        entity.RenewLock(m.LockToken!.Value);
        _time.Advance(TimeSpan.FromSeconds(8));
        entity.Complete(m.LockToken!.Value); // would throw without the renewal
    }

    [Fact]
    public void Ttl_expired_messages_go_to_dead_letter()
    {
        _broker.CreateQueue("q", new QueueOptions { DefaultTimeToLive = TimeSpan.FromMinutes(1) });
        _broker.Send("q", [Msg("old")]);

        _time.Advance(TimeSpan.FromMinutes(2));
        _broker.Sweep();

        Assert.Empty(_broker.GetEntity("q").TryReceive(1, ReceiveMode.PeekLock).Messages);
        var dead = _broker.Peek("q/$deadletter", 0, 10).Single();
        Assert.Equal("TTLExpiredException", dead.DeadLetterReason);
    }

    [Fact]
    public void Scheduled_message_is_invisible_until_due()
    {
        _broker.CreateQueue("q");
        _broker.Send("q", [new OutgoingMessage { Body = [1], ScheduledEnqueueTimeUtc = _time.GetUtcNow().AddMinutes(5) }]);
        var entity = _broker.GetEntity("q");

        Assert.Empty(entity.TryReceive(1, ReceiveMode.PeekLock).Messages);
        Assert.Equal(1, entity.GetStats().Scheduled);

        _time.Advance(TimeSpan.FromMinutes(5));
        _broker.Sweep();

        Assert.Single(entity.TryReceive(1, ReceiveMode.PeekLock).Messages);
    }

    [Fact]
    public void Scheduled_message_can_be_cancelled()
    {
        _broker.CreateQueue("q");
        var seq = _broker.Send("q", [new OutgoingMessage { ScheduledEnqueueTimeUtc = _time.GetUtcNow().AddMinutes(5) }]).SequenceNumbers.Single();

        Assert.True(_broker.CancelScheduled("q", seq));
        _time.Advance(TimeSpan.FromMinutes(10));
        _broker.Sweep();
        Assert.Empty(_broker.GetEntity("q").TryReceive(1, ReceiveMode.PeekLock).Messages);
    }

    [Fact]
    public void Duplicate_detection_drops_repeated_message_ids_within_window()
    {
        _broker.CreateQueue("q", new QueueOptions { DuplicateDetectionWindow = TimeSpan.FromMinutes(10) });

        _broker.Send("q", [Msg("1", "same"), Msg("2", "same")]);
        Assert.Equal(1, _broker.GetEntity("q").GetStats().Active);

        _time.Advance(TimeSpan.FromMinutes(11));
        _broker.Sweep();
        _broker.Send("q", [Msg("3", "same")]);
        Assert.Equal(2, _broker.GetEntity("q").GetStats().Active);
    }

    [Fact]
    public void Deferred_message_is_only_receivable_by_sequence_number()
    {
        _broker.CreateQueue("q");
        _broker.Send("q", [Msg("later")]);
        var entity = _broker.GetEntity("q");
        var m = entity.TryReceive(1, ReceiveMode.PeekLock).Messages.Single();

        var seq = entity.Defer(m.LockToken!.Value);

        Assert.Empty(entity.TryReceive(1, ReceiveMode.PeekLock).Messages);
        var deferred = entity.ReceiveDeferred([seq]).Single();
        Assert.Equal("later", Text(deferred));
        entity.Complete(deferred.LockToken!.Value);
    }

    [Fact]
    public void Explicit_dead_letter_and_resubmit()
    {
        _broker.CreateQueue("q");
        _broker.Send("q", [Msg("bad")]);
        var m = _broker.GetEntity("q").TryReceive(1, ReceiveMode.PeekLock).Messages.Single();

        _broker.DeadLetter("q", m.LockToken!.Value, "ValidationFailed", "missing customer");
        var dead = _broker.Peek("q/$deadletter", 0, 10).Single();
        Assert.Equal("ValidationFailed", dead.DeadLetterReason);
        Assert.Equal("missing customer", dead.DeadLetterErrorDescription);

        Assert.Equal(1, _broker.ResubmitDeadLetters("q"));
        Assert.Empty(_broker.Peek("q/$deadletter", 0, 10));
        Assert.Equal("bad", Text(_broker.GetEntity("q").TryReceive(1, ReceiveMode.PeekLock).Messages.Single()));
    }

    [Fact]
    public void Quota_is_enforced()
    {
        _broker.CreateQueue("q", new QueueOptions { MaxSizeInMessages = 2 });
        _broker.Send("q", [Msg("1"), Msg("2")]);
        var ex = Assert.Throws<NexusException>(() => _broker.Send("q", [Msg("3")]));
        Assert.Equal(NexusErrorCode.QuotaExceeded, ex.Code);
    }

    [Fact]
    public void Unknown_entity_throws_not_found()
    {
        var ex = Assert.Throws<NexusException>(() => _broker.Send("missing", [Msg("x")]));
        Assert.Equal(NexusErrorCode.EntityNotFound, ex.Code);
    }

    [Fact]
    public async Task Long_poll_receive_wakes_up_when_a_message_arrives()
    {
        using var broker = new Broker(options: new BrokerOptions { SweepInterval = null });
        broker.CreateQueue("q");

        var receive = broker.ReceiveAsync("q", 1, TimeSpan.FromSeconds(10));
        await Task.Delay(100);
        Assert.False(receive.IsCompleted);

        broker.Send("q", [Msg("ping")]);
        var result = await receive.WaitAsync(TimeSpan.FromSeconds(5));
        Assert.Equal("ping", Text(result.Single()));
    }

    [Fact]
    public async Task Long_poll_receive_times_out_empty()
    {
        using var broker = new Broker(options: new BrokerOptions { SweepInterval = null });
        broker.CreateQueue("q");
        var result = await broker.ReceiveAsync("q", 1, TimeSpan.FromMilliseconds(200));
        Assert.Empty(result);
    }
}
