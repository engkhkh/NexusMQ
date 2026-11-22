using System.Diagnostics.Metrics;
using NexusMQ.Core;

namespace NexusMQ.Tests;

public class ForwardingTests
{
    private readonly Broker _broker = new(options: new BrokerOptions { SweepInterval = null });
    private int Active(string path) => _broker.GetEntity(path).GetStats().Active;

    [Fact]
    public void Queue_auto_forwards_to_another_queue()
    {
        _broker.CreateQueue("final");
        _broker.CreateQueue("inbox", new QueueOptions { ForwardTo = "final" });

        var r = _broker.Send("inbox", [new OutgoingMessage()]);

        Assert.Equal(1, r.Delivered);
        Assert.Equal(0, Active("inbox"));
        Assert.Equal(1, Active("final"));
    }

    [Fact]
    public void Subscription_forwards_to_a_queue_with_rule_filter()
    {
        _broker.CreateQueue("eu-orders");
        _broker.CreateTopic("events");
        _broker.CreateSubscription("events", "eu", new QueueOptions { ForwardTo = "eu-orders" },
            [new SubscriptionRule { SqlFilter = "region = 'eu'" }]);

        _broker.Send("events", [new OutgoingMessage { ApplicationProperties = new() { ["region"] = "eu" } }]);
        _broker.Send("events", [new OutgoingMessage { ApplicationProperties = new() { ["region"] = "us" } }]);

        Assert.Equal(1, Active("eu-orders"));
        Assert.Equal(0, Active("events/subscriptions/eu"));
    }

    [Fact]
    public void Forwarding_chain_through_topic_and_exchange()
    {
        _broker.CreateQueue("sink");
        _broker.CreateExchange("x", ExchangeType.Fanout);
        _broker.Bind(new BindingDefinition { Exchange = "x", Destination = "sink" });
        _broker.CreateQueue("entry", new QueueOptions { ForwardTo = "exchanges/x" });

        _broker.Send("entry", [new OutgoingMessage()]);

        Assert.Equal(1, Active("sink"));
    }

    [Fact]
    public void Forwarding_loop_stops_after_max_hops_and_keeps_the_message()
    {
        _broker.CreateQueue("a");
        _broker.CreateQueue("b", new QueueOptions { ForwardTo = "a" });
        _broker.CreateQueue("a", new QueueOptions { ForwardTo = "b" }); // update creates a loop a <-> b

        _broker.Send("a", [new OutgoingMessage()]);

        Assert.Equal(1, Active("a") + Active("b"));
    }

    [Fact]
    public void Forward_to_missing_entity_is_rejected_at_create()
    {
        Assert.Equal(NexusErrorCode.EntityNotFound,
            Assert.Throws<NexusException>(() => _broker.CreateQueue("q", new QueueOptions { ForwardTo = "nope" })).Code);
        Assert.Equal(NexusErrorCode.BadRequest,
            Assert.Throws<NexusException>(() => _broker.CreateQueue("self", new QueueOptions { ForwardTo = "self" })).Code);
    }

    [Fact]
    public void Dead_letters_can_be_forwarded_to_a_shared_queue()
    {
        _broker.CreateQueue("all-failures");
        _broker.CreateQueue("q", new QueueOptions { ForwardDeadLetteredMessagesTo = "all-failures" });
        _broker.Send("q", [new OutgoingMessage()]);
        var m = _broker.GetEntity("q").TryReceive(1, ReceiveMode.PeekLock).Messages.Single();

        _broker.DeadLetter("q", m.LockToken!.Value, "Broken", null);

        Assert.Empty(_broker.Peek("q/$deadletter", 0, 10));
        var forwarded = _broker.Peek("all-failures", 0, 10).Single();
        Assert.Equal("Broken", forwarded.DeadLetterReason);
        Assert.Equal("q", forwarded.DeadLetterSource);
    }

    [Fact]
    public void Dead_letter_falls_back_to_own_dlq_when_forward_target_was_deleted()
    {
        _broker.CreateQueue("gone");
        _broker.CreateQueue("q", new QueueOptions { ForwardDeadLetteredMessagesTo = "gone" });
        _broker.DeleteQueue("gone");
        _broker.Send("q", [new OutgoingMessage()]);
        var m = _broker.GetEntity("q").TryReceive(1, ReceiveMode.PeekLock).Messages.Single();

        _broker.DeadLetter("q", m.LockToken!.Value, "Broken", null);

        Assert.Single(_broker.Peek("q/$deadletter", 0, 10));
    }
}

public class RuleActionTests
{
    private static BrokerMessage Msg() => new()
    {
        Subject = "order.created",
        ApplicationProperties = new() { ["region"] = "eu", ["debug"] = "1" }
    };

    [Fact]
    public void Set_and_remove_properties()
    {
        var m = Msg();
        SqlRuleAction.Parse("SET priority = 'high'; SET copy = region; REMOVE debug; SET n = 42").Apply(m);

        Assert.Equal("high", m.ApplicationProperties["priority"]);
        Assert.Equal("eu", m.ApplicationProperties["copy"]);
        Assert.Equal("42", m.ApplicationProperties["n"]);
        Assert.False(m.ApplicationProperties.ContainsKey("debug"));
    }

    [Fact]
    public void Set_system_property_with_concatenation_and_quotes()
    {
        var m = Msg();
        SqlRuleAction.Parse("SET sys.Subject = 'billing.' + region; SET note = 'it''s; fine'").Apply(m);

        Assert.Equal("billing.eu", m.Subject);
        Assert.Equal("it's; fine", m.ApplicationProperties["note"]);
    }

    [Theory]
    [InlineData("")]
    [InlineData("DROP TABLE x")]
    [InlineData("SET = 'x'")]
    [InlineData("SET a =")]
    [InlineData("SET sys.MessageId = 'x'")]
    [InlineData("REMOVE sys.Subject")]
    public void Invalid_actions_are_rejected(string action) =>
        Assert.Equal(NexusErrorCode.BadRequest, Assert.Throws<NexusException>(() => SqlRuleAction.Parse(action)).Code);

    [Fact]
    public void Action_changes_only_that_subscriptions_copy()
    {
        using var broker = new Broker(options: new BrokerOptions { SweepInterval = null });
        broker.CreateTopic("t");
        broker.CreateSubscription("t", "tagged", rules: [new SubscriptionRule { SqlAction = "SET tagged = 'yes'" }]);
        broker.CreateSubscription("t", "plain");

        broker.Send("t", [new OutgoingMessage()]);

        Assert.Equal("yes", broker.Peek("t/subscriptions/tagged", 0, 1).Single().ApplicationProperties["tagged"]);
        Assert.False(broker.Peek("t/subscriptions/plain", 0, 1).Single().ApplicationProperties.ContainsKey("tagged"));
    }

    [Fact]
    public void Bad_action_fails_subscription_creation()
    {
        using var broker = new Broker(options: new BrokerOptions { SweepInterval = null });
        broker.CreateTopic("t");
        Assert.Throws<NexusException>(() => broker.CreateSubscription("t", "s", rules: [new SubscriptionRule { SqlAction = "oops" }]));
    }
}

public class MetricsTests
{
    [Fact]
    public void Totals_track_message_lifecycle()
    {
        using var broker = new Broker(options: new BrokerOptions { SweepInterval = null });
        broker.CreateQueue("q", new QueueOptions { MaxDeliveryCount = 1 });
        broker.Send("q", [new OutgoingMessage(), new OutgoingMessage()]);
        var batch = broker.GetEntity("q").TryReceive(2, ReceiveMode.PeekLock).Messages;
        broker.Complete("q", batch[0].LockToken!.Value);
        broker.Abandon("q", batch[1].LockToken!.Value); // MaxDeliveryCount 1 -> dead-lettered

        var t = broker.Metrics.Totals;
        Assert.Equal(2, t[("sent", "q")]);
        Assert.Equal(2, t[("delivered", "q")]);
        Assert.Equal(1, t[("completed", "q")]);
        Assert.Equal(1, t[("abandoned", "q")]);
        Assert.Equal(1, t[("deadlettered", "q")]);
    }

    [Fact]
    public void Counters_are_published_through_System_Diagnostics_Metrics()
    {
        using var broker = new Broker(options: new BrokerOptions { SweepInterval = null });
        broker.CreateQueue("metered");
        long sent = 0;
        using var listener = new MeterListener();
        listener.InstrumentPublished = (inst, l) =>
        {
            if (inst.Meter.Name == BrokerMetrics.MeterName && inst.Name == "nexusmq.messages.sent") l.EnableMeasurementEvents(inst);
        };
        listener.SetMeasurementEventCallback<long>((_, value, tags, _) =>
        {
            foreach (var tag in tags)
                if (tag.Key == "entity" && (string?)tag.Value == "metered") Interlocked.Add(ref sent, value);
        });
        listener.Start();

        broker.Send("metered", [new OutgoingMessage(), new OutgoingMessage(), new OutgoingMessage()]);

        Assert.Equal(3, Interlocked.Read(ref sent));
    }
}

public class MessageBrowserTests
{
    private readonly Broker _broker = new(options: new BrokerOptions { SweepInterval = null });

    private long DeadLetterOne(string queue)
    {
        _broker.Send(queue, [new OutgoingMessage { Subject = "s", Body = [1] }]);
        var m = _broker.GetEntity(queue).TryReceive(1, ReceiveMode.PeekLock).Messages.Single();
        _broker.DeadLetter(queue, m.LockToken!.Value, "Bad", "broken");
        return _broker.Peek(queue + "/$deadletter", 0, 10).Single().SequenceNumber;
    }

    [Fact]
    public void Delete_single_message_by_sequence_number()
    {
        _broker.CreateQueue("q");
        var seqs = _broker.Send("q", [new OutgoingMessage(), new OutgoingMessage(), new OutgoingMessage()]).SequenceNumbers;

        _broker.DeleteMessage("q", seqs[1]);

        Assert.Equal([seqs[0], seqs[2]], _broker.Peek("q", 0, 10).Select(m => m.SequenceNumber).ToArray());
        Assert.Equal(NexusErrorCode.MessageNotFound, Assert.Throws<NexusException>(() => _broker.DeleteMessage("q", seqs[1])).Code);
    }

    [Fact]
    public void Locked_message_cannot_be_deleted()
    {
        _broker.CreateQueue("q");
        var seq = _broker.Send("q", [new OutgoingMessage()]).SequenceNumbers.Single();
        _broker.GetEntity("q").TryReceive(1, ReceiveMode.PeekLock);
        Assert.Equal(NexusErrorCode.BadRequest, Assert.Throws<NexusException>(() => _broker.DeleteMessage("q", seq)).Code);
    }

    [Fact]
    public void Resend_from_dead_letter_moves_message_back_to_its_source()
    {
        _broker.CreateQueue("q");
        var dlSeq = DeadLetterOne("q");

        Assert.Equal(1, _broker.ResendMessage("q/$deadletter", dlSeq));

        Assert.Empty(_broker.Peek("q/$deadletter", 0, 10));
        var back = _broker.Peek("q", 0, 10).Single();
        Assert.Null(back.DeadLetterReason);
        Assert.Equal("s", back.Subject);
    }

    [Fact]
    public void Resend_from_subscription_dead_letter_goes_back_to_that_subscription_only()
    {
        _broker.CreateTopic("t");
        _broker.CreateSubscription("t", "a");
        _broker.CreateSubscription("t", "b");
        _broker.Send("t", [new OutgoingMessage()]);
        var m = _broker.GetEntity("t/subscriptions/a").TryReceive(1, ReceiveMode.PeekLock).Messages.Single();
        _broker.DeadLetter("t/subscriptions/a", m.LockToken!.Value, "x", null);
        var seq = _broker.Peek("t/subscriptions/a/$deadletter", 0, 1).Single().SequenceNumber;

        _broker.ResendMessage("t/subscriptions/a/$deadletter", seq);

        Assert.Single(_broker.Peek("t/subscriptions/a", 0, 10));
        Assert.Single(_broker.Peek("t/subscriptions/b", 0, 10)); // untouched, still just the original copy
    }

    [Fact]
    public void Resend_from_queue_duplicates_by_default_and_can_target_another_entity()
    {
        _broker.CreateQueue("q");
        _broker.CreateQueue("other");
        var seq = _broker.Send("q", [new OutgoingMessage()]).SequenceNumbers.Single();

        _broker.ResendMessage("q", seq);
        Assert.Equal(2, _broker.Peek("q", 0, 10).Count);

        _broker.ResendMessage("q", seq, "other", removeOriginal: true);
        Assert.Single(_broker.Peek("q", 0, 10));
        Assert.Single(_broker.Peek("other", 0, 10));
    }

    [Fact]
    public void Move_keeps_original_when_copy_is_dropped_by_duplicate_detection()
    {
        _broker.CreateQueue("dedup", new QueueOptions { DuplicateDetectionWindow = TimeSpan.FromHours(1) });
        _broker.CreateQueue("src");
        _broker.Send("dedup", [new OutgoingMessage { MessageId = "same" }]);
        var seq = _broker.Send("src", [new OutgoingMessage { MessageId = "same" }]).SequenceNumbers.Single();

        Assert.Throws<NexusException>(() => _broker.ResendMessage("src", seq, "dedup", removeOriginal: true));
        Assert.Single(_broker.Peek("src", 0, 10));
    }
}
