using System.Text;
using NexusMQ.Core;

namespace NexusMQ.Tests;

public class RoutingTests
{
    private readonly Broker _broker = new(options: new BrokerOptions { SweepInterval = null });

    private int Active(string path) => _broker.GetEntity(path).GetStats().Active;

    [Theory]
    [InlineData("a.b.c", "a.b.c", true)]
    [InlineData("a.*.c", "a.b.c", true)]
    [InlineData("a.*", "a.b.c", false)]
    [InlineData("a.#", "a.b.c", true)]
    [InlineData("a.#", "a", true)]
    [InlineData("#", "anything.at.all", true)]
    [InlineData("#.error", "billing.error", true)]
    [InlineData("#.error", "billing.info", false)]
    [InlineData("*.error", "a.b.error", false)]
    [InlineData("a.#.z", "a.b.c.z", true)]
    [InlineData("a.#.z", "a.z", true)]
    public void Topic_pattern_matching(string pattern, string key, bool expected) =>
        Assert.Equal(expected, Routing.TopicMatches(pattern, key));

    [Fact]
    public void Topic_subscriptions_receive_copies_filtered_by_rules()
    {
        _broker.CreateTopic("events");
        _broker.CreateSubscription("events", "all");
        _broker.CreateSubscription("events", "orders", rules: [new SubscriptionRule { Name = "o", SubjectPattern = "order.#" }]);
        _broker.CreateSubscription("events", "eu", rules: [new SubscriptionRule { Name = "eu", Properties = new() { ["region"] = "eu" } }]);

        var r1 = _broker.Send("events", [new OutgoingMessage { Subject = "order.created" }]);
        var r2 = _broker.Send("events", [new OutgoingMessage { Subject = "user.signup", ApplicationProperties = new() { ["region"] = "eu" } }]);

        Assert.Equal(2, r1.Delivered);
        Assert.Equal(2, r2.Delivered);
        Assert.Equal(2, Active("events/subscriptions/all"));
        Assert.Equal(1, Active("events/subscriptions/orders"));
        Assert.Equal(1, Active("events/subscriptions/eu"));
    }

    [Fact]
    public void Subscriptions_are_independent()
    {
        _broker.CreateTopic("t");
        _broker.CreateSubscription("t", "a");
        _broker.CreateSubscription("t", "b");
        _broker.Send("t", [new OutgoingMessage { Body = [1] }]);

        var a = _broker.GetEntity("t/subscriptions/a").TryReceive(1, ReceiveMode.PeekLock).Messages.Single();
        _broker.Complete("t/subscriptions/a", a.LockToken!.Value);

        Assert.Equal(0, Active("t/subscriptions/a"));
        Assert.Equal(1, Active("t/subscriptions/b"));
    }

    [Fact]
    public void Direct_exchange_routes_by_exact_key()
    {
        _broker.CreateExchange("direct", ExchangeType.Direct);
        _broker.CreateQueue("info");
        _broker.CreateQueue("error");
        _broker.Bind(new BindingDefinition { Exchange = "direct", Destination = "info", RoutingKey = "info" });
        _broker.Bind(new BindingDefinition { Exchange = "direct", Destination = "error", RoutingKey = "error" });

        _broker.Send("exchanges/direct", [new OutgoingMessage()], "error");
        _broker.Send("exchanges/direct", [new OutgoingMessage { Subject = "info" }]); // Subject is the default routing key
        var unrouted = _broker.Send("exchanges/direct", [new OutgoingMessage()], "debug");

        Assert.Equal(1, Active("info"));
        Assert.Equal(1, Active("error"));
        Assert.Equal(0, unrouted.Delivered);
    }

    [Fact]
    public void Fanout_exchange_routes_to_all_bindings()
    {
        _broker.CreateExchange("fan", ExchangeType.Fanout);
        _broker.CreateQueue("a");
        _broker.CreateQueue("b");
        _broker.Bind(new BindingDefinition { Exchange = "fan", Destination = "a" });
        _broker.Bind(new BindingDefinition { Exchange = "fan", Destination = "b" });

        var r = _broker.Send("exchanges/fan", [new OutgoingMessage()], "ignored");

        Assert.Equal(2, r.Delivered);
    }

    [Fact]
    public void Topic_exchange_routes_by_pattern()
    {
        _broker.CreateExchange("logs", ExchangeType.Topic);
        _broker.CreateQueue("errors");
        _broker.CreateQueue("billing");
        _broker.Bind(new BindingDefinition { Exchange = "logs", Destination = "errors", RoutingKey = "*.error" });
        _broker.Bind(new BindingDefinition { Exchange = "logs", Destination = "billing", RoutingKey = "billing.#" });

        _broker.Send("exchanges/logs", [new OutgoingMessage()], "billing.error");
        _broker.Send("exchanges/logs", [new OutgoingMessage()], "billing.info");
        _broker.Send("exchanges/logs", [new OutgoingMessage()], "auth.error");

        Assert.Equal(2, Active("errors"));
        Assert.Equal(2, Active("billing"));
    }

    [Fact]
    public void Headers_exchange_matches_all_or_any()
    {
        _broker.CreateExchange("h", ExchangeType.Headers);
        _broker.CreateQueue("all");
        _broker.CreateQueue("any");
        _broker.Bind(new BindingDefinition { Exchange = "h", Destination = "all", Arguments = new() { ["format"] = "pdf", ["type"] = "report" } });
        _broker.Bind(new BindingDefinition { Exchange = "h", Destination = "any", RoutingKey = "k2", Arguments = new() { ["x-match"] = "any", ["format"] = "pdf", ["type"] = "report" } });

        _broker.Send("exchanges/h", [new OutgoingMessage { ApplicationProperties = new() { ["format"] = "pdf" } }]);
        _broker.Send("exchanges/h", [new OutgoingMessage { ApplicationProperties = new() { ["format"] = "pdf", ["type"] = "report" } }]);

        Assert.Equal(1, Active("all"));
        Assert.Equal(2, Active("any"));
    }

    [Fact]
    public void Exchange_can_route_to_a_topic_and_to_another_exchange()
    {
        _broker.CreateExchange("front", ExchangeType.Fanout);
        _broker.CreateExchange("back", ExchangeType.Fanout);
        _broker.CreateTopic("t");
        _broker.CreateSubscription("t", "s");
        _broker.CreateQueue("q");
        _broker.Bind(new BindingDefinition { Exchange = "front", Destination = "exchanges/back" });
        _broker.Bind(new BindingDefinition { Exchange = "back", Destination = "t" });
        _broker.Bind(new BindingDefinition { Exchange = "back", Destination = "q" });
        _broker.Bind(new BindingDefinition { Exchange = "back", Destination = "exchanges/front" }); // cycle is ignored

        var r = _broker.Send("exchanges/front", [new OutgoingMessage { Body = Encoding.UTF8.GetBytes("hi") }]);

        Assert.Equal(2, r.Delivered);
        Assert.Equal(1, Active("t/subscriptions/s"));
        Assert.Equal(1, Active("q"));
    }

    [Fact]
    public void Deleting_a_queue_removes_its_bindings()
    {
        _broker.CreateExchange("x", ExchangeType.Fanout);
        _broker.CreateQueue("q");
        _broker.Bind(new BindingDefinition { Exchange = "x", Destination = "q" });

        _broker.DeleteQueue("q");

        Assert.Empty(_broker.GetTopology().Bindings);
    }

    [Fact]
    public void Queue_and_topic_names_share_a_namespace()
    {
        _broker.CreateQueue("name");
        Assert.Equal(NexusErrorCode.EntityAlreadyExists, Assert.Throws<NexusException>(() => _broker.CreateTopic("name")).Code);
    }

    [Theory]
    [InlineData("")]
    [InlineData("has space")]
    [InlineData("a/b")]
    [InlineData("$system")]
    public void Invalid_names_are_rejected(string name) =>
        Assert.Equal(NexusErrorCode.BadRequest, Assert.Throws<NexusException>(() => _broker.CreateQueue(name)).Code);
}
