using NexusMQ.Core;

namespace NexusMQ.Tests;

public class SqlFilterTests
{
    private static readonly BrokerMessage Order = new()
    {
        MessageId = "m-1",
        Subject = "order.created",
        CorrelationId = "corr-9",
        ContentType = "application/json",
        ApplicationProperties = new()
        {
            ["region"] = "eu",
            ["amount"] = "150.5",
            ["priority"] = "2",
            ["vip"] = "true",
            ["customer name"] = "O'Brien"
        }
    };

    [Theory]
    [InlineData("region = 'eu'", true)]
    [InlineData("region = 'us'", false)]
    [InlineData("region <> 'us'", true)]
    [InlineData("region != 'eu'", false)]
    [InlineData("amount > 100", true)]
    [InlineData("amount >= 150.5", true)]
    [InlineData("amount < 100", false)]
    [InlineData("priority = 2", true)]
    [InlineData("priority = 2.0", true)]
    [InlineData("amount > 100 AND region = 'eu'", true)]
    [InlineData("amount > 1000 OR region = 'eu'", true)]
    [InlineData("amount > 1000 OR region = 'us'", false)]
    [InlineData("NOT region = 'us'", true)]
    [InlineData("(region = 'us' OR region = 'eu') AND priority > 1", true)]
    [InlineData("sys.Subject LIKE 'order.%'", true)]
    [InlineData("sys.Label LIKE 'order._reated'", true)]
    [InlineData("sys.Subject NOT LIKE 'user%'", true)]
    [InlineData("sys.CorrelationId = 'corr-9'", true)]
    [InlineData("sys.ContentType = 'application/json'", true)]
    [InlineData("region IN ('us', 'eu', 'apac')", true)]
    [InlineData("region NOT IN ('us', 'apac')", true)]
    [InlineData("priority IN (1, 3)", false)]
    [InlineData("missing IS NULL", true)]
    [InlineData("region IS NOT NULL", true)]
    [InlineData("EXISTS(region)", true)]
    [InlineData("EXISTS(missing)", false)]
    [InlineData("missing = 'x'", false)]
    [InlineData("NOT missing = 'x'", false)]   // UNKNOWN stays UNKNOWN under NOT
    [InlineData("missing = 'x' OR region = 'eu'", true)]
    [InlineData("vip = TRUE", true)]
    [InlineData("vip", true)]
    [InlineData("user.region = 'eu'", true)]
    [InlineData("[customer name] = 'O''Brien'", true)]
    [InlineData("TRUE", true)]
    [InlineData("FALSE", false)]
    [InlineData("amount > -5", true)]
    [InlineData("region = 'eu' and amount > 100 or 1 = 0", true)]
    public void Evaluates(string sql, bool expected) => Assert.Equal(expected, SqlFilter.Parse(sql).Matches(Order));

    [Theory]
    [InlineData("")]
    [InlineData("region =")]
    [InlineData("region = 'eu")]
    [InlineData("(region = 'eu'")]
    [InlineData("region LIKE 5")]
    [InlineData("sys.Nope = 1")]
    [InlineData("region = 'eu' garbage")]
    public void Rejects_invalid_syntax(string sql) =>
        Assert.Equal(NexusErrorCode.BadRequest, Assert.Throws<NexusException>(() => SqlFilter.Parse(sql)).Code);

    [Fact]
    public void Subscriptions_use_sql_filters()
    {
        using var broker = new Broker(options: new BrokerOptions { SweepInterval = null });
        broker.CreateTopic("orders");
        broker.CreateSubscription("orders", "big-eu", rules: [new SubscriptionRule { Name = "big", SqlFilter = "region = 'eu' AND amount > 100" }]);

        broker.Send("orders", [new OutgoingMessage { ApplicationProperties = new() { ["region"] = "eu", ["amount"] = "500" } }]);
        broker.Send("orders", [new OutgoingMessage { ApplicationProperties = new() { ["region"] = "eu", ["amount"] = "5" } }]);
        broker.Send("orders", [new OutgoingMessage { ApplicationProperties = new() { ["region"] = "us", ["amount"] = "500" } }]);

        Assert.Equal(1, broker.GetEntity("orders/subscriptions/big-eu").GetStats().Active);
    }

    [Fact]
    public void Creating_a_subscription_with_a_bad_filter_fails_fast()
    {
        using var broker = new Broker(options: new BrokerOptions { SweepInterval = null });
        broker.CreateTopic("t");
        Assert.Throws<NexusException>(() => broker.CreateSubscription("t", "s", rules: [new SubscriptionRule { SqlFilter = "a = = 1" }]));
    }
}
