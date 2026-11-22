using Microsoft.AspNetCore.Routing;
using NexusMQ.AspNetCore;
using NexusMQ.Core;

namespace NexusMQ.Tests;

public class AccessTests
{
    private static NexusAuthorizer Auth() => new(new NexusServerOptions
    {
        ApiKey = "master",
        AccessKeys =
        [
            new NexusAccessKey { Name = "sender", Key = "s", Rights = NexusRights.Send, Entities = ["orders", "exchanges/*"] },
            new NexusAccessKey { Name = "listener", Key = "l", Rights = NexusRights.Listen, Entities = ["events/subscriptions/billing"] },
            new NexusAccessKey { Name = "tenant", Key = "t", Rights = NexusRights.All, Entities = ["tenant-*"] },
            new NexusAccessKey { Name = "reader-all", Key = "r", Rights = NexusRights.Listen },
            new NexusAccessKey { Name = "prometheus", Key = "m", Rights = NexusRights.Monitor }
        ]
    });

    private static void Allowed(string key, NexusRights right, string? entity)
    {
        var a = Auth();
        a.Demand(a.Authenticate(key), right, entity);
    }

    private static NexusErrorCode Denied(string? key, NexusRights right, string? entity)
    {
        var a = Auth();
        return Assert.Throws<NexusException>(() => a.Demand(a.Authenticate(key), right, entity)).Code;
    }

    [Fact] public void Open_when_no_keys_configured() => new NexusAuthorizer(new NexusServerOptions()).Demand(null, NexusRights.Manage, null);
    [Fact] public void Missing_key_is_unauthorized() => Assert.Equal(NexusErrorCode.Unauthorized, Denied(null, NexusRights.Send, "orders"));
    [Fact] public void Wrong_key_is_unauthorized() => Assert.Equal(NexusErrorCode.Unauthorized, Denied("nope", NexusRights.Send, "orders"));
    [Fact] public void Master_key_can_do_everything() => Allowed("master", NexusRights.Manage, null);
    [Fact] public void Sender_can_send_to_its_queue() => Allowed("s", NexusRights.Send, "orders");
    [Fact] public void Sender_cannot_listen() => Assert.Equal(NexusErrorCode.Forbidden, Denied("s", NexusRights.Listen, "orders"));
    [Fact] public void Sender_cannot_send_elsewhere() => Assert.Equal(NexusErrorCode.Forbidden, Denied("s", NexusRights.Send, "payments"));
    [Fact] public void Wildcard_covers_exchanges() => Allowed("s", NexusRights.Send, "exchanges/logs");
    [Fact] public void Entity_pattern_covers_dead_letter_queue() => Allowed("l", NexusRights.Listen, "events/subscriptions/billing/$deadletter");
    [Fact] public void Subscription_key_does_not_cover_sibling() => Assert.Equal(NexusErrorCode.Forbidden, Denied("l", NexusRights.Listen, "events/subscriptions/audit"));
    [Fact] public void Prefix_wildcard_matches_tenant_entities() => Allowed("t", NexusRights.Manage, "tenant-acme");
    [Fact] public void Prefix_wildcard_covers_subscriptions() => Allowed("t", NexusRights.Listen, "tenant-acme/subscriptions/x");
    [Fact] public void Prefix_wildcard_does_not_match_others() => Assert.Equal(NexusErrorCode.Forbidden, Denied("t", NexusRights.Manage, "other"));
    [Fact] public void Scoped_key_cannot_do_broker_wide_manage() => Assert.Equal(NexusErrorCode.Forbidden, Denied("t", NexusRights.Manage, null));
    [Fact] public void Monitor_key_can_read_metrics() => Allowed("m", NexusRights.Monitor, null);
    [Fact] public void Monitor_key_cannot_manage() => Assert.Equal(NexusErrorCode.Forbidden, Denied("m", NexusRights.Manage, "orders"));
    [Fact] public void Monitor_key_cannot_receive() => Assert.Equal(NexusErrorCode.Forbidden, Denied("m", NexusRights.Listen, "orders"));
    [Fact] public void Manage_implies_monitor() => Allowed("master", NexusRights.Monitor, null);
    [Fact] public void Empty_entity_list_means_all_entities() => Allowed("r", NexusRights.Listen, "anything/subscriptions/y");

    [Theory]
    [InlineData("/send/{**path}", NexusRights.Send)]
    [InlineData("/receive/{**path}", NexusRights.Listen)]
    [InlineData("/settle/{**path}", NexusRights.Listen)]
    [InlineData("/sessions/accept/{**path}", NexusRights.Listen)]
    [InlineData("/peek/{**path}", NexusRights.Listen)]
    [InlineData("/messages/{**path}", NexusRights.Manage)]
    [InlineData("/resubmit/{**path}", NexusRights.Manage)]
    public void Routes_map_to_rights_on_their_path(string route, NexusRights right)
    {
        var (r, entity) = NexusAuthorizer.RequirementFor(route, new RouteValueDictionary { ["path"] = "orders" }, []);
        Assert.Equal(right, r);
        Assert.Equal("orders", entity);
    }

    [Fact]
    public void Management_routes_name_their_entity()
    {
        Assert.Equal("q1", NexusAuthorizer.RequirementFor("/queues/{name}", new RouteValueDictionary { ["name"] = "q1" }, []).Entity);
        Assert.Equal("t/subscriptions/s", NexusAuthorizer.RequirementFor("/topics/{topic}/subscriptions/{name}",
            new RouteValueDictionary { ["topic"] = "t", ["name"] = "s" }, []).Entity);
        Assert.Equal("exchanges/x", NexusAuthorizer.RequirementFor("/exchanges/{name}", new RouteValueDictionary { ["name"] = "x" }, []).Entity);
        Assert.Equal("exchanges/logs", NexusAuthorizer.RequirementFor("/bindings", new RouteValueDictionary(),
            [new BindingDefinition { Exchange = "logs", Destination = "q" }]).Entity);
        Assert.Equal((NexusRights.Monitor, (string?)null), NexusAuthorizer.RequirementFor("/stats", new RouteValueDictionary(), []));
        Assert.Equal(NexusRights.Monitor, NexusAuthorizer.RequirementFor("/metrics", new RouteValueDictionary(), []).Right);
        Assert.Equal(NexusRights.None, NexusAuthorizer.RequirementFor("/health", new RouteValueDictionary(), []).Right);
    }
}
