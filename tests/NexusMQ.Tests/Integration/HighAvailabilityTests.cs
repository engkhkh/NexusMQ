using System.Collections.Concurrent;
using NexusMQ.AspNetCore;
using NexusMQ.Client;

namespace NexusMQ.Tests.Integration;

/// <summary>Primary + standby pairs: replication, promotion, client failover.</summary>
public sealed class HighAvailabilityTests : IDisposable
{
    private readonly string _standbyDir = Path.Combine(Path.GetTempPath(), "nexusmq-ha-" + Guid.NewGuid().ToString("N"));
    private readonly LiveServer _primary = LiveServer.Start(o => o.ApiKey = "master");
    private LiveServer? _standby;

    private LiveServer StartStandby(bool autoFailover = false) => _standby = LiveServer.Start(o =>
    {
        o.ApiKey = "master";
        o.Durable = true;
        o.DataDirectory = _standbyDir;
        o.Replication.Role = NexusRole.Standby;
        o.Replication.PrimaryUrl = _primary.Url;
        o.Replication.PrimaryKey = "master";
        o.Replication.AutoFailover = autoFailover;
        o.Replication.FailoverAfterSeconds = 1;
    });

    private static async Task Eventually(Func<Task<bool>> condition, int seconds = 15)
    {
        var until = DateTime.UtcNow.AddSeconds(seconds);
        while (DateTime.UtcNow < until)
        {
            try { if (await condition()) return; } catch (NexusClientException) { } catch (HttpRequestException) { }
            await Task.Delay(100);
        }
        throw new TimeoutException("Condition not met in time.");
    }

    private static async Task<string> Role(NexusClient c) => (await c.Admin.GetReplicationStatusAsync()).GetProperty("role").GetString()!;

    [Fact]
    public async Task Standby_follows_primary_refuses_clients_and_serves_everything_after_promotion()
    {
        using var p = _primary.Client("master");
        await p.Admin.CreateQueueAsync("orders", new NexusQueueOptions { MaxDeliveryCount = 1 });
        await p.CreateSender("orders").SendMessagesAsync([new NexusMessage("before-standby-1"), new NexusMessage("before-standby-2")]);
        var app = await p.Admin.CreateKeyAsync("app", NexusKeyRights.Send | NexusKeyRights.Listen, ["orders"]);

        var standby = StartStandby();
        using var s = standby.Client("master");
        await Eventually(async () => (await s.Admin.GetReplicationStatusAsync()).GetProperty("connectedToPrimary").GetBoolean());

        // Changes after the snapshot stream through.
        await p.CreateSender("orders").SendMessageAsync(new NexusMessage("after-standby"));
        var r = p.CreateReceiver("orders");
        var first = (await r.ReceiveMessageAsync(TimeSpan.FromSeconds(1)))!;
        await r.AbandonMessageAsync(first); // dead-lettered
        await Eventually(async () =>
            (await s.Admin.GetReplicationStatusAsync()).GetProperty("position").GetInt64() ==
            (await p.Admin.GetReplicationStatusAsync()).GetProperty("position").GetInt64());

        var refused = await Assert.ThrowsAsync<NexusClientException>(() => s.Admin.GetStatsAsync());
        Assert.Equal("Standby", refused.Code);
        Assert.Equal(503, refused.StatusCode);

        await s.Admin.GetReplicationStatusAsync(); // status works on a standby
        using (var http = new HttpClient()) // promote
        {
            http.DefaultRequestHeaders.Add("X-Api-Key", "master");
            (await http.PostAsync(standby.Url + "/api/replication/promote", null)).EnsureSuccessStatusCode();
        }
        Assert.Equal("Primary", await Role(s));

        Assert.Equal(["before-standby-2", "after-standby"], (await s.CreateReceiver("orders").PeekMessagesAsync()).Select(m => m.BodyAsString).ToArray());
        Assert.Equal("before-standby-1", Assert.Single(await s.CreateReceiver("orders/$deadletter").PeekMessagesAsync()).BodyAsString);

        using var appOnStandby = standby.Client(app.Secret); // runtime key was replicated
        await appOnStandby.CreateSender("orders").SendMessageAsync(new NexusMessage("hello new primary"));
    }

    [Fact]
    public async Task Client_with_both_urls_skips_the_standby()
    {
        var standby = StartStandby();
        using var both = new NexusClient($"{standby.Url},{_primary.Url}", "master"); // standby listed first on purpose
        await both.Admin.CreateQueueAsync("q");
        Assert.Equal(_primary.Url + "/", both.CurrentEndpoint.ToString());

        var got = new TaskCompletionSource<string>(TaskCreationOptions.RunContinuationsAsynchronously);
        await using var proc = new NexusClient($"{standby.Url},{_primary.Url}", "master").CreateProcessor("q"); // push connection too
        proc.ProcessMessageAsync += a => { got.TrySetResult(a.Message.BodyAsString); return Task.CompletedTask; };
        proc.ProcessErrorAsync += _ => Task.CompletedTask;
        await proc.StartProcessingAsync();
        await both.CreateSender("q").SendMessageAsync(new NexusMessage("routed"));
        Assert.Equal("routed", await got.Task.WaitAsync(TimeSpan.FromSeconds(10)));
        await proc.StopProcessingAsync();
    }

    [Fact]
    public async Task Automatic_failover_keeps_producers_and_consumers_going()
    {
        using (var p = _primary.Client("master")) await p.Admin.CreateQueueAsync("ha");
        var standby = StartStandby(autoFailover: true);
        using (var s = standby.Client("master"))
            await Eventually(async () => (await s.Admin.GetReplicationStatusAsync()).GetProperty("connectedToPrimary").GetBoolean());

        using var client = new NexusClient($"{_primary.Url},{standby.Url}", "master");
        var received = new ConcurrentDictionary<string, bool>();
        await using var consumer = client.CreateProcessor("ha");
        consumer.ProcessMessageAsync += a => { received[a.Message.BodyAsString] = true; return Task.CompletedTask; };
        consumer.ProcessErrorAsync += _ => Task.CompletedTask;
        await consumer.StartProcessingAsync();

        var sender = client.CreateSender("ha");
        for (var i = 0; i < 10; i++) await sender.SendMessageAsync(new NexusMessage($"a{i}"));
        await Eventually(() => Task.FromResult(received.Count == 10));
        await Eventually(async () =>
        {
            using var s = standby.Client("master");
            using var p = _primary.Client("master");
            return (await s.Admin.GetReplicationStatusAsync()).GetProperty("position").GetInt64() ==
                   (await p.Admin.GetReplicationStatusAsync()).GetProperty("position").GetInt64();
        });

        await _primary.DisposeAsync(); // the primary goes away

        for (var i = 0; i < 10; i++) await sender.SendMessageAsync(new NexusMessage($"b{i}")); // waits through the failover
        await Eventually(() => Task.FromResult(received.Count == 20), seconds: 30);
        Assert.Equal(standby.Url + "/", client.CurrentEndpoint.ToString());
        await consumer.StopProcessingAsync();
    }

    public void Dispose()
    {
        _standby?.Dispose();
        try { _primary.Dispose(); } catch (ObjectDisposedException) { }
        try { Directory.Delete(_standbyDir, true); } catch { }
    }
}
