using NexusMQ.AspNetCore;
using NexusMQ.Core;

namespace NexusMQ.Tests;

public class RateLimiterTests
{
    [Fact]
    public void Burst_then_refill_at_the_configured_rate()
    {
        var time = new ManualTimeProvider();
        var limiter = new NexusRateLimiter(time);
        for (var i = 0; i < 3; i++) Assert.True(limiter.TryAcquire("k", 2, 3, out _));
        Assert.False(limiter.TryAcquire("k", 2, 3, out var wait));
        Assert.True(wait > TimeSpan.Zero && wait <= TimeSpan.FromSeconds(0.5));

        time.Advance(TimeSpan.FromSeconds(0.5)); // 2/s → one token back
        Assert.True(limiter.TryAcquire("k", 2, 3, out _));
        Assert.False(limiter.TryAcquire("k", 2, 3, out _));
    }

    [Fact]
    public void Keys_have_separate_buckets()
    {
        var limiter = new NexusRateLimiter(new ManualTimeProvider());
        Assert.True(limiter.TryAcquire("a", 1, 1, out _));
        Assert.False(limiter.TryAcquire("a", 1, 1, out _));
        Assert.True(limiter.TryAcquire("b", 1, 1, out _));
    }

    [Fact]
    public void Authorizer_limits_only_keys_with_a_rate()
    {
        var time = new ManualTimeProvider();
        var auth = new NexusAuthorizer(new NexusServerOptions
        {
            ApiKey = "master",
            AccessKeys = [new NexusAccessKey { Name = "slow", Key = "s", Rights = NexusRights.Send, RequestsPerSecond = 1, Burst = 2 }]
        }, time: time);

        var slow = auth.Authenticate("s");
        auth.CheckRate(slow);
        auth.CheckRate(slow);
        var ex = Assert.Throws<NexusException>(() => auth.CheckRate(slow));
        Assert.Equal(NexusErrorCode.RateLimited, ex.Code);
        Assert.NotNull(ex.RetryAfter);

        var master = auth.Authenticate("master");
        for (var i = 0; i < 100; i++) auth.CheckRate(master); // unlimited

        time.Advance(TimeSpan.FromSeconds(1));
        auth.CheckRate(slow);
    }

    [Fact]
    public void Runtime_limits_can_be_set_and_removed()
    {
        var auth = new NexusAuthorizer(new NexusServerOptions { ApiKey = "master" }, time: new ManualTimeProvider());
        var admin = auth.Authenticate("master");
        var (_, secret) = auth.CreateKey(admin, "app", NexusRights.Send, null, requestsPerSecond: 1, burst: 1);
        var app = auth.Authenticate(secret);
        auth.CheckRate(app);
        Assert.Throws<NexusException>(() => auth.CheckRate(auth.Authenticate(secret)));

        auth.SetLimits(admin, "app", null, null);
        for (var i = 0; i < 10; i++) auth.CheckRate(auth.Authenticate(secret));
        Assert.Throws<NexusException>(() => auth.SetLimits(admin, "app", -1, null));
    }
}

public sealed class AuditLogTests : IDisposable
{
    private readonly string _dir = Path.Combine(Path.GetTempPath(), "nexusmq-audit-" + Guid.NewGuid().ToString("N"));

    [Fact]
    public void Records_newest_first_with_filters()
    {
        var log = new NexusAuditLog();
        log.Record("admin", "PUT /queues/{name}", "orders", "ok");
        log.Record("app", "PUT /queues/{name}", "hack", "denied", "no Manage");
        log.Record("admin", "DELETE /queues/{name}", "orders", "ok");

        var all = log.Recent();
        Assert.Equal("DELETE /queues/{name}", all[0].Action);
        Assert.Single(log.Recent(result: "denied"));
        Assert.Equal(2, log.Recent(actor: "admin").Count);
        Assert.Single(log.Recent(max: 1));
    }

    [Fact]
    public void Writes_json_lines_to_file()
    {
        var file = Path.Combine(_dir, "audit.log");
        var log = new NexusAuditLog(file);
        log.Record("admin", "POST /keys", "orders-api", "ok", "rights=Send");
        log.Record("admin", "DELETE /keys/{name}", "orders-api", "ok");

        var lines = File.ReadAllLines(file);
        Assert.Equal(2, lines.Length);
        Assert.Contains("\"target\":\"orders-api\"", lines[0]);
    }

    public void Dispose()
    {
        try { Directory.Delete(_dir, true); } catch { }
    }
}
