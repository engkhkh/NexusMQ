using System.Collections.Concurrent;
using NexusMQ.Client;

namespace NexusMQ.Tests.Integration;

/// <summary>End-to-end tests: real server, real HTTP and WebSocket traffic, public client API.</summary>
public sealed class LiveServerTests : IDisposable
{
    private readonly LiveServer _server = LiveServer.Start();
    private readonly NexusClient _client;

    public LiveServerTests() => _client = _server.Client();

    public void Dispose()
    {
        _client.Dispose();
        _server.Dispose();
    }

    private static async Task<T> Within<T>(Task<T> task, int seconds = 10) => await task.WaitAsync(TimeSpan.FromSeconds(seconds));

    [Fact]
    public async Task Http_send_receive_complete()
    {
        await _client.Admin.CreateQueueAsync("orders");
        await _client.CreateSender("orders").SendMessageAsync(NexusMessage.FromJson(new { Id = 7 }));

        var receiver = _client.CreateReceiver("orders");
        var m = await receiver.ReceiveMessageAsync(TimeSpan.FromSeconds(2));
        Assert.NotNull(m);
        Assert.Equal(7, m!.BodyAs<Dictionary<string, int>>()!["id"]);
        await receiver.CompleteMessageAsync(m);

        Assert.Equal(0, (await _client.Admin.GetStatsAsync()).Single(s => s.Path == "orders").Active);
    }

    [Fact]
    public async Task WebSocket_processor_receives_pushed_messages_and_completes_them()
    {
        await _client.Admin.CreateQueueAsync("push");
        var got = new ConcurrentBag<string>();
        var done = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        await using var p = _client.CreateProcessor("push", new NexusProcessorOptions { MaxConcurrentCalls = 4 });
        p.ProcessMessageAsync += a => { got.Add(a.Message.BodyAsString); if (got.Count == 20) done.TrySetResult(); return Task.CompletedTask; };
        p.ProcessErrorAsync += _ => Task.CompletedTask;
        await p.StartProcessingAsync();

        await _client.CreateSender("push").SendMessagesAsync(Enumerable.Range(0, 20).Select(i => new NexusMessage($"m{i}")));
        await done.Task.WaitAsync(TimeSpan.FromSeconds(10));
        await p.StopProcessingAsync();

        Assert.Equal(20, got.Distinct().Count());
        var stats = (await _client.Admin.GetStatsAsync()).Single(s => s.Path == "push");
        Assert.Equal(0, stats.Active + stats.Locked);
    }

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public async Task Pushed_bodies_arrive_byte_for_byte_with_or_without_binary_bodies(bool binary)
    {
        using var client = _server.Client(options: new NexusClientOptions { BinaryBodies = binary });
        await client.Admin.CreateQueueAsync("bodies");
        var rnd = new Random(42);
        byte[] Bytes(int n) { var b = new byte[n]; rnd.NextBytes(b); return b; }
        // Empty, tiny, not valid UTF-8, larger than one WebSocket read (64 KB), and several reads.
        var bodies = new Dictionary<string, byte[]>
        {
            ["empty"] = [], ["one"] = [0xFF], ["binary"] = Bytes(300), ["big"] = Bytes(70_000), ["huge"] = Bytes(400_000)
        };
        var got = new ConcurrentDictionary<string, (byte[] Body, string Base64, string? Region)>();
        var done = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        await using var p = client.CreateProcessor("bodies", new NexusProcessorOptions { MaxConcurrentCalls = 2 });
        p.ProcessMessageAsync += a =>
        {
            var m = a.Message;
            got[m.Subject!] = (m.Body, m.BodyBase64, m.ApplicationProperties.GetValueOrDefault("region"));
            if (got.Count == bodies.Count) done.TrySetResult();
            return Task.CompletedTask;
        };
        p.ProcessErrorAsync += _ => Task.CompletedTask;
        await p.StartProcessingAsync();

        var sender = client.CreateSender("bodies");
        foreach (var (name, body) in bodies)
            await sender.SendMessageAsync(new NexusMessage(body) { Subject = name, ApplicationProperties = { ["region"] = "eu" } });
        await done.Task.WaitAsync(TimeSpan.FromSeconds(15));
        await p.StopProcessingAsync();

        foreach (var (name, body) in bodies)
        {
            Assert.Equal(body, got[name].Body);
            Assert.Equal(Convert.ToBase64String(body), got[name].Base64); // still available for code that reads base64
            Assert.Equal("eu", got[name].Region);
        }
        var stats = (await client.Admin.GetStatsAsync()).Single(s => s.Path == "bodies");
        Assert.Equal(0, stats.Active + stats.Locked); // every one was completed through the same connection
    }

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public async Task Sends_keep_bodies_properties_routing_and_schedules_with_or_without_binary_bodies(bool binary)
    {
        using var client = _server.Client(options: new NexusClientOptions { BinaryBodies = binary });
        await client.Admin.CreateQueueAsync("raw");
        await client.Admin.CreateQueueAsync("errors");
        await client.Admin.CreateExchangeAsync("logs", NexusExchangeType.Topic);
        await client.Admin.BindAsync("logs", "errors", "*.error");

        var big = new byte[300_000];
        new Random(7).NextBytes(big);
        var result = await client.CreateSender("raw").SendMessagesAsync(
        [
            new NexusMessage(big) { Subject = "big", ApplicationProperties = { ["region"] = "eu" } },
            new NexusMessage([]) { Subject = "empty" },
            new NexusMessage("héllo ✓") { Subject = "text", ContentType = "text/plain" }
        ]);
        Assert.Equal(3, result.Accepted);
        await client.CreateSender("exchanges/logs").PublishAsync("billing.error", new NexusMessage([0xFF, 0xFE]) { Subject = "routed" });
        var seq = await client.CreateSender("raw").ScheduleMessageAsync(new NexusMessage([9]) { Subject = "later" }, DateTimeOffset.UtcNow.AddHours(1));

        var receiver = client.CreateReceiver("raw");
        var got = new Dictionary<string, NexusReceivedMessage>();
        for (var i = 0; i < 3; i++)
        {
            var m = await receiver.ReceiveMessageAsync(TimeSpan.FromSeconds(5));
            Assert.NotNull(m);
            got[m!.Subject!] = m;
            await receiver.CompleteMessageAsync(m);
        }
        Assert.Equal(big, got["big"].Body);
        Assert.Equal("eu", got["big"].ApplicationProperties["region"]);
        Assert.Empty(got["empty"].Body);
        Assert.Equal("héllo ✓", got["text"].BodyAsString);
        Assert.Equal("text/plain", got["text"].ContentType);

        var routed = await client.CreateReceiver("errors").ReceiveMessageAsync(TimeSpan.FromSeconds(5));
        Assert.Equal(new byte[] { 0xFF, 0xFE }, routed!.Body);
        Assert.True(seq > 0);
        Assert.Equal(1, (await client.Admin.GetStatsAsync()).Single(s => s.Path == "raw").Scheduled);
    }

    [Fact]
    public async Task Failing_handler_dead_letters_after_max_delivery_count()
    {
        await _client.Admin.CreateQueueAsync("fragile", new NexusQueueOptions { MaxDeliveryCount = 2 });
        await using var p = _client.CreateProcessor("fragile");
        p.ProcessMessageAsync += _ => throw new InvalidOperationException("boom");
        p.ProcessErrorAsync += _ => Task.CompletedTask;
        await p.StartProcessingAsync();
        await _client.CreateSender("fragile").SendMessageAsync(new NexusMessage("poison"));

        var dlq = _client.CreateReceiver("fragile/$deadletter");
        NexusReceivedMessage? dead = null;
        for (var i = 0; i < 50 && dead is null; i++)
        {
            dead = (await dlq.PeekMessagesAsync()).FirstOrDefault();
            if (dead is null) await Task.Delay(100);
        }
        await p.StopProcessingAsync();

        Assert.NotNull(dead);
        Assert.Equal("MaxDeliveryCountExceeded", dead!.DeadLetterReason);
        Assert.Equal(1, await _client.Admin.ResubmitDeadLettersAsync("fragile"));
    }

    [Fact]
    public async Task Session_processor_keeps_order_per_session()
    {
        await _client.Admin.CreateQueueAsync("ordered", new NexusQueueOptions { RequiresSession = true });
        var sender = _client.CreateSender("ordered");
        for (var i = 0; i < 10; i++)
            await sender.SendMessagesAsync(new[] { "a", "b", "c" }.Select(s => new NexusMessage(i.ToString()) { SessionId = s }));

        var seen = new ConcurrentDictionary<string, List<int>>();
        var total = 0;
        var done = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        await using var p = _client.CreateSessionProcessor("ordered", new NexusSessionProcessorOptions
        {
            MaxConcurrentSessions = 2,
            SessionIdleTimeout = TimeSpan.FromMilliseconds(300)
        });
        p.ProcessMessageAsync += async a =>
        {
            seen.GetOrAdd(a.Session.SessionId, _ => []).Add(int.Parse(a.Message.BodyAsString));
            await a.Session.SetSessionStateAsync(a.Message.BodyAsString);
            if (Interlocked.Increment(ref total) == 30) done.TrySetResult();
        };
        p.ProcessErrorAsync += _ => Task.CompletedTask;
        await p.StartProcessingAsync();
        await done.Task.WaitAsync(TimeSpan.FromSeconds(20));
        await p.StopProcessingAsync();

        Assert.Equal(3, seen.Count);
        Assert.True(seen.Values.All(l => l.SequenceEqual(Enumerable.Range(0, 10))));
    }

    [Fact]
    public async Task Http_session_receiver_and_state()
    {
        await _client.Admin.CreateQueueAsync("sq", new NexusQueueOptions { RequiresSession = true });
        await _client.CreateSender("sq").SendMessagesAsync([new NexusMessage("1") { SessionId = "s" }, new NexusMessage("2") { SessionId = "s" }]);

        await using var session = await _client.AcceptNextSessionAsync("sq", TimeSpan.FromSeconds(2));
        Assert.NotNull(session);
        var msgs = await session!.ReceiveMessagesAsync(10, TimeSpan.FromSeconds(1));
        Assert.Equal(["1", "2"], msgs.Select(m => m.BodyAsString).ToArray());
        foreach (var m in msgs) await session.CompleteMessageAsync(m);
        await session.SetSessionStateAsync("checkpoint");
        Assert.Equal("checkpoint", await session.GetSessionStateAsStringAsync());

        var ex = await Assert.ThrowsAsync<NexusClientException>(() => _client.AcceptSessionAsync("sq", "s"));
        Assert.Equal("SessionCannotBeLocked", ex.Code);
    }

    [Fact]
    public async Task Topic_with_sql_filter_action_and_exchange_routing()
    {
        await _client.Admin.CreateTopicAsync("events");
        await _client.Admin.CreateSubscriptionAsync("events", "eu",
            [new NexusSubscriptionRule { SqlFilter = "region = 'eu'", SqlAction = "SET tagged = 'yes'" }]);
        await _client.Admin.CreateExchangeAsync("logs", NexusExchangeType.Topic);
        await _client.Admin.BindAsync("logs", "events", "*.error");

        var r = await _client.CreateSender("exchanges/logs").PublishAsync("billing.error",
            new NexusMessage("x") { ApplicationProperties = { ["region"] = "eu" } });
        await _client.CreateSender("events").SendMessageAsync(new NexusMessage("y") { ApplicationProperties = { ["region"] = "us" } });

        Assert.Equal(1, r.Delivered);
        var eu = await _client.CreateReceiver("events/subscriptions/eu").PeekMessagesAsync();
        Assert.Equal("yes", Assert.Single(eu).ApplicationProperties["tagged"]);
    }

    [Fact]
    public async Task Scheduled_message_can_be_cancelled_over_http()
    {
        await _client.Admin.CreateQueueAsync("later");
        var sender = _client.CreateSender("later");
        var seq = await sender.ScheduleMessageAsync(new NexusMessage("x"), DateTimeOffset.UtcNow.AddHours(1));
        Assert.Equal(1, (await _client.Admin.GetStatsAsync()).Single(s => s.Path == "later").Scheduled);
        await sender.CancelScheduledMessageAsync(seq);
        Assert.Equal(0, (await _client.Admin.GetStatsAsync()).Single(s => s.Path == "later").Scheduled);
    }

    [Fact]
    public async Task Metrics_endpoint_reports_counters()
    {
        await _client.Admin.CreateQueueAsync("m");
        await _client.CreateSender("m").SendMessageAsync(new NexusMessage("x"));
        var text = await _client.Admin.GetMetricsTextAsync();
        Assert.Contains("nexusmq_messages_sent_total{entity=\"m\"} 1", text);
        Assert.Contains("nexusmq_entity_active_messages{entity=\"m\",kind=\"queue\"} 1", text);
    }
}

/// <summary>Access keys over the wire: HTTP and WebSocket enforcement, runtime key management.</summary>
public sealed class LiveServerSecurityTests : IDisposable
{
    private readonly LiveServer _server = LiveServer.Start(o => o.ApiKey = "master");
    public void Dispose() => _server.Dispose();

    [Fact]
    public async Task Requests_without_a_key_are_rejected()
    {
        using var anonymous = _server.Client();
        var ex = await Assert.ThrowsAsync<NexusClientException>(() => anonymous.Admin.GetStatsAsync());
        Assert.Equal(401, ex.StatusCode);
    }

    [Fact]
    public async Task Scoped_keys_created_at_runtime_are_enforced_on_http_and_websocket()
    {
        using var master = _server.Client("master");
        await master.Admin.CreateQueueAsync("orders");
        var send = await master.Admin.CreateKeyAsync("orders-api", NexusKeyRights.Send, ["orders"]);
        var listen = await master.Admin.CreateKeyAsync("worker", NexusKeyRights.Listen, ["orders"]);

        using var sender = _server.Client(send.Secret);
        await sender.CreateSender("orders").SendMessageAsync(new NexusMessage("hi"));
        var forbidden = await Assert.ThrowsAsync<NexusClientException>(() => sender.CreateReceiver("orders").ReceiveMessageAsync(TimeSpan.Zero));
        Assert.Equal("Forbidden", forbidden.Code);

        // WebSocket: the send-only key cannot subscribe.
        var errors = new ConcurrentQueue<NexusClientException>();
        await using (var p = sender.CreateProcessor("orders"))
        {
            p.ProcessMessageAsync += _ => Task.CompletedTask;
            p.ProcessErrorAsync += e => { if (e.Exception is NexusClientException c) errors.Enqueue(c); return Task.CompletedTask; };
            await p.StartProcessingAsync();
            for (var i = 0; i < 30 && errors.IsEmpty; i++) await Task.Delay(100);
            await p.StopProcessingAsync();
        }
        Assert.Contains(errors, e => e.Code == "Forbidden");

        // The listen key gets the message over the WebSocket.
        using var worker = _server.Client(listen.Secret);
        var got = new TaskCompletionSource<string>(TaskCreationOptions.RunContinuationsAsynchronously);
        await using var wp = worker.CreateProcessor("orders");
        wp.ProcessMessageAsync += a => { got.TrySetResult(a.Message.BodyAsString); return Task.CompletedTask; };
        wp.ProcessErrorAsync += _ => Task.CompletedTask;
        await wp.StartProcessingAsync();
        Assert.Equal("hi", await got.Task.WaitAsync(TimeSpan.FromSeconds(10)));
        await wp.StopProcessingAsync();
    }

    [Fact]
    public async Task Rotation_with_grace_keeps_old_secret_until_it_ends()
    {
        using var master = _server.Client("master");
        await master.Admin.CreateQueueAsync("q");
        var original = await master.Admin.CreateKeyAsync("app", NexusKeyRights.Send, ["q"]);
        var rotated = await master.Admin.RotateKeyAsync("app", TimeSpan.FromMinutes(5));
        Assert.NotNull(rotated.Key.PreviousKeyExpiresUtc);

        using (var old = _server.Client(original.Secret)) await old.CreateSender("q").SendMessageAsync(new NexusMessage("old"));
        using (var fresh = _server.Client(rotated.Secret)) await fresh.CreateSender("q").SendMessageAsync(new NexusMessage("new"));

        var hard = await master.Admin.RotateKeyAsync("app"); // no grace: previous secret dies now
        using var stale = _server.Client(rotated.Secret);
        var ex = await Assert.ThrowsAsync<NexusClientException>(() => stale.CreateSender("q").SendMessageAsync(new NexusMessage("x")));
        Assert.Equal(401, ex.StatusCode);
        using var current = _server.Client(hard.Secret);
        await current.CreateSender("q").SendMessageAsync(new NexusMessage("ok"));
    }

    [Fact]
    public async Task Keys_cannot_escalate_or_remove_config_keys()
    {
        using var master = _server.Client("master");
        var ops = await master.Admin.CreateKeyAsync("ops", NexusKeyRights.Manage);
        using var opsClient = _server.Client(ops.Secret);

        var escalate = await Assert.ThrowsAsync<NexusClientException>(() => opsClient.Admin.CreateKeyAsync("sneaky", NexusKeyRights.Send));
        Assert.Equal("Forbidden", escalate.Code);
        var configKey = await Assert.ThrowsAsync<NexusClientException>(() => master.Admin.DeleteKeyAsync("master"));
        Assert.Equal("BadRequest", configKey.Code);
        Assert.Contains(await master.Admin.ListKeysAsync(), k => k.Name == "ops" && k.Source == "api");
    }
}

/// <summary>An open (development) broker: the first key created must be able to manage it.</summary>
public sealed class LiveServerFirstKeyTests : IDisposable
{
    private readonly LiveServer _server = LiveServer.Start();
    public void Dispose() => _server.Dispose();

    [Fact]
    public async Task First_key_must_include_manage_and_then_turns_authentication_on()
    {
        using var open = _server.Client();
        var refused = await Assert.ThrowsAsync<NexusClientException>(() => open.Admin.CreateKeyAsync("sender", NexusKeyRights.Send));
        Assert.Equal("BadRequest", refused.Code);

        var admin = await open.Admin.CreateKeyAsync("admin", NexusKeyRights.All);
        var locked = await Assert.ThrowsAsync<NexusClientException>(() => open.Admin.GetStatsAsync());
        Assert.Equal(401, locked.StatusCode);

        using var withKey = _server.Client(admin.Secret);
        await withKey.Admin.GetStatsAsync();
        var lastAdmin = await Assert.ThrowsAsync<NexusClientException>(() => withKey.Admin.DeleteKeyAsync("admin"));
        Assert.Equal("BadRequest", lastAdmin.Code);
    }
}

/// <summary>Phase 6 over the wire: rate limits, audit, single-message operations, HTTPS.</summary>
public sealed class LiveServerOperationsTests : IDisposable
{
    private readonly LiveServer _server = LiveServer.Start(o => o.ApiKey = "master");
    public void Dispose() => _server.Dispose();

    [Fact]
    public async Task Rate_limited_key_gets_429_and_the_client_retries_transparently()
    {
        using var master = _server.Client("master");
        await master.Admin.CreateQueueAsync("q");
        var key = await master.Admin.CreateKeyAsync("slow", NexusKeyRights.Send, ["q"], requestsPerSecond: 4, burst: 2);

        using var noRetry = _server.Client(key.Secret, new NexusClientOptions { MaxRateLimitRetries = 0 });
        var sender = noRetry.CreateSender("q");
        await sender.SendMessageAsync(new NexusMessage("1"));
        await sender.SendMessageAsync(new NexusMessage("2"));
        var ex = await Assert.ThrowsAsync<NexusClientException>(() => sender.SendMessageAsync(new NexusMessage("3")));
        Assert.Equal("RateLimited", ex.Code);
        Assert.Equal(429, ex.StatusCode);
        Assert.NotNull(ex.RetryAfter);

        using var retrying = _server.Client(key.Secret);
        for (var i = 0; i < 6; i++) await retrying.CreateSender("q").SendMessageAsync(new NexusMessage($"r{i}")); // paced, no failures
        Assert.Equal(8, (await master.Admin.GetStatsAsync()).Single(s => s.Path == "q").Active);
    }

    [Fact]
    public async Task Audit_records_management_actions_and_denials_without_secrets()
    {
        using var master = _server.Client("master");
        await master.Admin.CreateQueueAsync("orders");
        var app = await master.Admin.CreateKeyAsync("app", NexusKeyRights.Send, ["orders"]);
        using var appClient = _server.Client(app.Secret);
        await Assert.ThrowsAsync<NexusClientException>(() => appClient.Admin.CreateQueueAsync("hack"));

        var audit = await master.Admin.GetAuditAsync();
        Assert.Contains(audit, e => e.Action == "PUT /queues/{name}" && e.Target == "orders" && e.Result == "ok" && e.Actor == "master");
        Assert.Contains(audit, e => e.Action == "POST /keys" && e.Target == "app");
        Assert.Contains(audit, e => e.Target == "hack" && e.Result == "denied" && e.Actor == "app");
        Assert.DoesNotContain(audit, e => (e.Detail ?? "").Contains(app.Secret));
    }

    [Fact]
    public async Task Resend_and_delete_single_messages_over_http()
    {
        using var c = _server.Client("master");
        await c.Admin.CreateQueueAsync("q", new NexusQueueOptions { MaxDeliveryCount = 1 });
        await c.CreateSender("q").SendMessagesAsync([new NexusMessage("a"), new NexusMessage("b")]);
        var r = c.CreateReceiver("q");
        var first = (await r.ReceiveMessageAsync(TimeSpan.FromSeconds(1)))!;
        await r.AbandonMessageAsync(first); // MaxDeliveryCount 1 → dead-lettered

        var dead = Assert.Single(await c.CreateReceiver("q/$deadletter").PeekMessagesAsync());
        Assert.Equal(1, await c.Admin.ResendMessageAsync("q/$deadletter", dead.SequenceNumber));
        Assert.Empty(await c.CreateReceiver("q/$deadletter").PeekMessagesAsync());

        var inQueue = await c.CreateReceiver("q").PeekMessagesAsync();
        Assert.Equal(2, inQueue.Count);
        await c.Admin.DeleteMessageAsync("q", inQueue[0].SequenceNumber);
        Assert.Single(await c.CreateReceiver("q").PeekMessagesAsync());
        var gone = await Assert.ThrowsAsync<NexusClientException>(() => c.Admin.DeleteMessageAsync("q", inQueue[0].SequenceNumber));
        Assert.Equal("MessageNotFound", gone.Code);
    }
}

public sealed class LiveServerHttpsTests : IDisposable
{
    private readonly LiveServer _server = LiveServer.Start(https: true);
    public void Dispose() => _server.Dispose();

    [Fact]
    public async Task Http_and_websocket_work_over_tls()
    {
        Assert.StartsWith("https://", _server.Url);

        using var strict = _server.Client();
        await Assert.ThrowsAsync<HttpRequestException>(() => strict.Admin.GetStatsAsync()); // self-signed: not trusted

        using var dev = _server.Client(options: new NexusClientOptions { AllowUntrustedCertificates = true });
        await dev.Admin.CreateQueueAsync("tls");
        await dev.CreateSender("tls").SendMessageAsync(new NexusMessage("secret stuff"));

        var got = new TaskCompletionSource<string>(TaskCreationOptions.RunContinuationsAsynchronously);
        await using var p = dev.CreateProcessor("tls"); // WebSocket push → wss://
        p.ProcessMessageAsync += a => { got.TrySetResult(a.Message.BodyAsString); return Task.CompletedTask; };
        p.ProcessErrorAsync += _ => Task.CompletedTask;
        await p.StartProcessingAsync();
        Assert.Equal("secret stuff", await got.Task.WaitAsync(TimeSpan.FromSeconds(10)));
        await p.StopProcessingAsync();
    }
}
