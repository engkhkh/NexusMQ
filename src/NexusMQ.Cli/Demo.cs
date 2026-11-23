using System.Text.Json;
using NexusMQ.Client;

namespace NexusMQ.Cli;

/// <summary>
/// <c>nexusmq demo</c>: fills a broker with a small, realistic workload so the dashboard has something to show.
/// <list type="bullet">
/// <item>demo-orders (queue): orders are sent and processed; "poison" orders fail and end up in demo-orders/$deadletter.</item>
/// <item>demo-events (topic): each order is published; subscription "billing" (SQL filter amount &gt; 500) and "shipping" (all,
/// processed slowly so a backlog builds up).</item>
/// <item>demo-logs (topic exchange): "*.error" is routed to the demo-errors queue, which nobody reads, so it can be browsed.</item>
/// <item>One message scheduled two minutes ahead.</item>
/// </list>
/// </summary>
internal static class Demo
{
    private static readonly string[] Customers =
        ["أحمد علي", "Sara Smith", "محمد حسن", "Lina Ibrahim", "فاطمة يوسف", "John Carter", "خالد عمر", "Mona Adel"];
    private static readonly string[] Regions = ["riyadh", "cairo", "dubai", "london"];
    /// <summary>Readable JSON: Arabic (and other non-ASCII) text as is, not as \uXXXX escapes.</summary>
    private static readonly JsonSerializerOptions Readable = new() { Encoder = System.Text.Encodings.Web.JavaScriptEncoder.UnsafeRelaxedJsonEscaping };

    public static async Task<int> RunAsync(NexusClient client, TimeSpan duration, CancellationToken ct)
    {
        var admin = client.Admin;
        Console.WriteLine("Creating the demo entities (demo-orders, demo-events + billing/shipping, demo-logs -> demo-errors)…");
        await admin.CreateQueueAsync("demo-orders", new NexusQueueOptions { MaxDeliveryCount = 3 }, ct);
        await admin.CreateTopicAsync("demo-events", ct: ct);
        await admin.CreateSubscriptionAsync("demo-events", "billing", [new NexusSubscriptionRule { SqlFilter = "amount > 500" }], ct: ct);
        await admin.CreateSubscriptionAsync("demo-events", "shipping", ct: ct);
        await admin.CreateQueueAsync("demo-errors", ct: ct);
        await admin.CreateExchangeAsync("demo-logs", NexusExchangeType.Topic, ct);
        await admin.BindAsync("demo-logs", "demo-errors", "*.error", ct: ct);
        await client.CreateSender("demo-orders").ScheduleMessageAsync(
            new NexusMessage("{\"note\":\"scheduled two minutes ahead\"}") { Subject = "reminder", ContentType = "application/json" },
            DateTimeOffset.UtcNow.AddMinutes(2), ct);

        long sent = 0, processed = 0, failedAttempts = 0, billing = 0, shipping = 0, errors = 0;
        await using var orders = client.CreateProcessor("demo-orders", new NexusProcessorOptions { MaxConcurrentCalls = 4 });
        orders.ProcessMessageAsync += async a =>
        {
            if (a.Message.Subject == "reminder") { Interlocked.Increment(ref processed); return; }
            await Task.Delay(Random.Shared.Next(20, 80), a.CancellationToken); // "work"
            if (a.Message.ApplicationProperties.GetValueOrDefault("poison") == "true")
            {
                Interlocked.Increment(ref failedAttempts);
                throw new InvalidOperationException("payment gateway rejected the card"); // retried, then dead-lettered
            }
            Interlocked.Increment(ref processed);
        };
        orders.ProcessErrorAsync += _ => Task.CompletedTask;

        await using var billingProcessor = client.CreateProcessor("demo-events/subscriptions/billing");
        billingProcessor.ProcessMessageAsync += _ => { Interlocked.Increment(ref billing); return Task.CompletedTask; };
        billingProcessor.ProcessErrorAsync += _ => Task.CompletedTask;

        await using var shippingProcessor = client.CreateProcessor("demo-events/subscriptions/shipping",
            new NexusProcessorOptions { MaxConcurrentCalls = 1, PrefetchCount = 1 });
        shippingProcessor.ProcessMessageAsync += async a =>
        {
            await Task.Delay(600, a.CancellationToken); // slower than orders arrive: a backlog builds up
            Interlocked.Increment(ref shipping);
        };
        shippingProcessor.ProcessErrorAsync += _ => Task.CompletedTask;

        await orders.StartProcessingAsync(ct);
        await billingProcessor.StartProcessingAsync(ct);
        await shippingProcessor.StartProcessingAsync(ct);

        var orderSender = client.CreateSender("demo-orders");
        var eventSender = client.CreateSender("demo-events");
        var logSender = client.CreateSender("exchanges/demo-logs");
        var until = DateTimeOffset.UtcNow + duration;
        var nextReport = DateTimeOffset.UtcNow;
        Console.WriteLine($"Running for {duration.TotalMinutes:0.#} min (Ctrl+C stops). Open the dashboard and watch the numbers move.");
        try
        {
            for (var i = 1; DateTimeOffset.UtcNow < until && !ct.IsCancellationRequested; i++)
            {
                var amount = Random.Shared.Next(20, 1500);
                var region = Regions[i % Regions.Length];
                var poison = i % 17 == 0;
                var order = JsonSerializer.Serialize(new
                {
                    orderId = $"ORD-{i:D5}", customer = Customers[i % Customers.Length], amount, region,
                    items = Random.Shared.Next(1, 6), createdUtc = DateTimeOffset.UtcNow
                }, Readable);
                await orderSender.SendMessageAsync(new NexusMessage(order)
                {
                    MessageId = $"ORD-{i:D5}", Subject = "order.created", ContentType = "application/json",
                    ApplicationProperties = { ["amount"] = amount.ToString(), ["region"] = region, ["poison"] = poison ? "true" : "false" }
                }, ct);
                await eventSender.SendMessageAsync(new NexusMessage(order)
                {
                    Subject = "order.created", ContentType = "application/json",
                    ApplicationProperties = { ["amount"] = amount.ToString(), ["region"] = region }
                }, ct);
                sent++;
                if (i % 9 == 0)
                {
                    await logSender.PublishAsync(i % 2 == 0 ? "payments.error" : "inventory.error",
                        new NexusMessage($"{{\"orderId\":\"ORD-{i:D5}\",\"error\":\"timeout talking to the {(i % 2 == 0 ? "payment" : "inventory")} service\"}}")
                        { ContentType = "application/json" }, ct);
                    errors++;
                }
                if (DateTimeOffset.UtcNow >= nextReport)
                {
                    Console.WriteLine($"  sent {sent,5} orders | processed {Interlocked.Read(ref processed),5} | failed attempts {Interlocked.Read(ref failedAttempts),3} " +
                                      $"| billing {Interlocked.Read(ref billing),4} | shipping {Interlocked.Read(ref shipping),4} | errors logged {errors,3}");
                    nextReport = DateTimeOffset.UtcNow.AddSeconds(3);
                }
                await Task.Delay(Random.Shared.Next(150, 350), ct);
            }
        }
        catch (OperationCanceledException) { }

        await orders.StopProcessingAsync();
        await billingProcessor.StopProcessingAsync();
        await shippingProcessor.StopProcessingAsync();
        Console.WriteLine($"Done: {sent} orders sent, {Interlocked.Read(ref processed)} processed. Left for you to look at in the dashboard:");
        Console.WriteLine("  demo-orders/$deadletter (poison orders), demo-errors (routed error logs), shipping backlog, a scheduled reminder.");
        Console.WriteLine("Remove it all with: nexusmq delete queue demo-orders / demo-errors, delete topic demo-events, delete exchange demo-logs");
        return 0;
    }
}
