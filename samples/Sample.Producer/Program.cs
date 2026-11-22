using NexusMQ.Client;

// Start NexusMQ.Server first (F5 on the server project, or "Multiple startup projects").
var url = args.FirstOrDefault() ?? "http://localhost:5680";
await using var client = new NexusClient(url);

Console.WriteLine($"Producer connected to {url}");

// 1) Azure Service Bus style: queue + topic with filtered subscriptions.
await client.Admin.CreateQueueAsync("orders", new NexusQueueOptions { MaxDeliveryCount = 3, LockDuration = TimeSpan.FromSeconds(30) });
await client.Admin.CreateTopicAsync("events");
await client.Admin.CreateSubscriptionAsync("events", "billing", [new NexusSubscriptionRule { Name = "orders", SubjectPattern = "order.#" }]);
await client.Admin.CreateSubscriptionAsync("events", "audit"); // receives everything

// 2) RabbitMQ style: topic exchange routed to a queue.
await client.Admin.CreateExchangeAsync("logs", NexusExchangeType.Topic);
await client.Admin.CreateQueueAsync("errors");
await client.Admin.BindAsync("logs", "errors", "*.error");

var orders = client.CreateSender("orders");
var events = client.CreateSender("events");
var logs = client.CreateSender("exchanges/logs");

for (var i = 1; i <= 10; i++)
{
    var order = new Order(i, $"customer-{i % 3}", 19.99m * i);
    var msg = NexusMessage.FromJson(order);
    msg.Subject = "order.created";
    msg.MessageId = $"order-{i}";
    msg.ApplicationProperties["priority"] = i % 2 == 0 ? "high" : "normal";
    await orders.SendMessageAsync(msg);
    await events.SendMessageAsync(new NexusMessage($"order {i} created") { Subject = "order.created" });
    Console.WriteLine($"sent order {i}");
}

await events.SendMessageAsync(new NexusMessage("user signed up") { Subject = "user.signup" });

// Scheduled message: delivered 10 seconds from now.
var seq = await orders.ScheduleMessageAsync(NexusMessage.FromJson(new Order(99, "late", 1m)), DateTimeOffset.UtcNow.AddSeconds(10));
Console.WriteLine($"scheduled order 99 (sequence {seq}) for +10s");

// "Poison" message: the consumer will fail it until it is dead-lettered.
await orders.SendMessageAsync(new NexusMessage("this is not json") { Subject = "order.created", MessageId = "poison-1" });

var routed = await logs.PublishAsync("billing.error", new NexusMessage("payment gateway timeout"));
await logs.PublishAsync("billing.info", new NexusMessage("this one is not routed"));
Console.WriteLine($"published log to {routed.Delivered} queue(s)");

Console.WriteLine();
foreach (var s in await client.Admin.GetStatsAsync()) Console.WriteLine(s);
Console.WriteLine("\nDashboard: " + url);

public record Order(int Id, string Customer, decimal Total);
