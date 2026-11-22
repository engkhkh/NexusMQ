using NexusMQ.Client;

var url = args.FirstOrDefault() ?? "http://localhost:5680";
await using var client = new NexusClient(url);
Console.WriteLine($"Consumer connected to {url}. Press Ctrl+C to stop.");

// Make sure the entities exist even if the producer has not run yet (create calls are idempotent).
await client.Admin.CreateQueueAsync("orders", new NexusQueueOptions { MaxDeliveryCount = 3 });
await client.Admin.CreateTopicAsync("events");
await client.Admin.CreateSubscriptionAsync("events", "billing", [new NexusSubscriptionRule { Name = "orders", SubjectPattern = "order.#" }]);

// Push-style processor (like ServiceBusProcessor): 4 handlers in parallel, auto-complete on success, auto-abandon on exception.
await using var processor = client.CreateProcessor("orders", new NexusProcessorOptions { MaxConcurrentCalls = 4 });
processor.ProcessMessageAsync += async args =>
{
    var order = args.Message.BodyAs<Order>(); // throws for the poison message -> abandoned -> dead-lettered after 3 tries
    Console.WriteLine($"[orders] #{args.Message.SequenceNumber} order {order!.Id} for {order.Customer}: {order.Total:C} (delivery {args.Message.DeliveryCount})");
    await Task.Delay(200, args.CancellationToken);
};
processor.ProcessErrorAsync += args =>
{
    Console.ForegroundColor = ConsoleColor.Yellow;
    Console.WriteLine($"[error] {args.EntityPath}: {args.Exception.Message}");
    Console.ResetColor();
    return Task.CompletedTask;
};

await using var billing = client.CreateProcessor("events", "billing");
billing.ProcessMessageAsync += args =>
{
    Console.WriteLine($"[events/billing] {args.Message.Subject}: {args.Message.BodyAsString}");
    return Task.CompletedTask;
};
billing.ProcessErrorAsync += _ => Task.CompletedTask;

await processor.StartProcessingAsync();
await billing.StartProcessingAsync();

// Pull-style receiver (like RabbitMQ basic.get / ServiceBusReceiver) for the RabbitMQ-routed "errors" queue.
var errors = client.CreateReceiver("errors", NexusReceiveMode.PeekLock);
using var cts = new CancellationTokenSource();
Console.CancelKeyPress += (_, e) => { e.Cancel = true; cts.Cancel(); };

try
{
    while (!cts.IsCancellationRequested)
    {
        var batch = await errors.ReceiveMessagesAsync(10, TimeSpan.FromSeconds(5), cts.Token);
        foreach (var m in batch)
        {
            Console.WriteLine($"[errors] {m.BodyAsString}");
            await errors.CompleteMessageAsync(m);
        }
    }
}
catch (OperationCanceledException) { }
catch (NexusClientException ex) when (ex.Code == "EntityNotFound")
{
    Console.WriteLine("Queue 'errors' does not exist yet — run Sample.Producer first.");
    await Task.Delay(Timeout.Infinite, cts.Token).ContinueWith(_ => { });
}

await processor.StopProcessingAsync();
await billing.StopProcessingAsync();

var dlq = client.CreateReceiver("orders/$deadletter");
foreach (var m in await dlq.PeekMessagesAsync())
    Console.WriteLine($"[dead-letter] {m.MessageId}: {m.DeadLetterReason} - {m.DeadLetterErrorDescription}");

public record Order(int Id, string Customer, decimal Total);
