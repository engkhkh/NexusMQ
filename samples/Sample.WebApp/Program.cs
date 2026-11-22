using NexusMQ.AspNetCore;
using NexusMQ.Client;
using NexusMQ.Core;

// Example of using NexusMQ inside your own ASP.NET Core app.
//
// Option A (this file): the broker runs INSIDE this app (embedded). Good for a single service or a modular monolith.
// Option B: run NexusMQ.Server separately and keep only the AddNexusMQClient / AddNexusMQHandler lines here,
//           pointing "NexusMQ:Url" at the server (e.g. http://localhost:5680). Good for several services.

var builder = WebApplication.CreateBuilder(args);
var nexusUrl = builder.Configuration["NexusMQ:Url"] ?? "http://localhost:5690";

// 1) Host the broker in this app (remove for Option B).
builder.Services.AddNexusMQBroker(builder.Configuration.GetSection("NexusMQ"));

// 2) Client used to send messages (injectable NexusClient / NexusAdminClient).
builder.Services.AddNexusMQClient(nexusUrl);

// 3) Background consumers, resolved from DI per message.
builder.Services.AddNexusMQHandler<OrderPlacedHandler, OrderPlaced>("orders", o => o.MaxConcurrentCalls = 4);
builder.Services.AddNexusMQHandler<BillingHandler>("events/subscriptions/billing");
builder.Services.AddNexusMQSessionHandler<ShipmentStepHandler>("shipments");

builder.Services.AddSingleton<OrderStore>();

var app = builder.Build();

// Create the topology at startup. With the embedded broker you can call Broker directly (no HTTP).
var broker = app.Services.GetRequiredService<Broker>();
broker.CreateQueue("orders", new QueueOptions { MaxDeliveryCount = 5 });
broker.CreateQueue("shipments", new QueueOptions { RequiresSession = true });
broker.CreateTopic("events");
broker.CreateSubscription("events", "billing", rules: [new SubscriptionRule { Name = "big", SqlFilter = "total >= 100" }]);

app.MapNexusMQ("/api");                                   // broker HTTP API (needed by clients, incl. this app's own handlers)
app.MapNexusMQDashboard("/nexusmq", apiPrefix: "/api");   // dashboard at http://localhost:5690/nexusmq

// Your own endpoints just inject NexusClient and send.
app.MapPost("/orders", async (OrderPlaced order, NexusClient nexus) =>
{
    var message = NexusMessage.FromJson(order);
    message.MessageId = order.OrderId;
    await nexus.CreateSender("orders").SendMessageAsync(message);

    var evt = NexusMessage.FromJson(order);
    evt.Subject = "order.placed";
    evt.ApplicationProperties["total"] = order.Total.ToString(System.Globalization.CultureInfo.InvariantCulture);
    await nexus.CreateSender("events").SendMessageAsync(evt);

    // Three ordered steps for this order: they share a SessionId, so they are always handled in order.
    var shipments = nexus.CreateSender("shipments");
    foreach (var step in new[] { "picked", "packed", "shipped" })
        await shipments.SendMessageAsync(new NexusMessage(step) { SessionId = order.OrderId });

    return Results.Accepted($"/orders/{order.OrderId}", new { order.OrderId, status = "queued" });
});

app.MapGet("/orders", (OrderStore store) => store.All());
app.MapGet("/", () => Results.Redirect("/nexusmq"));

app.Run();

// ------------------------------------------------------------------ messages & handlers

public record OrderPlaced(string OrderId, string Customer, decimal Total);

public sealed class OrderStore
{
    private readonly System.Collections.Concurrent.ConcurrentDictionary<string, List<string>> _log = new();
    public void Add(string orderId, string entry) => _log.AddOrUpdate(orderId, [entry], (_, l) => { lock (l) l.Add(entry); return l; });
    public IDictionary<string, List<string>> All() => _log;
}

/// <summary>Typed handler: the JSON body is deserialized for you. Throwing = abandon + retry, then dead-letter.</summary>
public sealed class OrderPlacedHandler(OrderStore store, ILogger<OrderPlacedHandler> log) : INexusMessageHandler<OrderPlaced>
{
    public Task HandleAsync(OrderPlaced order, ProcessMessageEventArgs args)
    {
        log.LogInformation("Order {Id} for {Customer}: {Total}", order.OrderId, order.Customer, order.Total);
        store.Add(order.OrderId, "processed");
        return Task.CompletedTask; // auto-completed
    }
}

/// <summary>Raw handler on a topic subscription filtered with SQL (total >= 100).</summary>
public sealed class BillingHandler(OrderStore store) : INexusMessageHandler
{
    public Task HandleAsync(ProcessMessageEventArgs args)
    {
        var order = args.Message.BodyAs<OrderPlaced>()!;
        store.Add(order.OrderId, "billing: large order flagged");
        return Task.CompletedTask;
    }
}

/// <summary>Session handler: steps of the same order arrive one at a time, in order. Session state stores progress.</summary>
public sealed class ShipmentStepHandler(OrderStore store) : INexusSessionMessageHandler
{
    public async Task HandleAsync(ProcessSessionMessageEventArgs args)
    {
        var previous = await args.Session.GetSessionStateAsStringAsync(args.CancellationToken);
        var step = args.Message.BodyAsString;
        store.Add(args.Session.SessionId, $"shipment: {previous ?? "start"} -> {step}");
        await args.Session.SetSessionStateAsync(step, args.CancellationToken);
    }
}
