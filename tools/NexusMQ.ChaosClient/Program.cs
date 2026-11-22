using System.Collections.Concurrent;
using NexusMQ.Client;

// Steady traffic through one or more servers while you break things (kill the leader, fail over, change membership).
// Every message the server CONFIRMED must be delivered; the summary counts any that were lost or delivered twice.
//
//   nexusmq-chaos <urls> <key> <seconds> [queue] [--interval-ms 20]
//
// Exit code 0 = nothing lost, nothing duplicated; 1 = a confirmed message was LOST (or nothing got through);
// 3 = nothing lost but some messages arrived twice (allowed: delivery is at-least-once); 2 = bad arguments.
if (args.Length < 3)
{
    Console.Error.WriteLine("usage: nexusmq-chaos <comma-separated urls> <access key> <seconds> [queue] [--interval-ms 20]");
    return 2;
}
var urls = args[0];
var key = args[1];
var seconds = int.Parse(args[2]);
var queue = args.Length > 3 && !args[3].StartsWith("--") ? args[3] : "chaos";
var intervalMs = args.SkipWhile(a => a != "--interval-ms").Skip(1).Select(int.Parse).DefaultIfEmpty(20).First();

await using var client = new NexusClient(urls, key);
var sentOk = new ConcurrentDictionary<int, bool>();
var received = new ConcurrentDictionary<int, int>();
int sendErrors = 0, handlerErrors = 0;
using var stop = new CancellationTokenSource(TimeSpan.FromSeconds(seconds));

await using var consumer = client.CreateProcessor(queue, new NexusProcessorOptions { MaxConcurrentCalls = 4 });
consumer.ProcessMessageAsync += a => { received.AddOrUpdate(int.Parse(a.Message.BodyAsString), 1, (_, n) => n + 1); return Task.CompletedTask; };
consumer.ProcessErrorAsync += _ => { Interlocked.Increment(ref handlerErrors); return Task.CompletedTask; };
await consumer.StartProcessingAsync();

var producer = Task.Run(async () =>
{
    var sender = client.CreateSender(queue);
    for (var i = 0; !stop.IsCancellationRequested; i++)
    {
        try { await sender.SendMessageAsync(new NexusMessage(i.ToString()) { MessageId = $"chaos-{i}" }); sentOk[i] = true; }
        catch { Interlocked.Increment(ref sendErrors); }
        if (intervalMs > 0) await Task.Delay(intervalMs);
    }
});
var monitor = Task.Run(async () =>
{
    while (!stop.IsCancellationRequested)
    {
        try { await Task.Delay(1000, stop.Token); } catch (OperationCanceledException) { break; }
        Console.WriteLine($"{DateTime.Now:HH:mm:ss} server={client.CurrentEndpoint.Authority} sent={sentOk.Count} received={received.Count} sendErrors={sendErrors}");
    }
});
await producer;
await monitor;

// Drain: wait until everything confirmed has arrived (or 20 s).
var drainUntil = DateTime.UtcNow.AddSeconds(20);
while (sentOk.Keys.Any(k => !received.ContainsKey(k)) && DateTime.UtcNow < drainUntil) await Task.Delay(200);
await consumer.StopProcessingAsync();

var missing = sentOk.Keys.Where(k => !received.ContainsKey(k)).OrderBy(k => k).ToList();
var duplicates = received.Count(kv => kv.Value > 1);
Console.WriteLine($"SUMMARY sentOk={sentOk.Count} received={received.Count} missing={missing.Count} [{string.Join(",", missing.Take(15))}] " +
                  $"duplicates={duplicates} sendErrors={sendErrors} handlerErrors={handlerErrors} finalServer={client.CurrentEndpoint}");
if (missing.Count > 0 || sentOk.Count == 0) return 1;
return duplicates > 0 ? 3 : 0;
