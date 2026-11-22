using System.Diagnostics;
using System.Globalization;
using NexusMQ.Client;

// Compares WebSocket push vs HTTP long-polling. Start NexusMQ.Server first, then run in Release:
//   dotnet run -c Release --project samples/Sample.Benchmark [serverUrl(s)] [messages] [accessKey] [bodyBytes]
// Set NEXUSMQ_BINARY=0 to receive push bodies as base64 JSON (the pre-1.11 format) for comparison.
var url = args.ElementAtOrDefault(0) ?? "http://localhost:5680";
var count = int.TryParse(args.ElementAtOrDefault(1), out var n) ? n : 5000;
var key = args.ElementAtOrDefault(2) is { Length: > 0 } k && k != "-" ? k : Environment.GetEnvironmentVariable("NEXUSMQ_KEY");
var bodyBytes = int.TryParse(args.ElementAtOrDefault(3), out var bb) ? bb : 0;
var binary = Environment.GetEnvironmentVariable("NEXUSMQ_BINARY") != "0";
await using var client = new NexusClient(url, key, new NexusClientOptions { BinaryBodies = binary });
var payload = new byte[bodyBytes];
Random.Shared.NextBytes(payload);
NexusMessage Make(string text) => bodyBytes > 0 ? new NexusMessage(payload) : new NexusMessage(text);

Console.WriteLine($"NexusMQ benchmark against {url}" + (bodyBytes > 0 ? $", {bodyBytes:N0}-byte bodies" : "") + (binary ? "" : ", base64 push bodies") + "\n");
Console.WriteLine($"{"transport",-12} {"latency p50",12} {"p99",9} {"throughput",16} {"send (batches)",16}");

foreach (var transport in new[] { NexusTransport.WebSocket, NexusTransport.LongPolling })
{
    var queue = $"bench-{transport.ToString().ToLowerInvariant()}";
    await client.Admin.CreateQueueAsync(queue);
    await client.Admin.PurgeAsync(queue);

    var (p50, p99) = await MeasureLatency(client, queue, transport, 300);
    var (rate, sendRate) = await MeasureThroughput(client, queue, transport, count);
    Console.WriteLine($"{transport,-12} {p50,10:F2}ms {p99,7:F2}ms {rate,12:N0} msg/s {sendRate,12:N0} msg/s");
}

async Task<(double P50, double P99)> MeasureLatency(NexusClient client, string queue, NexusTransport transport, int rounds)
{
    TaskCompletionSource<long>? waiting = null;
    await using var processor = client.CreateProcessor(queue, new NexusProcessorOptions { Transport = transport });
    processor.ProcessMessageAsync += args =>
    {
        waiting?.TrySetResult(Stopwatch.GetTimestamp());
        return Task.CompletedTask;
    };
    processor.ProcessErrorAsync += e => { Console.WriteLine("  error: " + e.Exception.Message); return Task.CompletedTask; };
    await processor.StartProcessingAsync();
    await Task.Delay(500); // let the subscription / first poll settle

    var sender = client.CreateSender(queue);
    var samples = new List<double>();
    for (var i = 0; i < rounds + 20; i++)
    {
        waiting = new TaskCompletionSource<long>(TaskCreationOptions.RunContinuationsAsynchronously);
        var start = Stopwatch.GetTimestamp();
        await sender.SendMessageAsync(Make("ping"));
        var end = await waiting.Task.WaitAsync(TimeSpan.FromSeconds(30));
        if (i >= 20) samples.Add(Stopwatch.GetElapsedTime(start, end).TotalMilliseconds); // skip warm-up
    }
    await processor.StopProcessingAsync();
    samples.Sort();
    return (samples[samples.Count / 2], samples[(int)(samples.Count * 0.99)]);
}

async Task<(double Receive, double Send)> MeasureThroughput(NexusClient client, string queue, NexusTransport transport, int count)
{
    var sender = client.CreateSender(queue);
    var sendClock = Stopwatch.StartNew();
    for (var i = 0; i < count; i += 250)
        await sender.SendMessagesAsync(Enumerable.Range(i, Math.Min(250, count - i)).Select(x => Make(x.ToString(CultureInfo.InvariantCulture))));

    var sendRate = count / sendClock.Elapsed.TotalSeconds;
    var received = 0;
    var done = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
    await using var processor = client.CreateProcessor(queue, new NexusProcessorOptions
    {
        Transport = transport,
        MaxConcurrentCalls = 16,
        PrefetchCount = 64,
        MaxAutoLockRenewalDuration = TimeSpan.Zero
    });
    processor.ProcessMessageAsync += _ =>
    {
        if (Interlocked.Increment(ref received) == count) done.TrySetResult();
        return Task.CompletedTask;
    };
    processor.ProcessErrorAsync += e => { Console.WriteLine("  error: " + e.Exception.Message); return Task.CompletedTask; };

    var sw = Stopwatch.StartNew();
    await processor.StartProcessingAsync();
    await done.Task.WaitAsync(TimeSpan.FromMinutes(5));
    sw.Stop();
    await processor.StopProcessingAsync();
    return (count / sw.Elapsed.TotalSeconds, sendRate);
}
