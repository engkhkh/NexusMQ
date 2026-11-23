using System.Text.Json;
using NexusMQ.Client;

namespace NexusMQ.Cli;

internal sealed class CliException(string message) : Exception(message);

/// <summary>Tiny argument parser: positional words, "--name value" options, "--flag" switches, repeatable options.</summary>
internal sealed class Args
{
    private static readonly HashSet<string> Switches =
        ["json", "session", "abandon", "deadletter", "delete", "version", "help", "insecure", "move", "copy", "none"];

    private readonly List<string> _positional = [];
    private readonly List<(string Name, string? Value)> _options = [];
    private int _next;

    public Args(string[] args)
    {
        for (var i = 0; i < args.Length; i++)
        {
            var a = args[i];
            if (a.StartsWith("--") && a.Length > 2)
            {
                var name = a[2..];
                string? value = null;
                var eq = name.IndexOf('=');
                if (eq > 0) { value = name[(eq + 1)..]; name = name[..eq]; }
                else if (!Switches.Contains(name) && i + 1 < args.Length && !args[i + 1].StartsWith("--")) value = args[++i];
                _options.Add((name.ToLowerInvariant(), value));
            }
            else _positional.Add(a);
        }
    }

    public string? Next() => _next < _positional.Count ? _positional[_next++] : null;
    public string Required(string what) => Next() ?? throw new CliException($"Missing {what}. Run 'nexusmq help'.");
    public string? Opt(string name) => _options.LastOrDefault(o => o.Name == name && o.Value is not null).Value;
    public bool Has(string name) => _options.Any(o => o.Name == name);
    public List<string> All(string name) => _options.Where(o => o.Name == name && o.Value is not null).Select(o => o.Value!).ToList();
}

internal static class Out
{
    public static void Json<T>(T value, JsonSerializerOptions options) => Console.WriteLine(JsonSerializer.Serialize(value, options));

    public static void Table(string[] headers, IEnumerable<string[]> rows)
    {
        var all = rows.ToList();
        var widths = headers.Select((h, i) => Math.Max(h.Length, all.Count == 0 ? 0 : all.Max(r => r[i].Length))).ToArray();
        Console.WriteLine(string.Join("  ", headers.Select((h, i) => h.PadRight(widths[i]))));
        foreach (var r in all) Console.WriteLine(string.Join("  ", r.Select((c, i) => c.PadRight(widths[i]))));
    }

    public static void Messages(IEnumerable<NexusReceivedMessage> messages, bool json, JsonSerializerOptions options)
    {
        foreach (var m in messages)
        {
            if (json)
            {
                // One compact JSON object per line (JSON Lines) so output can be piped and parsed line by line.
                Console.WriteLine(JsonSerializer.Serialize(new
                {
                    m.SequenceNumber, m.MessageId, m.Subject, m.CorrelationId, m.SessionId, m.ContentType,
                    Properties = m.ApplicationProperties, m.DeliveryCount, m.EnqueuedTimeUtc, m.State,
                    m.DeadLetterReason, m.DeadLetterErrorDescription, Body = m.BodyAsString
                }, new JsonSerializerOptions(options) { WriteIndented = false }));
                continue;
            }
            var meta = new List<string> { $"#{m.SequenceNumber}", m.MessageId };
            if (m.Subject is not null) meta.Add($"subject={m.Subject}");
            if (m.SessionId is not null) meta.Add($"session={m.SessionId}");
            if (m.DeliveryCount > 0) meta.Add($"deliveries={m.DeliveryCount}");
            if (m.State is NexusMessageState.Scheduled or NexusMessageState.Deferred) meta.Add(m.State.ToString().ToLowerInvariant());
            if (m.DeadLetterReason is not null) meta.Add($"dlq={m.DeadLetterReason}");
            foreach (var (k, v) in m.ApplicationProperties) meta.Add($"{k}={v}");
            Console.ForegroundColor = ConsoleColor.DarkGray;
            Console.WriteLine(string.Join("  ", meta));
            Console.ResetColor();
            Console.WriteLine(m.BodyAsString);
        }
    }
}

internal static class Help
{
    public static void Print() => Console.WriteLine("""
        nexusmq — command-line tool for NexusMQ

        Connection (or set NEXUSMQ_URL / NEXUSMQ_KEY):
          --url http://localhost:5680   --key <access key>   --json (machine-readable output)
          --insecure   accept self-signed HTTPS certificates (development only)

        Try it:
          nexusmq demo [--minutes 5]          sample workload (orders, events, dead letters, errors) for the dashboard

        Messages:
          nexusmq send <entity> <body | @file> [--subject s] [--session id] [--prop k=v]... [--count n]
                       [--delay seconds] [--ttl seconds] [--message-id id] [--routing-key key]
                       (entity = queue, topic, or exchanges/<name>; the body can also be piped in)
          nexusmq peek <entity> [--max 20] [--from seq]                  browse without locking
          nexusmq receive <entity> [--max 1] [--wait 5] [--abandon | --deadletter]   lock, print, complete
          nexusmq listen <entity> [--abandon | --delete]                 stream until Ctrl+C
          nexusmq purge <entity>              nexusmq resubmit <entity>  (move all dead letters back)
          nexusmq resend <entity> <seq> [--to destination] [--move | --copy]   send one message again
                       (from a $deadletter queue the default is --move back to where it failed)
          nexusmq delete-message <entity> <seq>

          Entity paths: orders   orders/$deadletter   events/subscriptions/billing   exchanges/logs

        Topology:
          nexusmq stats | topology | metrics
          nexusmq create queue <name> [--session] [--max-delivery n] [--lock s] [--ttl s] [--dedup s]
                                      [--max-size n] [--forward-to x] [--forward-dlq-to x]
          nexusmq create topic <name> [--dedup seconds]
          nexusmq create subscription <topic> <name> [--sql "region = 'eu'"] [--action "SET p = 'x'"] [--subject "order.#"]
          nexusmq create exchange <name> <direct | fanout | topic | headers>
          nexusmq bind <exchange> <destination> [routing-key]    nexusmq unbind <exchange> <destination> [routing-key]
          nexusmq delete queue|topic|exchange <name>             nexusmq delete subscription <topic> <name>

        Access keys (needs a broker-wide Manage key):
          nexusmq keys list
          nexusmq keys create <name> --rights Send,Listen,Manage,Monitor [--entity orders]... [--rps 50 --burst 100]
          nexusmq keys limits <name> --rps 50 [--burst 100] | --none
          nexusmq keys rotate <name> [--grace seconds]
          nexusmq keys delete <name>
          nexusmq audit [--max 50] [--actor name] [--result denied]     management actions and denied requests

        High availability (give both servers: --url http://a:5680,http://b:5680):
          nexusmq replication status          role, position and connection of every server
          nexusmq --url http://b:5680 replication promote     make that standby the primary

        Raft cluster membership (on any server; followers redirect to the leader):
          nexusmq cluster members
          nexusmq cluster add <id> <url>      start the new server first with NexusMQ:Cluster:Join=true
          nexusmq cluster remove <id>         then shut that server down

        Examples:
          nexusmq create queue orders --max-delivery 5
          echo '{"id":42}' | nexusmq send orders --subject order.created --prop region=eu
          nexusmq listen orders
          nexusmq keys create orders-api --rights Send --entity orders
        """);
}
