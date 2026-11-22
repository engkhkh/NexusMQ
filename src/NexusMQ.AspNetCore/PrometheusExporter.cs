using System.Globalization;
using System.Text;
using NexusMQ.Core;

namespace NexusMQ.AspNetCore;

/// <summary>Renders broker metrics in the Prometheus text exposition format (scrape <c>/api/metrics</c>).</summary>
public static class PrometheusExporter
{
    private static readonly (string Metric, string Name, string Help)[] Counters =
    [
        ("sent", "nexusmq_messages_sent_total", "Messages accepted into an entity."),
        ("delivered", "nexusmq_messages_delivered_total", "Messages handed to receivers."),
        ("completed", "nexusmq_messages_completed_total", "Messages completed (acknowledged)."),
        ("abandoned", "nexusmq_messages_abandoned_total", "Messages abandoned by receivers."),
        ("deadlettered", "nexusmq_messages_deadlettered_total", "Messages moved to a dead-letter queue."),
        ("expired", "nexusmq_messages_expired_total", "Messages whose time-to-live elapsed."),
        ("lock_expired", "nexusmq_locks_expired_total", "Message locks that expired before settlement."),
    ];

    private static readonly (string Name, string Help, Func<EntityStats, long> Pick)[] Gauges =
    [
        ("nexusmq_entity_active_messages", "Messages ready for delivery.", s => s.Active),
        ("nexusmq_entity_locked_messages", "Messages locked by receivers.", s => s.Locked),
        ("nexusmq_entity_scheduled_messages", "Scheduled messages.", s => s.Scheduled),
        ("nexusmq_entity_deferred_messages", "Deferred messages.", s => s.Deferred),
        ("nexusmq_entity_deadletter_messages", "Messages in the dead-letter queue.", s => s.DeadLetter),
        ("nexusmq_entity_locked_sessions", "Sessions locked by receivers.", s => s.LockedSessions),
    ];

    public static string Render(Broker broker)
    {
        var sb = new StringBuilder();
        var totals = broker.Metrics.Totals;
        foreach (var (metric, name, help) in Counters)
        {
            sb.Append("# HELP ").Append(name).Append(' ').AppendLine(help);
            sb.Append("# TYPE ").Append(name).AppendLine(" counter");
            foreach (var ((m, entity), value) in totals.Where(kv => kv.Key.Metric == metric).OrderBy(kv => kv.Key.Entity))
                sb.Append(name).Append("{entity=\"").Append(Escape(entity)).Append("\"} ").AppendLine(value.ToString(CultureInfo.InvariantCulture));
        }

        var stats = broker.GetStats();
        foreach (var (name, help, pick) in Gauges)
        {
            sb.Append("# HELP ").Append(name).Append(' ').AppendLine(help);
            sb.Append("# TYPE ").Append(name).AppendLine(" gauge");
            foreach (var s in stats)
                sb.Append(name).Append("{entity=\"").Append(Escape(s.Path)).Append("\",kind=\"").Append(s.Kind).Append("\"} ")
                  .AppendLine(pick(s).ToString(CultureInfo.InvariantCulture));
        }
        return sb.ToString();
    }

    private static string Escape(string v) => v.Replace("\\", "\\\\").Replace("\"", "\\\"").Replace("\n", "\\n");
}
