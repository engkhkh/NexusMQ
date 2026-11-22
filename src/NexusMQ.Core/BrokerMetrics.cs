using System.Collections.Concurrent;
using System.Diagnostics.Metrics;

namespace NexusMQ.Core;

/// <summary>
/// Broker metrics. Published through <see cref="System.Diagnostics.Metrics"/> under the meter name
/// <see cref="MeterName"/> (hook it into OpenTelemetry with <c>.AddMeter("NexusMQ")</c>), and also kept as
/// in-memory totals for the built-in Prometheus endpoint.
/// </summary>
public sealed class BrokerMetrics : IDisposable
{
    public const string MeterName = "NexusMQ";

    private readonly Meter _meter;
    private readonly Counter<long> _sent, _delivered, _completed, _abandoned, _deadLettered, _expired, _lockExpired;
    private readonly ConcurrentDictionary<(string Metric, string Entity), long> _totals = new();

    public BrokerMetrics(Func<IEnumerable<EntityStats>>? statsSource = null)
    {
        _meter = new Meter(MeterName, "1.2.0");
        _sent = _meter.CreateCounter<long>("nexusmq.messages.sent", "{message}", "Messages accepted into an entity");
        _delivered = _meter.CreateCounter<long>("nexusmq.messages.delivered", "{message}", "Messages handed to receivers");
        _completed = _meter.CreateCounter<long>("nexusmq.messages.completed", "{message}", "Messages completed (acknowledged)");
        _abandoned = _meter.CreateCounter<long>("nexusmq.messages.abandoned", "{message}", "Messages abandoned by receivers");
        _deadLettered = _meter.CreateCounter<long>("nexusmq.messages.deadlettered", "{message}", "Messages moved to a dead-letter queue");
        _expired = _meter.CreateCounter<long>("nexusmq.messages.expired", "{message}", "Messages whose time-to-live elapsed");
        _lockExpired = _meter.CreateCounter<long>("nexusmq.locks.expired", "{lock}", "Message locks that expired before settlement");

        if (statsSource is not null)
        {
            _meter.CreateObservableGauge("nexusmq.entity.active", () => Gauge(statsSource, s => s.Active), "{message}", "Messages ready for delivery");
            _meter.CreateObservableGauge("nexusmq.entity.locked", () => Gauge(statsSource, s => s.Locked), "{message}", "Messages locked by receivers");
            _meter.CreateObservableGauge("nexusmq.entity.scheduled", () => Gauge(statsSource, s => s.Scheduled), "{message}", "Scheduled messages");
            _meter.CreateObservableGauge("nexusmq.entity.deadletter", () => Gauge(statsSource, s => s.DeadLetter), "{message}", "Messages in the dead-letter queue");
        }
    }

    private static IEnumerable<Measurement<int>> Gauge(Func<IEnumerable<EntityStats>> source, Func<EntityStats, int> pick) =>
        source().Select(s => new Measurement<int>(pick(s), new KeyValuePair<string, object?>("entity", s.Path)));

    private void Add(Counter<long> counter, string metric, string entity, long n = 1)
    {
        counter.Add(n, new KeyValuePair<string, object?>("entity", entity));
        _totals.AddOrUpdate((metric, entity), n, (_, v) => v + n);
    }

    internal void Sent(string entity) => Add(_sent, "sent", entity);
    internal void Delivered(string entity) => Add(_delivered, "delivered", entity);
    internal void Completed(string entity) => Add(_completed, "completed", entity);
    internal void Abandoned(string entity) => Add(_abandoned, "abandoned", entity);
    internal void DeadLettered(string entity) => Add(_deadLettered, "deadlettered", entity);
    internal void Expired(string entity) => Add(_expired, "expired", entity);
    internal void LockExpired(string entity) => Add(_lockExpired, "lock_expired", entity);

    /// <summary>Running totals since start: (metric, entity) → count.</summary>
    public IReadOnlyDictionary<(string Metric, string Entity), long> Totals => _totals;

    public void Dispose() => _meter.Dispose();
}
