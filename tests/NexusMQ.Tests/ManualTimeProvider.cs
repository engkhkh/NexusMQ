namespace NexusMQ.Tests;

/// <summary>Clock the tests can move forward by hand.</summary>
public sealed class ManualTimeProvider(DateTimeOffset start) : TimeProvider
{
    private DateTimeOffset _now = start;

    public ManualTimeProvider() : this(new DateTimeOffset(2026, 1, 1, 12, 0, 0, TimeSpan.Zero)) { }

    public override DateTimeOffset GetUtcNow() => _now;

    // Timestamps follow the manual clock too, so rate limiters and elapsed-time code can be tested.
    public override long GetTimestamp() => _now.UtcTicks;
    public override long TimestampFrequency => TimeSpan.TicksPerSecond;

    public void Advance(TimeSpan by) => _now += by;
}
