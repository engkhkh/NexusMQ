using System.Collections.Concurrent;
using System.Text.Json;
using System.Text.Json.Serialization;

namespace NexusMQ.AspNetCore;

/// <summary>One audited action.</summary>
public sealed record AuditEntry(
    DateTimeOffset TimeUtc,
    string Actor,
    string Action,
    string? Target,
    string Result,
    string? Detail,
    string? RemoteIp);

/// <summary>
/// Audit trail of management actions (create/delete/purge/resend/keys…) and of denied requests.
/// Keeps the latest entries in memory for <c>GET /api/audit</c> and, when the broker is durable, appends JSON lines to
/// <c>audit.log</c> (rolled to <c>audit.1.log</c> at 10 MB). Secrets are never written.
/// </summary>
public sealed class NexusAuditLog
{
    private const int MemoryCapacity = 2000;
    private const long MaxFileBytes = 10 * 1024 * 1024;

    private static readonly JsonSerializerOptions Json = new(JsonSerializerDefaults.Web)
    {
        DefaultIgnoreCondition = JsonIgnoreCondition.WhenWritingNull
    };

    private readonly ConcurrentQueue<AuditEntry> _recent = new();
    private readonly string? _file;
    private readonly TimeProvider _time;
    private readonly Lock _fileGate = new();

    public NexusAuditLog(string? file = null, TimeProvider? time = null)
    {
        _file = file;
        _time = time ?? TimeProvider.System;
        if (file is not null) Directory.CreateDirectory(Path.GetDirectoryName(file)!);
    }

    public void Record(string actor, string action, string? target, string result, string? detail = null, string? remoteIp = null)
    {
        var entry = new AuditEntry(_time.GetUtcNow(), actor, action, target, result, detail, remoteIp);
        _recent.Enqueue(entry);
        while (_recent.Count > MemoryCapacity) _recent.TryDequeue(out _);
        if (_file is null) return;
        try
        {
            lock (_fileGate)
            {
                if (File.Exists(_file) && new FileInfo(_file).Length > MaxFileBytes)
                    File.Move(_file, Path.Combine(Path.GetDirectoryName(_file)!, "audit.1.log"), overwrite: true);
                File.AppendAllText(_file, JsonSerializer.Serialize(entry, Json) + Environment.NewLine);
            }
        }
        catch (IOException)
        {
            // Never fail a request because the audit file is unavailable; the in-memory copy still has it.
        }
    }

    /// <summary>Newest first, optionally filtered.</summary>
    public List<AuditEntry> Recent(int max = 100, string? actor = null, string? result = null) =>
        _recent.Reverse()
            .Where(e => actor is null || e.Actor.Contains(actor, StringComparison.OrdinalIgnoreCase))
            .Where(e => result is null || e.Result.StartsWith(result, StringComparison.OrdinalIgnoreCase))
            .Take(Math.Clamp(max, 1, MemoryCapacity))
            .ToList();
}

/// <summary>Token-bucket rate limiting per access key: <c>RequestsPerSecond</c> sustained, <c>Burst</c> requests at once.</summary>
public sealed class NexusRateLimiter(TimeProvider? time = null)
{
    private sealed class Bucket
    {
        public double Tokens;
        public long LastTimestamp;
        public readonly Lock Gate = new();
    }

    private readonly TimeProvider _time = time ?? TimeProvider.System;
    private readonly ConcurrentDictionary<string, Bucket> _buckets = new(StringComparer.OrdinalIgnoreCase);

    /// <summary>Take one token for <paramref name="key"/>. When none is left, returns false and how long until one is.</summary>
    public bool TryAcquire(string key, double requestsPerSecond, int burst, out TimeSpan retryAfter)
    {
        retryAfter = TimeSpan.Zero;
        if (requestsPerSecond <= 0) return true;
        var capacity = Math.Max(1, burst);
        var bucket = _buckets.GetOrAdd(key, _ => new Bucket { Tokens = capacity, LastTimestamp = _time.GetTimestamp() });
        lock (bucket.Gate)
        {
            var now = _time.GetTimestamp();
            var elapsed = _time.GetElapsedTime(bucket.LastTimestamp, now).TotalSeconds;
            bucket.LastTimestamp = now;
            bucket.Tokens = Math.Min(capacity, bucket.Tokens + elapsed * requestsPerSecond);
            if (bucket.Tokens >= 1)
            {
                bucket.Tokens -= 1;
                return true;
            }
            retryAfter = TimeSpan.FromSeconds((1 - bucket.Tokens) / requestsPerSecond);
            return false;
        }
    }

    /// <summary>Forget a key's bucket (after its limits change or it is deleted).</summary>
    public void Reset(string key) => _buckets.TryRemove(key, out _);
}
