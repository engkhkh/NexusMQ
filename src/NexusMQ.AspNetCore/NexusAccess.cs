using System.Collections.Concurrent;
using System.Security.Cryptography;
using System.Text.Json;
using System.Text.Json.Serialization;
using System.Text;
using System.Text.RegularExpressions;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Routing;
using NexusMQ.Core;

namespace NexusMQ.AspNetCore;

/// <summary>What an access key may do (like Azure Service Bus SAS claims).</summary>
[Flags]
public enum NexusRights
{
    None = 0,
    /// <summary>Send messages (and cancel scheduled ones).</summary>
    Send = 1,
    /// <summary>Receive, peek, settle, use sessions.</summary>
    Listen = 2,
    /// <summary>Create/delete entities, bindings, purge, resubmit dead letters (includes Monitor).</summary>
    Manage = 4,
    /// <summary>Read-only: stats, topology and Prometheus metrics. Give this to monitoring tools.</summary>
    Monitor = 8,
    All = Send | Listen | Manage | Monitor
}

/// <summary>
/// A named key with rights on a set of entities. In appsettings.json:
/// <code>
/// "AccessKeys": [
///   { "Name": "orders-api",  "Key": "…", "Rights": "Send",   "Entities": [ "orders", "events" ] },
///   { "Name": "billing-svc", "Key": "…", "Rights": "Listen", "Entities": [ "events/subscriptions/billing" ] },
///   { "Name": "ops",         "Key": "…", "Rights": "All" }
/// ]
/// </code>
/// Entity patterns: <c>*</c> = everything; <c>orders</c> also covers <c>orders/$deadletter</c>; <c>events</c> covers its
/// subscriptions; <c>*</c> inside a pattern is a wildcard (<c>tenant-*</c>, <c>exchanges/*</c>).
/// </summary>
public sealed class NexusAccessKey
{
    public string Name { get; set; } = "";
    public string Key { get; set; } = "";
    public NexusRights Rights { get; set; } = NexusRights.None;
    /// <summary>Entity patterns. Empty (the default) = every entity. (Not pre-filled: the config binder appends to lists.)</summary>
    public List<string> Entities { get; set; } = [];
    /// <summary>Sustained request limit (HTTP calls and WebSocket operations). Null = unlimited.</summary>
    public double? RequestsPerSecond { get; set; }
    /// <summary>Requests allowed at once before the rate applies. Default: 2 × RequestsPerSecond (at least 1).</summary>
    public int? Burst { get; set; }
}

/// <summary>Public view of a key (never includes the secret).</summary>
public sealed class AccessKeyInfo
{
    public required string Name { get; init; }
    public NexusRights Rights { get; init; }
    public List<string> Entities { get; init; } = [];
    /// <summary>"config" (appsettings, read-only here) or "api" (created at runtime, stored hashed).</summary>
    public required string Source { get; init; }
    public DateTimeOffset? CreatedUtc { get; init; }
    public DateTimeOffset? RotatedUtc { get; init; }
    /// <summary>When the previous secret stops working after a rotation with a grace period.</summary>
    public DateTimeOffset? PreviousKeyExpiresUtc { get; init; }
    public DateTimeOffset? LastUsedUtc { get; init; }
    public double? RequestsPerSecond { get; init; }
    public int? Burst { get; init; }
}

/// <summary>A key created through the API. Only SHA-256 hashes of secrets are stored.</summary>
public sealed class StoredAccessKey
{
    public string Name { get; set; } = "";
    public string KeyHash { get; set; } = "";
    public string? PreviousKeyHash { get; set; }
    public DateTimeOffset? PreviousKeyExpiresUtc { get; set; }
    public NexusRights Rights { get; set; }
    public List<string> Entities { get; set; } = [];
    public DateTimeOffset CreatedUtc { get; set; }
    public DateTimeOffset? RotatedUtc { get; set; }
    public double? RequestsPerSecond { get; set; }
    public int? Burst { get; set; }
}

/// <summary>
/// Resolves presented keys and checks rights. Keys come from configuration (<see cref="NexusServerOptions.ApiKey"/>,
/// <see cref="NexusServerOptions.AccessKeys"/>) and from the runtime key API (persisted hashed in <c>access-keys.json</c>).
/// Without any key the broker is open (development mode).
/// </summary>
public sealed partial class NexusAuthorizer
{
    private static readonly JsonSerializerOptions FileJson = new(JsonSerializerDefaults.Web)
    {
        WriteIndented = true,
        Converters = { new JsonStringEnumConverter() }
    };

    private readonly List<NexusAccessKey> _configKeys;
    private readonly string? _keyFile;
    private readonly TimeProvider _time;
    private readonly Lock _gate = new();
    private readonly ConcurrentDictionary<string, DateTimeOffset> _lastUsed = new(StringComparer.OrdinalIgnoreCase);
    private volatile List<StoredAccessKey> _stored;
    private readonly NexusRateLimiter _limiter;

    /// <summary>Create the authorizer. <c>keyFile</c> is where runtime keys are persisted; null keeps them in memory only.</summary>
    public NexusAuthorizer(NexusServerOptions options, string? keyFile = null, TimeProvider? time = null)
    {
        _configKeys = [.. options.AccessKeys.Where(k => !string.IsNullOrEmpty(k.Key))];
        if (!string.IsNullOrEmpty(options.ApiKey))
            _configKeys.Add(new NexusAccessKey { Name = "master", Key = options.ApiKey, Rights = NexusRights.All });
        _keyFile = keyFile;
        _time = time ?? TimeProvider.System;
        _limiter = new NexusRateLimiter(_time);
        _stored = keyFile is not null && File.Exists(keyFile)
            ? JsonSerializer.Deserialize<List<StoredAccessKey>>(File.ReadAllText(keyFile), FileJson) ?? []
            : [];
    }

    /// <summary>True when no keys exist at all: every request is allowed.</summary>
    public bool IsOpen => _configKeys.Count == 0 && _stored.Count == 0;

    /// <summary>The key from the X-Api-Key header or the apiKey query parameter (for Prometheus scrapes and browsers).</summary>
    public static string? PresentedKey(HttpContext http)
    {
        var header = http.Request.Headers["X-Api-Key"].ToString();
        if (!string.IsNullOrEmpty(header)) return header;
        var query = http.Request.Query["apiKey"].ToString();
        return string.IsNullOrEmpty(query) ? null : query;
    }

    private static string Hash(string secret) => Convert.ToBase64String(SHA256.HashData(Encoding.UTF8.GetBytes(secret)));

    private static bool HashEquals(string a, string b) =>
        CryptographicOperations.FixedTimeEquals(Encoding.ASCII.GetBytes(a), Encoding.ASCII.GetBytes(b));

    /// <summary>All keys equal to <paramref name="presented"/> (compared in constant time). Null when none match.</summary>
    public IReadOnlyList<NexusAccessKey>? Authenticate(string? presented)
    {
        if (string.IsNullOrEmpty(presented)) return null;
        var now = _time.GetUtcNow();
        var bytes = Encoding.UTF8.GetBytes(presented);
        var matches = _configKeys.Where(k => CryptographicOperations.FixedTimeEquals(Encoding.UTF8.GetBytes(k.Key), bytes)).ToList();

        var hash = Hash(presented);
        foreach (var k in _stored)
        {
            var current = HashEquals(k.KeyHash, hash);
            var previous = k.PreviousKeyHash is not null && k.PreviousKeyExpiresUtc > now && HashEquals(k.PreviousKeyHash, hash);
            if (current || previous)
                matches.Add(new NexusAccessKey { Name = k.Name, Rights = k.Rights, Entities = k.Entities, RequestsPerSecond = k.RequestsPerSecond, Burst = k.Burst });
        }

        if (matches.Count == 0) return null;
        foreach (var m in matches) _lastUsed[m.Name] = now;
        return matches;
    }

    /// <summary>Throws Unauthorized/Forbidden unless the caller may do <paramref name="right"/> on <paramref name="entity"/> (null = broker-wide).</summary>
    public void Demand(IReadOnlyList<NexusAccessKey>? caller, NexusRights right, string? entity)
    {
        if (IsOpen || right == NexusRights.None) return;
        if (caller is null)
            throw new NexusException(NexusErrorCode.Unauthorized, "Missing or invalid access key (send header X-Api-Key).");
        bool Has(NexusAccessKey k) => k.Rights.HasFlag(right) || (right == NexusRights.Monitor && k.Rights.HasFlag(NexusRights.Manage));
        if (!caller.Any(k => Has(k) && Covers(k, entity)))
            throw new NexusException(NexusErrorCode.Forbidden,
                $"Key '{caller[0].Name}' does not have {right} rights on '{entity ?? "the broker"}'.");
    }

    /// <summary>
    /// Count one request against the caller's rate limit. Throws RateLimited (with RetryAfter) when exhausted.
    /// Open brokers, anonymous callers and keys without a limit are not limited; with several matching keys the most generous applies.
    /// </summary>
    public void CheckRate(IReadOnlyList<NexusAccessKey>? caller)
    {
        if (IsOpen || caller is null || caller.Count == 0 || caller.Any(k => k.RequestsPerSecond is null or <= 0)) return;
        var key = caller.MaxBy(k => k.RequestsPerSecond)!;
        var rps = key.RequestsPerSecond!.Value;
        var burst = key.Burst ?? (int)Math.Max(1, Math.Ceiling(rps * 2));
        if (!_limiter.TryAcquire(key.Name, rps, burst, out var retryAfter))
            throw new NexusException(NexusErrorCode.RateLimited,
                $"Key '{key.Name}' is limited to {rps:0.##} requests/second; retry in {retryAfter.TotalMilliseconds:0} ms.")
            { RetryAfter = retryAfter };
    }

    /// <summary>A short "who" for the audit log.</summary>
    public string Actor(IReadOnlyList<NexusAccessKey>? caller) =>
        IsOpen ? "anonymous (open broker)" : caller is null ? "unauthenticated" : string.Join(",", caller.Select(k => k.Name));

    // ------------------------------------------------------------------ runtime key management

    [GeneratedRegex(@"^[A-Za-z0-9][A-Za-z0-9._\-]{0,99}$")]
    private static partial Regex KeyNameRegex();

    public List<AccessKeyInfo> ListKeys()
    {
        var list = _configKeys.Select(k => new AccessKeyInfo
        {
            Name = k.Name, Rights = k.Rights, Entities = k.Entities, Source = "config",
            LastUsedUtc = _lastUsed.TryGetValue(k.Name, out var u) ? u : null,
            RequestsPerSecond = k.RequestsPerSecond, Burst = k.Burst
        }).ToList();
        list.AddRange(_stored.Select(Info));
        return list.OrderBy(k => k.Name, StringComparer.OrdinalIgnoreCase).ToList();
    }

    private AccessKeyInfo Info(StoredAccessKey k) => new()
    {
        Name = k.Name, Rights = k.Rights, Entities = k.Entities, Source = "api",
        CreatedUtc = k.CreatedUtc, RotatedUtc = k.RotatedUtc,
        PreviousKeyExpiresUtc = k.PreviousKeyExpiresUtc > _time.GetUtcNow() ? k.PreviousKeyExpiresUtc : null,
        LastUsedUtc = _lastUsed.TryGetValue(k.Name, out var u) ? u : null,
        RequestsPerSecond = k.RequestsPerSecond, Burst = k.Burst
    };

    private static string NewSecret() => "nmq_" + Convert.ToBase64String(RandomNumberGenerator.GetBytes(32))
        .TrimEnd('=').Replace('+', '-').Replace('/', '_');

    /// <summary>Rights the caller holds broker-wide (all rights when the broker is open).</summary>
    private NexusRights BrokerWideRights(IReadOnlyList<NexusAccessKey>? caller) =>
        IsOpen ? NexusRights.All : (caller ?? []).Where(k => Covers(k, null)).Aggregate(NexusRights.None, (r, k) => r | k.Rights);

    /// <summary>Create a key. Returns its info and the secret, which is shown only this once.</summary>
    public (AccessKeyInfo Key, string Secret) CreateKey(IReadOnlyList<NexusAccessKey>? caller, string name, NexusRights rights, List<string>? entities,
        double? requestsPerSecond = null, int? burst = null)
    {
        ValidateLimits(requestsPerSecond, burst);
        if (!KeyNameRegex().IsMatch(name ?? ""))
            throw new NexusException(NexusErrorCode.BadRequest, "Key names use letters, digits, '.', '-' or '_' (max 100 chars).");
        if (rights == NexusRights.None)
            throw new NexusException(NexusErrorCode.BadRequest, "Give the key at least one right: Send, Listen, Manage or Monitor.");
        var missing = rights & ~BrokerWideRights(caller);
        if (missing != NexusRights.None)
            throw new NexusException(NexusErrorCode.Forbidden, $"You cannot grant rights you do not hold broker-wide: {missing}.");
        if (IsOpen && !rights.HasFlag(NexusRights.Manage))
            throw new NexusException(NexusErrorCode.BadRequest,
                "The first key turns authentication on, so it must include Manage (otherwise nobody could manage the broker).");

        var secret = NewSecret();
        lock (_gate)
        {
            if (_configKeys.Any(k => k.Name.Equals(name, StringComparison.OrdinalIgnoreCase))
                || _stored.Any(k => k.Name.Equals(name, StringComparison.OrdinalIgnoreCase)))
                throw new NexusException(NexusErrorCode.EntityAlreadyExists, $"A key named '{name}' already exists.");
            var key = new StoredAccessKey
            {
                Name = name!, KeyHash = Hash(secret), Rights = rights,
                Entities = entities?.Where(e => !string.IsNullOrWhiteSpace(e)).Select(e => e.Trim()).ToList() ?? [],
                CreatedUtc = _time.GetUtcNow(),
                RequestsPerSecond = requestsPerSecond,
                Burst = burst
            };
            Save([.. _stored, key]);
            return (Info(key), secret);
        }
    }

    /// <summary>Issue a new secret. With a grace period the old secret keeps working until it ends (zero-downtime rollout).</summary>
    public (AccessKeyInfo Key, string Secret) RotateKey(IReadOnlyList<NexusAccessKey>? caller, string name, TimeSpan grace)
    {
        var secret = NewSecret();
        lock (_gate)
        {
            var existing = FindStored(name);
            var missing = existing.Rights & ~BrokerWideRights(caller);
            if (missing != NexusRights.None)
                throw new NexusException(NexusErrorCode.Forbidden, $"You cannot rotate a key with rights you do not hold broker-wide: {missing}.");
            var now = _time.GetUtcNow();
            var rotated = new StoredAccessKey
            {
                Name = existing.Name, KeyHash = Hash(secret), Rights = existing.Rights, Entities = existing.Entities,
                CreatedUtc = existing.CreatedUtc, RotatedUtc = now,
                RequestsPerSecond = existing.RequestsPerSecond, Burst = existing.Burst,
                PreviousKeyHash = grace > TimeSpan.Zero ? existing.KeyHash : null,
                PreviousKeyExpiresUtc = grace > TimeSpan.Zero ? now + grace : null
            };
            Save(_stored.Select(k => k == existing ? rotated : k).ToList());
            return (Info(rotated), secret);
        }
    }

    /// <summary>Change (or with nulls, remove) a runtime key's rate limit. Takes effect immediately.</summary>
    public AccessKeyInfo SetLimits(IReadOnlyList<NexusAccessKey>? caller, string name, double? requestsPerSecond, int? burst)
    {
        ValidateLimits(requestsPerSecond, burst);
        lock (_gate)
        {
            var existing = FindStored(name);
            var missing = existing.Rights & ~BrokerWideRights(caller);
            if (missing != NexusRights.None)
                throw new NexusException(NexusErrorCode.Forbidden, $"You cannot change a key with rights you do not hold broker-wide: {missing}.");
            var updated = new StoredAccessKey
            {
                Name = existing.Name, KeyHash = existing.KeyHash, PreviousKeyHash = existing.PreviousKeyHash,
                PreviousKeyExpiresUtc = existing.PreviousKeyExpiresUtc, Rights = existing.Rights, Entities = existing.Entities,
                CreatedUtc = existing.CreatedUtc, RotatedUtc = existing.RotatedUtc,
                RequestsPerSecond = requestsPerSecond, Burst = burst
            };
            Save(_stored.Select(k => k == existing ? updated : k).ToList());
            _limiter.Reset(name);
            return Info(updated);
        }
    }

    private static void ValidateLimits(double? requestsPerSecond, int? burst)
    {
        if (requestsPerSecond is <= 0 || burst is <= 0)
            throw new NexusException(NexusErrorCode.BadRequest, "RequestsPerSecond and Burst must be positive (leave them empty for no limit).");
    }

    public void DeleteKey(IReadOnlyList<NexusAccessKey>? caller, string name)
    {
        lock (_gate)
        {
            var existing = FindStored(name);
            var missing = existing.Rights & ~BrokerWideRights(caller);
            if (missing != NexusRights.None)
                throw new NexusException(NexusErrorCode.Forbidden, $"You cannot delete a key with rights you do not hold broker-wide: {missing}.");
            var remaining = _stored.Where(k => k != existing).ToList();
            var manageLeft = _configKeys.Any(k => k.Rights.HasFlag(NexusRights.Manage) && Covers(k, null))
                             || remaining.Any(k => k.Rights.HasFlag(NexusRights.Manage) && (k.Entities.Count == 0 || k.Entities.Contains("*")));
            if (!manageLeft)
                throw new NexusException(NexusErrorCode.BadRequest,
                    "Deleting this key would leave no broker-wide Manage key (you would be locked out, or the broker would become open).");
            Save(remaining);
            _lastUsed.TryRemove(name, out _);
            _limiter.Reset(name);
        }
    }

    private StoredAccessKey FindStored(string name)
    {
        if (_configKeys.Any(k => k.Name.Equals(name, StringComparison.OrdinalIgnoreCase)))
            throw new NexusException(NexusErrorCode.BadRequest, $"Key '{name}' comes from configuration; change it in appsettings.json.");
        return _stored.FirstOrDefault(k => k.Name.Equals(name, StringComparison.OrdinalIgnoreCase))
               ?? throw new NexusException(NexusErrorCode.EntityNotFound, $"Key '{name}' not found.");
    }

    /// <summary>Raised with the stored keys (hashes) as JSON whenever runtime keys change, so a standby can follow.</summary>
    public event Action<string>? KeysChanged;

    /// <summary>The runtime keys (hashes only) as JSON, for replication snapshots.</summary>
    public string ExportKeysJson() => JsonSerializer.Serialize(_stored, FileJson);

    /// <summary>Replace the runtime keys with a replicated copy (standby side).</summary>
    public void ReplaceKeys(string? json)
    {
        var keys = string.IsNullOrEmpty(json) ? [] : JsonSerializer.Deserialize<List<StoredAccessKey>>(json, FileJson) ?? [];
        lock (_gate) Save(keys, raiseEvent: false);
    }

    private void Save(List<StoredAccessKey> keys, bool raiseEvent = true)
    {
        if (_keyFile is not null)
        {
            Directory.CreateDirectory(Path.GetDirectoryName(_keyFile)!);
            var tmp = _keyFile + ".tmp";
            File.WriteAllText(tmp, JsonSerializer.Serialize(keys, FileJson));
            File.Move(tmp, _keyFile, overwrite: true);
        }
        _stored = keys;
        if (raiseEvent) KeysChanged?.Invoke(JsonSerializer.Serialize(keys, FileJson));
    }

    public static bool Covers(NexusAccessKey key, string? entity) =>
        key.Entities.Count == 0 || key.Entities.Any(p => p == "*" || (entity is not null && PatternMatches(p, entity.Trim('/'))));

    private static bool PatternMatches(string pattern, string entity)
    {
        pattern = pattern.Trim('/');
        if (entity.Equals(pattern, StringComparison.OrdinalIgnoreCase)
            || entity.StartsWith(pattern + "/", StringComparison.OrdinalIgnoreCase)) return true;
        if (!pattern.Contains('*')) return false;
        var regex = "^" + Regex.Escape(pattern).Replace("\\*", "[^/]*") + "(/.*)?$";
        return Regex.IsMatch(entity, regex, RegexOptions.IgnoreCase | RegexOptions.CultureInvariant);
    }

    /// <summary>The right and entity an HTTP API call needs, from its route template (relative to the API prefix).</summary>
    public static (NexusRights Right, string? Entity) RequirementFor(string route, RouteValueDictionary values, IList<object?> arguments)
    {
        string? V(string name) => values.TryGetValue(name, out var v) ? v?.ToString() : null;
        var path = V("path");

        if (route.StartsWith("/send/")) return (NexusRights.Send, path);
        if (route.StartsWith("/scheduled/")) return (NexusRights.Send, V("queue"));
        if (route.StartsWith("/receive/") || route.StartsWith("/settle/") || route.StartsWith("/deferred/")
            || route.StartsWith("/peek/") || route.StartsWith("/sessions/")) return (NexusRights.Listen, path);
        if (route.StartsWith("/messages/") || route.StartsWith("/resubmit/")) return (NexusRights.Manage, path);
        if (route.StartsWith("/topics/{topic}/subscriptions/")) return (NexusRights.Manage, $"{V("topic")}/subscriptions/{V("name")}");
        if (route.StartsWith("/queues/") || route.StartsWith("/topics/")) return (NexusRights.Manage, V("name"));
        if (route.StartsWith("/exchanges/")) return (NexusRights.Manage, "exchanges/" + V("name"));
        if (route.StartsWith("/bindings"))
        {
            var binding = arguments.OfType<BindingDefinition>().FirstOrDefault();
            return (NexusRights.Manage, binding is null ? null : "exchanges/" + binding.Exchange);
        }
        if (route == "/health") return (NexusRights.None, null);
        if (route.StartsWith("/keys")) return (NexusRights.Manage, null); // key management is broker-wide
        if (route is "/stats" or "/topology" or "/metrics" or "/replication/status") return (NexusRights.Monitor, null);
        return (NexusRights.Manage, null); // anything new defaults to broker-wide manage
    }
}
