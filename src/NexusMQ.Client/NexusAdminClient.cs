namespace NexusMQ.Client;

/// <summary>Create and manage entities (like <c>ServiceBusAdministrationClient</c> / RabbitMQ declare calls). All create calls are idempotent.</summary>
public sealed class NexusAdminClient
{
    private readonly NexusClient _client;
    internal NexusAdminClient(NexusClient client) => _client = client;

    private static string E(string s) => Uri.EscapeDataString(s);

    public Task CreateQueueAsync(string name, NexusQueueOptions? options = null, CancellationToken ct = default) =>
        _client.SendAsync<bool>(HttpMethod.Put, $"api/queues/{E(name)}", options ?? new NexusQueueOptions(), ct);

    public Task DeleteQueueAsync(string name, CancellationToken ct = default) =>
        _client.SendAsync<bool>(HttpMethod.Delete, $"api/queues/{E(name)}", null, ct);

    public Task CreateTopicAsync(string name, TimeSpan? duplicateDetectionWindow = null, CancellationToken ct = default) =>
        _client.SendAsync<bool>(HttpMethod.Put, $"api/topics/{E(name)}", new { DuplicateDetectionWindow = duplicateDetectionWindow }, ct);

    public Task DeleteTopicAsync(string name, CancellationToken ct = default) =>
        _client.SendAsync<bool>(HttpMethod.Delete, $"api/topics/{E(name)}", null, ct);

    /// <summary>Create or update a subscription. A message is delivered when any rule matches; no rules = receive everything.</summary>
    public Task CreateSubscriptionAsync(string topic, string name, IEnumerable<NexusSubscriptionRule>? rules = null,
        NexusQueueOptions? options = null, CancellationToken ct = default) =>
        _client.SendAsync<bool>(HttpMethod.Put, $"api/topics/{E(topic)}/subscriptions/{E(name)}",
            new { Options = options, Rules = rules?.ToList() }, ct);

    public Task DeleteSubscriptionAsync(string topic, string name, CancellationToken ct = default) =>
        _client.SendAsync<bool>(HttpMethod.Delete, $"api/topics/{E(topic)}/subscriptions/{E(name)}", null, ct);

    public Task CreateExchangeAsync(string name, NexusExchangeType type, CancellationToken ct = default) =>
        _client.SendAsync<bool>(HttpMethod.Put, $"api/exchanges/{E(name)}", new { Type = type }, ct);

    public Task DeleteExchangeAsync(string name, CancellationToken ct = default) =>
        _client.SendAsync<bool>(HttpMethod.Delete, $"api/exchanges/{E(name)}", null, ct);

    /// <summary>Bind an exchange to a queue, topic, or "exchanges/{other}".</summary>
    public Task BindAsync(string exchange, string destination, string routingKey = "", Dictionary<string, string>? arguments = null, CancellationToken ct = default) =>
        _client.SendAsync<bool>(HttpMethod.Post, "api/bindings",
            new NexusBinding { Exchange = exchange, Destination = destination, RoutingKey = routingKey, Arguments = arguments }, ct);

    public Task UnbindAsync(string exchange, string destination, string routingKey = "", CancellationToken ct = default) =>
        _client.SendAsync<bool>(HttpMethod.Post, "api/bindings/remove",
            new NexusBinding { Exchange = exchange, Destination = destination, RoutingKey = routingKey }, ct);

    public Task<List<NexusEntityStats>> GetStatsAsync(CancellationToken ct = default) =>
        _client.SendAsync<List<NexusEntityStats>>(HttpMethod.Get, "api/stats", null, ct);

    public async Task<int> PurgeAsync(string entityPath, CancellationToken ct = default) =>
        (await _client.SendAsync<Dictionary<string, int>>(HttpMethod.Delete, $"api/messages/{NexusClient.Encode(entityPath)}", null, ct)
            .ConfigureAwait(false))["purged"];

    /// <summary>Move all dead-lettered messages back to their source entity.</summary>
    public async Task<int> ResubmitDeadLettersAsync(string entityPath, CancellationToken ct = default) =>
        (await _client.SendAsync<Dictionary<string, int>>(HttpMethod.Post, $"api/resubmit/{NexusClient.Encode(entityPath)}", null, ct)
            .ConfigureAwait(false))["resubmitted"];

    /// <summary>The whole topology (queues, topics, subscriptions, exchanges, bindings) as JSON.</summary>
    public Task<System.Text.Json.JsonElement> GetTopologyAsync(CancellationToken ct = default) =>
        _client.SendAsync<System.Text.Json.JsonElement>(HttpMethod.Get, "api/topology", null, ct);

    /// <summary>Prometheus metrics text.</summary>
    public async Task<string> GetMetricsTextAsync(CancellationToken ct = default)
    {
        using var resp = await _client.Http.GetAsync(new Uri(_client.CurrentEndpoint, "api/metrics"), ct).ConfigureAwait(false);
        var text = await resp.Content.ReadAsStringAsync(ct).ConfigureAwait(false);
        if (!resp.IsSuccessStatusCode) throw new NexusClientException(resp.StatusCode.ToString(), (int)resp.StatusCode, text);
        return text;
    }

    /// <summary>Replication status of the current server (role, position, connected standbys).</summary>
    public Task<System.Text.Json.JsonElement> GetReplicationStatusAsync(CancellationToken ct = default) =>
        _client.SendAsync<System.Text.Json.JsonElement>(HttpMethod.Get, "api/replication/status", null, ct);

    // ------------------------------------------------------------------ cluster membership (Raft mode, on the leader)

    /// <summary>Current voting members, learners being added, and the leader id.</summary>
    public Task<System.Text.Json.JsonElement> GetClusterMembersAsync(CancellationToken ct = default) =>
        _client.SendAsync<System.Text.Json.JsonElement>(HttpMethod.Get, "api/cluster/members", null, ct);

    /// <summary>
    /// Add a server to the cluster. Start it first with <c>NexusMQ:Cluster:Join=true</c> and the cluster secret; it catches up
    /// as a non-voting learner, then becomes a voting member. Returns when the change is committed.
    /// </summary>
    public Task<System.Text.Json.JsonElement> AddClusterMemberAsync(string id, string url, CancellationToken ct = default) =>
        _client.SendAsync<System.Text.Json.JsonElement>(HttpMethod.Post, "api/cluster/members", new { Id = id, Url = url }, ct);

    /// <summary>Remove a server from the cluster (it can then be shut down). Returns when the change is committed.</summary>
    public Task<System.Text.Json.JsonElement> RemoveClusterMemberAsync(string id, CancellationToken ct = default) =>
        _client.SendAsync<System.Text.Json.JsonElement>(HttpMethod.Delete, $"api/cluster/members/{E(id)}", null, ct);

    // ------------------------------------------------------------------ access keys (needs a broker-wide Manage key)

    public Task<List<NexusAccessKeyInfo>> ListKeysAsync(CancellationToken ct = default) =>
        _client.SendAsync<List<NexusAccessKeyInfo>>(HttpMethod.Get, "api/keys", null, ct);

    /// <summary>Create a key. The returned secret is shown only once. The first key on an open broker must include Manage.</summary>
    public Task<NexusKeySecret> CreateKeyAsync(string name, NexusKeyRights rights, IEnumerable<string>? entities = null, CancellationToken ct = default) =>
        CreateKeyAsync(name, rights, entities, null, null, ct);

    /// <summary>Create a key with a request limit (<paramref name="requestsPerSecond"/> sustained, <paramref name="burst"/> at once).</summary>
    public Task<NexusKeySecret> CreateKeyAsync(string name, NexusKeyRights rights, IEnumerable<string>? entities,
        double? requestsPerSecond, int? burst = null, CancellationToken ct = default) =>
        _client.SendAsync<NexusKeySecret>(HttpMethod.Post, "api/keys",
            new { Name = name, Rights = rights, Entities = entities?.ToList(), RequestsPerSecond = requestsPerSecond, Burst = burst }, ct);

    /// <summary>Change a runtime key's request limit; nulls remove it.</summary>
    public Task<NexusAccessKeyInfo> SetKeyLimitsAsync(string name, double? requestsPerSecond, int? burst = null, CancellationToken ct = default) =>
        _client.SendAsync<NexusAccessKeyInfo>(HttpMethod.Post, $"api/keys/{E(name)}/limits", new { RequestsPerSecond = requestsPerSecond, Burst = burst }, ct);

    /// <summary>Recent management actions and denied requests, newest first.</summary>
    public Task<List<NexusAuditEntry>> GetAuditAsync(int max = 100, string? actor = null, string? result = null, CancellationToken ct = default)
    {
        var url = $"api/audit?max={max}";
        if (actor is not null) url += $"&actor={E(actor)}";
        if (result is not null) url += $"&result={E(result)}";
        return _client.SendAsync<List<NexusAuditEntry>>(HttpMethod.Get, url, null, ct);
    }

    // ------------------------------------------------------------------ single messages

    /// <summary>Delete one message by sequence number (it must not be locked by a receiver).</summary>
    public Task DeleteMessageAsync(string entityPath, long sequenceNumber, CancellationToken ct = default) =>
        _client.SendAsync<bool>(HttpMethod.Delete, $"api/messages/{NexusClient.Encode(entityPath)}?seq={sequenceNumber}", null, ct);

    /// <summary>
    /// Send one message again. From a dead-letter queue the default moves it back to where it failed; elsewhere it is duplicated.
    /// <paramref name="destination"/> can be another queue, subscription, topic or "exchanges/name".
    /// </summary>
    public async Task<int> ResendMessageAsync(string entityPath, long sequenceNumber, string? destination = null, bool? removeOriginal = null, CancellationToken ct = default)
    {
        var url = $"api/messages/resend/{NexusClient.Encode(entityPath)}?seq={sequenceNumber}";
        if (destination is not null) url += $"&to={E(destination)}";
        if (removeOriginal is not null) url += $"&move={(removeOriginal.Value ? "true" : "false")}";
        return (await _client.SendAsync<Dictionary<string, int>>(HttpMethod.Post, url, null, ct).ConfigureAwait(false))["delivered"];
    }

    /// <summary>Issue a new secret. During <paramref name="gracePeriod"/> the old secret keeps working.</summary>
    public Task<NexusKeySecret> RotateKeyAsync(string name, TimeSpan? gracePeriod = null, CancellationToken ct = default) =>
        _client.SendAsync<NexusKeySecret>(HttpMethod.Post,
            $"api/keys/{E(name)}/rotate?graceSeconds={(int)(gracePeriod ?? TimeSpan.Zero).TotalSeconds}", null, ct);

    public Task DeleteKeyAsync(string name, CancellationToken ct = default) =>
        _client.SendAsync<bool>(HttpMethod.Delete, $"api/keys/{E(name)}", null, ct);
}
