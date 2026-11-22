using System.Net.Http.Json;
using System.Text.Json;

namespace NexusMQ.Client;

/// <summary>
/// Entry point, similar to <c>ServiceBusClient</c>. Thread-safe; create one per application.
/// </summary>
public sealed class NexusClient : IAsyncDisposable, IDisposable
{
    private readonly bool _ownsHttp;
    internal HttpClient Http { get; }
    private HttpMessageInvoker? _sockets;
    /// <summary>Connects push WebSockets (racing IPv6/IPv4 addresses, see <see cref="NexusConnect"/>).</summary>
    internal HttpMessageInvoker WebSocketInvoker =>
        _sockets ??= new HttpMessageInvoker(NexusConnect.CreateHandler(Options.AllowUntrustedCertificates), disposeHandler: true);
    internal string? ApiKey { get; }
    internal NexusClientOptions Options { get; }

    public NexusAdminClient Admin { get; }

    /// <summary>Connect to a NexusMQ server.</summary>
    /// <param name="baseUrl">
    /// e.g. http://localhost:5680 or https://mq.example.com. For a primary/standby pair give both, comma-separated
    /// ("http://mq-a:5680,http://mq-b:5680"): the client moves to the next one when a server is unreachable or answers as a standby.
    /// </param>
    /// <param name="apiKey">Optional access key (sent as X-Api-Key).</param>
    /// <param name="options">Optional settings: timeouts, rate-limit retries, development certificates.</param>
    public NexusClient(string baseUrl, string? apiKey = null, NexusClientOptions? options = null)
        : this(CreateHttp(baseUrl, options ?? new NexusClientOptions()), apiKey, ownsHttp: true, options)
    {
        _endpoints.Clear();
        _endpoints.AddRange(ParseEndpoints(baseUrl));
    }

    /// <summary>Use an existing HttpClient (its BaseAddress must point at the server).</summary>
    public NexusClient(HttpClient httpClient, string? apiKey = null) : this(httpClient, apiKey, ownsHttp: false, null) { }

    private static HttpClient CreateHttp(string baseUrl, NexusClientOptions options)
    {
        var handler = NexusConnect.CreateHandler(options.AllowUntrustedCertificates);
        return new HttpClient(handler) { BaseAddress = ParseEndpoints(baseUrl)[0], Timeout = options.RequestTimeout };
    }

    private static List<Uri> ParseEndpoints(string urls)
    {
        var list = urls.Split([',', ';', ' '], StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries)
            .Select(u => new Uri(u.TrimEnd('/') + "/")).ToList();
        return list.Count > 0 ? list : throw new ArgumentException("Give at least one server URL.", nameof(urls));
    }

    private readonly List<Uri> _endpoints = [];
    private int _current;

    /// <summary>Server URLs this client knows, in failover order.</summary>
    public IReadOnlyList<Uri> Endpoints => _endpoints;

    /// <summary>The server currently in use.</summary>
    public Uri CurrentEndpoint => _endpoints[Volatile.Read(ref _current) % _endpoints.Count];

    /// <summary>A server told us who the leader is: switch to it if it is one of our endpoints. Returns true if we switched.</summary>
    internal bool FollowLeaderHint(string? leaderUrl)
    {
        if (string.IsNullOrEmpty(leaderUrl) || !Uri.TryCreate(leaderUrl.TrimEnd('/') + "/", UriKind.Absolute, out var hint)) return false;
        var i = _endpoints.FindIndex(e => Uri.Compare(e, hint, UriComponents.SchemeAndServer | UriComponents.Path, UriFormat.Unescaped, StringComparison.OrdinalIgnoreCase) == 0);
        if (i < 0) return false;
        Volatile.Write(ref _current, i);
        return true;
    }

    /// <summary>Move to the next server if <paramref name="failed"/> is still the current one.</summary>
    internal void MarkFailed(Uri failed)
    {
        var cur = Volatile.Read(ref _current);
        if (_endpoints.Count > 1 && _endpoints[cur % _endpoints.Count] == failed)
            Interlocked.CompareExchange(ref _current, (cur + 1) % _endpoints.Count, cur);
    }

    private NexusClient(HttpClient http, string? apiKey, bool ownsHttp, NexusClientOptions? options)
    {
        Http = http;
        _ownsHttp = ownsHttp;
        ApiKey = apiKey;
        Options = options ?? new NexusClientOptions();
        _endpoints.AddRange(http.BaseAddress is { } b ? [b] : throw new ArgumentException("The HttpClient needs a BaseAddress."));
        if (!string.IsNullOrEmpty(apiKey)) Http.DefaultRequestHeaders.Add("X-Api-Key", apiKey);
        Admin = new NexusAdminClient(this);
    }

    /// <summary>Sender for a queue ("orders"), topic ("events") or exchange ("exchanges/logs").</summary>
    public NexusSender CreateSender(string entityPath) => new(this, entityPath);

    /// <summary>Receiver for a queue, "topic/subscriptions/name", or a dead-letter path ending in "/$deadletter".</summary>
    public NexusReceiver CreateReceiver(string entityPath, NexusReceiveMode mode = NexusReceiveMode.PeekLock) => new(this, entityPath, mode);

    public NexusReceiver CreateReceiver(string topic, string subscription, NexusReceiveMode mode = NexusReceiveMode.PeekLock) =>
        new(this, $"{topic}/subscriptions/{subscription}", mode);

    public NexusProcessor CreateProcessor(string entityPath, NexusProcessorOptions? options = null) =>
        new(CreateReceiver(entityPath, options?.ReceiveMode ?? NexusReceiveMode.PeekLock), options ?? new NexusProcessorOptions());

    public NexusProcessor CreateProcessor(string topic, string subscription, NexusProcessorOptions? options = null) =>
        CreateProcessor($"{topic}/subscriptions/{subscription}", options);

    /// <summary>Lock the next session that has waiting messages (null if none arrived within <paramref name="maxWait"/>).</summary>
    public Task<NexusSessionReceiver?> AcceptNextSessionAsync(string entityPath, TimeSpan? maxWait = null,
        NexusReceiveMode mode = NexusReceiveMode.PeekLock, CancellationToken ct = default) =>
        NexusSessionReceiver.AcceptAsync(this, entityPath, null, maxWait, mode, ct);

    /// <summary>Lock a specific session. Throws <see cref="NexusClientException"/> (SessionCannotBeLocked) if another receiver holds it.</summary>
    public async Task<NexusSessionReceiver> AcceptSessionAsync(string entityPath, string sessionId,
        NexusReceiveMode mode = NexusReceiveMode.PeekLock, CancellationToken ct = default) =>
        (await NexusSessionReceiver.AcceptAsync(this, entityPath, sessionId, TimeSpan.Zero, mode, ct).ConfigureAwait(false))!;

    /// <summary>Process a session-enabled queue or "topic/subscriptions/name": parallel across sessions, ordered within each.</summary>
    public NexusSessionProcessor CreateSessionProcessor(string entityPath, NexusSessionProcessorOptions? options = null) =>
        new(this, entityPath, options ?? new NexusSessionProcessorOptions());

    internal static string Encode(string path) => string.Join('/', path.Split('/').Select(Uri.EscapeDataString));

    internal Task<T> SendAsync<T>(HttpMethod method, string url, object? body, CancellationToken ct) =>
        SendAsync<T>(method, url, body is null ? null : () => JsonContent.Create(body, body.GetType(), options: NexusJson.Options), ct);

    /// <summary>
    /// Servers that answered 415 to a binary send batch (older than 1.12) are sent JSON until this time (Stopwatch ticks).
    /// </summary>
    internal long BinarySendsRefusedUntil;

    /// <summary>Send with a body made fresh for each attempt (a retry or failover needs new content).</summary>
    internal async Task<T> SendAsync<T>(HttpMethod method, string url, Func<HttpContent>? content, CancellationToken ct)
    {
        var failovers = 0;
        var started = DateTimeOffset.UtcNow;
        var failoverWait = Options.FailoverWaitTime ?? (_endpoints.Count > 1 ? TimeSpan.FromSeconds(30) : TimeSpan.Zero);
        bool KeepWaiting() => DateTimeOffset.UtcNow - started < failoverWait && !ct.IsCancellationRequested;
        for (var attempt = 0; ; attempt++)
        {
            var endpoint = CurrentEndpoint;
            using var req = new HttpRequestMessage(method, new Uri(endpoint, url));
            if (content is not null) req.Content = content();
            HttpResponseMessage resp;
            try
            {
                resp = await Http.SendAsync(req, ct).ConfigureAwait(false);
            }
            catch (HttpRequestException) when (!ct.IsCancellationRequested && (++failovers < _endpoints.Count || KeepWaiting()))
            {
                MarkFailed(endpoint); // unreachable: try the next server
                if (failovers >= _endpoints.Count) await Task.Delay(TimeSpan.FromMilliseconds(500), ct).ConfigureAwait(false);
                continue;
            }
            using (resp)
            {
                if (!resp.IsSuccessStatusCode)
                {
                    string code = resp.StatusCode.ToString(), message = resp.ReasonPhrase ?? "Request failed";
                    try
                    {
                        var err = await resp.Content.ReadFromJsonAsync<JsonElement>(NexusJson.Options, ct).ConfigureAwait(false);
                        if (err.TryGetProperty("code", out var c)) code = c.GetString() ?? code;
                        if (err.TryGetProperty("message", out var m)) message = m.GetString() ?? message;
                    }
                    catch (JsonException) { }
                    catch (NotSupportedException) { }

                    if (code is "Standby" or "NotCommitted" && (++failovers < _endpoints.Count || KeepWaiting()))
                    {
                        // A standby/follower: go to the leader it names, else the next server (a failover may be under way).
                        var hinted = resp.Headers.TryGetValues("X-NexusMQ-Leader", out var h) && FollowLeaderHint(h.FirstOrDefault())
                                     && CurrentEndpoint != endpoint;
                        if (!hinted) MarkFailed(endpoint);
                        if (!hinted && failovers >= _endpoints.Count) await Task.Delay(TimeSpan.FromMilliseconds(500), ct).ConfigureAwait(false);
                        continue;
                    }
                    var retryAfter = resp.Headers.RetryAfter?.Delta;
                    if (code == "RateLimited" && attempt < Options.MaxRateLimitRetries)
                    {
                        await Task.Delay(retryAfter ?? TimeSpan.FromSeconds(1), ct).ConfigureAwait(false);
                        continue;
                    }
                    throw new NexusClientException(code, (int)resp.StatusCode, message) { RetryAfter = retryAfter };
                }
                if (typeof(T) == typeof(bool)) return (T)(object)true;
                if (resp.Content.Headers.ContentLength == 0) return default!;
                return (await resp.Content.ReadFromJsonAsync<T>(NexusJson.Options, ct).ConfigureAwait(false))!;
            }
        }
    }

    public void Dispose()
    {
        if (_ownsHttp) Http.Dispose();
        _sockets?.Dispose();
    }

    public ValueTask DisposeAsync()
    {
        Dispose();
        return ValueTask.CompletedTask;
    }
}
