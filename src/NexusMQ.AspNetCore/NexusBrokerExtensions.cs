using System.Reflection;
using System.Text.Json.Serialization;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Http.Json;
using Microsoft.AspNetCore.Routing;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Options;
using NexusMQ.Core;
using NexusMQ.Core.Persistence;

namespace NexusMQ.AspNetCore;

/// <summary>Host the NexusMQ broker inside any ASP.NET Core app.</summary>
public static class NexusBrokerExtensions
{
    /// <summary>
    /// Register the broker (a singleton <see cref="Broker"/>) and its storage.
    /// Inject <see cref="Broker"/> directly for in-process messaging, and call <see cref="MapNexusMQ"/> to expose the HTTP API.
    /// </summary>
    public static IServiceCollection AddNexusMQBroker(this IServiceCollection services, Action<NexusServerOptions>? configure = null)
    {
        var options = services.AddOptions<NexusServerOptions>();
        if (configure is not null) options.Configure(configure);

        services.Configure<JsonOptions>(o =>
        {
            if (!o.SerializerOptions.Converters.OfType<JsonStringEnumConverter>().Any())
                o.SerializerOptions.Converters.Add(new JsonStringEnumConverter());
        });

        services.AddSingleton<IMessageStore>(sp =>
        {
            var opts = sp.GetRequiredService<IOptions<NexusServerOptions>>().Value;
            if (!opts.Durable) return new InMemoryMessageStore();
            return new FileMessageStore(DataDirectory(sp, opts), opts.FlushToDisk, format: opts.StorageFormat);
        });
        // The node owns the broker: created at once on a primary, only on promotion on a standby.
        services.AddSingleton(sp =>
        {
            var opts = sp.GetRequiredService<IOptions<NexusServerOptions>>().Value;
            var log = sp.GetRequiredService<Microsoft.Extensions.Logging.ILogger<NexusNode>>();
            if (opts.Durable) WarnIfDataIsWatched(sp, DataDirectory(sp, opts), log);
            return new NexusNode(opts, sp.GetRequiredService<IMessageStore>(), sp.GetRequiredService<NexusAuthorizer>(),
                log, opts.Durable ? DataDirectory(sp, opts) : null);
        });
        // On a standby this throws (not cached) until promotion; the standby guard answers 503 before endpoints run.
        services.AddSingleton(sp => sp.GetRequiredService<NexusNode>().Broker);
        services.AddHostedService<NexusStandbyService>();
        services.AddHostedService<NexusShutdownService>();
        services.AddSingleton(sp =>
        {
            var opts = sp.GetRequiredService<IOptions<NexusServerOptions>>().Value;
            var keyFile = opts.Durable ? Path.Combine(DataDirectory(sp, opts), "access-keys.json") : null;
            return new NexusAuthorizer(opts, keyFile);
        });
        services.AddSingleton(sp =>
        {
            var opts = sp.GetRequiredService<IOptions<NexusServerOptions>>().Value;
            return new NexusAuditLog(opts.Durable ? Path.Combine(DataDirectory(sp, opts), "audit.log") : null);
        });
        return services;
    }

    /// <summary>
    /// appsettings reload watches the whole content root (subfolders included). A data folder inside it makes every journal
    /// write wake that watcher, which costs a busy broker a large share of its CPU.
    /// </summary>
    private static void WarnIfDataIsWatched(IServiceProvider sp, string dataDir, Microsoft.Extensions.Logging.ILogger log)
    {
        var root = sp.GetService<IHostEnvironment>()?.ContentRootPath;
        var config = sp.GetService<IConfiguration>();
        if (root is null || config is null || !config.GetValue("hostBuilder:reloadConfigOnChange", true)) return;
        var full = Path.GetFullPath(dataDir).TrimEnd(Path.DirectorySeparatorChar) + Path.DirectorySeparatorChar;
        var rootFull = Path.GetFullPath(root).TrimEnd(Path.DirectorySeparatorChar) + Path.DirectorySeparatorChar;
        if (!full.StartsWith(rootFull, OperatingSystem.IsWindows() ? StringComparison.OrdinalIgnoreCase : StringComparison.Ordinal)) return;
        Microsoft.Extensions.Logging.LoggerExtensions.LogWarning(log,
            "NexusMQ data folder {DataDirectory} is inside the content root, which the appsettings reload watcher monitors: every message " +
            "write wakes it and slows the broker. Set NexusMQ:DataDirectory to a folder outside {ContentRoot}, or turn reloading off " +
            "(WebApplication.CreateBuilder([\"--hostBuilder:reloadConfigOnChange=false\", .. args])).", full, rootFull);
    }

    private static string DataDirectory(IServiceProvider sp, NexusServerOptions opts)
    {
        var root = sp.GetService<IHostEnvironment>()?.ContentRootPath ?? AppContext.BaseDirectory;
        return Path.IsPathRooted(opts.DataDirectory) ? opts.DataDirectory : Path.Combine(root, opts.DataDirectory);
    }

    /// <summary>Register the broker, binding options from a configuration section (e.g. <c>builder.Configuration.GetSection("NexusMQ")</c>).</summary>
    public static IServiceCollection AddNexusMQBroker(this IServiceCollection services, IConfiguration section)
    {
        services.Configure<NexusServerOptions>(section);
        return services.AddNexusMQBroker();
    }

    /// <summary>
    /// Map the NexusMQ HTTP API under <paramref name="prefix"/> (default <c>/api</c>), including the WebSocket push
    /// endpoint at <c>{prefix}/ws</c> and Prometheus metrics at <c>{prefix}/metrics</c>.
    /// For a <see cref="WebApplication"/> this also enables the WebSocket middleware.
    /// </summary>
    public static RouteGroupBuilder MapNexusMQ(this IEndpointRouteBuilder endpoints, string prefix = "/api")
    {
        var apiRoot = "/" + prefix.Trim('/');
        if (endpoints is IApplicationBuilder app && !app.Properties.ContainsKey("NexusMQ.WebSockets"))
        {
            app.Properties["NexusMQ.WebSockets"] = true;
            app.UseWebSockets(new WebSocketOptions { KeepAliveInterval = TimeSpan.FromSeconds(20) });

            // Standby guard: refuse client traffic with 503 (clients with several URLs move on to the primary).
            app.Use(async (http, next) =>
            {
                var node = http.RequestServices.GetRequiredService<NexusNode>();
                var path = http.Request.Path;
                if (node.Role == NexusRole.Standby && path.StartsWithSegments(apiRoot, out var rest)
                    && !rest.StartsWithSegments("/health") && !rest.StartsWithSegments("/replication") && !rest.StartsWithSegments("/raft"))
                {
                    var leader = node.LeaderUrl;
                    http.Response.StatusCode = StatusCodes.Status503ServiceUnavailable;
                    http.Response.Headers["X-NexusMQ-Role"] = node.RoleName.ToLowerInvariant();
                    if (leader is not null) http.Response.Headers["X-NexusMQ-Leader"] = leader;
                    await http.Response.WriteAsJsonAsync(new ErrorResponse
                    {
                        Code = nameof(NexusErrorCode.Standby),
                        Message = node.IsCluster
                            ? $"This node is a cluster follower (leader: {leader ?? "being elected"}). Send requests to the leader."
                            : $"This node is a standby (primary: {node.Options.PrimaryUrl}). Send requests to the primary."
                    });
                    return;
                }
                await next();
            });
        }

        endpoints.Map(apiRoot + "/replication/stream", NexusReplicationStream.HandleAsync);
        NexusClusterEndpoints.Map(endpoints, apiRoot);

        // Push protocol (checks keys per operation; browsers cannot set headers on WebSockets, so ?apiKey= works too).
        endpoints.Map(prefix.TrimEnd('/') + "/ws", NexusWebSocketHandler.HandleAsync);

        var api = endpoints.MapGroup(prefix);
        var routePrefix = "/" + prefix.Trim('/');
        api.AddEndpointFilter(async (ctx, next) =>
        {
            var http = ctx.HttpContext;
            var auth = http.RequestServices.GetRequiredService<NexusAuthorizer>();
            var audit = http.RequestServices.GetRequiredService<NexusAuditLog>();
            var raw = (http.GetEndpoint() as RouteEndpoint)?.RoutePattern.RawText ?? "";
            var route = raw.StartsWith(routePrefix, StringComparison.OrdinalIgnoreCase) ? raw[routePrefix.Length..] : raw;
            if (!route.StartsWith('/')) route = "/" + route;
            var (right, entity) = NexusAuthorizer.RequirementFor(route, http.Request.RouteValues, ctx.Arguments);
            var action = $"{http.Request.Method} {route}";
            var audited = right == NexusRights.Manage && !HttpMethods.IsGet(http.Request.Method);
            var ip = http.Connection.RemoteIpAddress?.ToString();
            IReadOnlyList<NexusAccessKey>? caller = null;
            var node = http.RequestServices.GetRequiredService<NexusNode>();
            var writes = node.IsCluster ? NexusMQ.Core.Cluster.RaftStore.Track() : null;
            try
            {
                if (!auth.IsOpen)
                {
                    caller = auth.Authenticate(NexusAuthorizer.PresentedKey(http));
                    auth.Demand(caller, right, entity);
                    auth.CheckRate(caller);
                }
                var actor = auth.Actor(caller); // before the call: creating the first key changes "open" to "secured"
                var result = await next(ctx);
                // Cluster: answer only once a majority has the changes this request made.
                if (writes is { Index: > 0 } && !await node.Raft!.WaitCommittedAsync(writes.Index, writes.Term, node.CommitTimeout, http.RequestAborted))
                    throw new NexusException(NexusErrorCode.NotCommitted,
                        "The cluster did not confirm this change (leader lost or no majority). It may or may not have been applied; retry.");
                if (audited) audit.Record(actor, action, AuditTarget(entity, http, ctx.Arguments), "ok", AuditDetail(ctx.Arguments) ?? QueryDetail(http), ip);
                return result;
            }
            catch (NexusException ex)
            {
                if (ex.Code is NexusErrorCode.Unauthorized or NexusErrorCode.Forbidden)
                    audit.Record(auth.Actor(caller), action, AuditTarget(entity, http, ctx.Arguments), "denied", ex.Message, ip);
                else if (audited)
                    audit.Record(auth.Actor(caller), action, AuditTarget(entity, http, ctx.Arguments), "error:" + ex.Code, ex.Message, ip);
                if (ex.RetryAfter is { } retry)
                    http.Response.Headers.RetryAfter = Math.Max(1, (int)Math.Ceiling(retry.TotalSeconds)).ToString(System.Globalization.CultureInfo.InvariantCulture);
                return Error(ex);
            }
            catch (FormatException ex)
            {
                return Error(new NexusException(NexusErrorCode.BadRequest, ex.Message));
            }
        });

        MapManagement(api);
        MapMessaging(api);
        MapSessions(api);
        MapKeys(api);
        api.MapGet("/health", (NexusNode node) => Results.Ok(new { status = "ok", role = node.RoleName, leader = node.LeaderUrl }));
        api.MapGet("/replication/status", (NexusNode node) => node.Status());

        // Cluster membership (on the leader; followers redirect clients with 503 + leader hint).
        api.MapGet("/cluster/members", (NexusNode node) => node.Raft?.Status() is { } st
            ? Results.Ok(new { members = st.Members, learners = st.Learners, leader = st.LeaderId })
            : Results.BadRequest(new ErrorResponse { Code = "BadRequest", Message = "This server is not running in cluster mode." }));
        api.MapPost("/cluster/members", async (NexusNode node, Core.Cluster.ClusterMember member, HttpContext http) =>
        {
            if (node.Raft is null) throw new NexusException(NexusErrorCode.BadRequest, "This server is not running in cluster mode.");
            if (string.IsNullOrWhiteSpace(member.Id) || !Uri.TryCreate(member.Url, UriKind.Absolute, out _))
                throw new NexusException(NexusErrorCode.BadRequest, "Give the new server an id and an absolute url.");
            await node.Raft.AddServerAsync(member, http.RequestAborted);
            return Results.Ok(node.Raft.Status().Members);
        });
        api.MapDelete("/cluster/members/{id}", async (NexusNode node, string id, HttpContext http) =>
        {
            if (node.Raft is null) throw new NexusException(NexusErrorCode.BadRequest, "This server is not running in cluster mode.");
            await node.Raft.RemoveServerAsync(id, http.RequestAborted);
            return Results.Ok(node.Raft.Status().Members);
        });
        // Leadership transfer: hand the leader role to another server (all of them if none is named: the most up to date).
        api.MapPost("/cluster/leader/transfer", async (NexusNode node, string? to, HttpContext http) =>
        {
            if (node.Raft is null) throw new NexusException(NexusErrorCode.BadRequest, "This server is not running in cluster mode.");
            var leader = await node.Raft.TransferLeadershipAsync(string.IsNullOrWhiteSpace(to) ? null : to, http.RequestAborted);
            var url = node.Raft.Status().Members.FirstOrDefault(m => m.Id == leader)?.Url;
            return Results.Ok(new { leader, url });
        });
        api.MapPost("/replication/promote", (NexusNode node) =>
        {
            node.Promote();
            return node.Status();
        });
        api.MapGet("/metrics", (Broker b) => Results.Text(PrometheusExporter.Render(b), "text/plain; version=0.0.4; charset=utf-8"));
        return api;
    }

    /// <summary>Serve the web dashboard at <paramref name="pattern"/>, talking to the API mapped at <paramref name="apiPrefix"/>.</summary>
    public static IEndpointConventionBuilder MapNexusMQDashboard(this IEndpointRouteBuilder endpoints, string pattern = "/nexusmq", string apiPrefix = "/api")
    {
        string? html = null;
        return endpoints.MapGet(pattern, () =>
        {
            html ??= LoadDashboard().Replace("__NEXUSMQ_API__", apiPrefix.TrimEnd('/'));
            return Results.Content(html, "text/html; charset=utf-8");
        });
    }

    private static string LoadDashboard()
    {
        using var stream = typeof(NexusBrokerExtensions).Assembly.GetManifestResourceStream("NexusMQ.Dashboard.index.html")
                           ?? throw new InvalidOperationException("Dashboard resource missing.");
        using var reader = new StreamReader(stream);
        return reader.ReadToEnd();
    }

    private static string? AuditTarget(string? entity, HttpContext http, IList<object?> args) =>
        entity
        ?? http.Request.RouteValues["name"]?.ToString()
        ?? args.OfType<CreateKeyRequest>().FirstOrDefault()?.Name;

    /// <summary>Query parameters (e.g. seq=12&amp;to=archive) for the audit log, without any apiKey.</summary>
    private static string? QueryDetail(HttpContext http)
    {
        var parts = http.Request.Query.Where(q => !q.Key.Equals("apiKey", StringComparison.OrdinalIgnoreCase))
            .Select(q => $"{q.Key}={q.Value}").ToList();
        return parts.Count == 0 ? null : string.Join("&", parts);
    }

    /// <summary>Extra context for the audit log. Never includes secrets or message bodies.</summary>
    private static string? AuditDetail(IList<object?> args)
    {
        if (args.OfType<CreateKeyRequest>().FirstOrDefault() is { } k)
            return $"rights={k.Rights}; entities={(k.Entities is { Count: > 0 } e ? string.Join(",", e) : "*")}"
                   + (k.RequestsPerSecond is { } r ? $"; rps={r}" : "");
        if (args.OfType<BindingDefinition>().FirstOrDefault() is { } b) return $"{b.Exchange} --[{b.RoutingKey}]--> {b.Destination}";
        if (args.OfType<QueueOptions>().FirstOrDefault() is { } q)
            return $"maxDelivery={q.MaxDeliveryCount}; lock={q.LockDuration}; session={q.RequiresSession}"
                   + (q.ForwardTo is not null ? $"; forwardTo={q.ForwardTo}" : "");
        return null;
    }

    private static IResult Error(NexusException ex) => Results.Json(
        new ErrorResponse { Code = ex.Code.ToString(), Message = ex.Message },
        statusCode: ex.Code switch
        {
            NexusErrorCode.BadRequest => StatusCodes.Status400BadRequest,
            NexusErrorCode.Unauthorized => StatusCodes.Status401Unauthorized,
            NexusErrorCode.Forbidden => StatusCodes.Status403Forbidden,
            NexusErrorCode.EntityNotFound or NexusErrorCode.MessageNotFound => StatusCodes.Status404NotFound,
            NexusErrorCode.EntityAlreadyExists or NexusErrorCode.SessionCannotBeLocked => StatusCodes.Status409Conflict,
            NexusErrorCode.MessageLockLost or NexusErrorCode.SessionLockLost => StatusCodes.Status410Gone,
            NexusErrorCode.QuotaExceeded or NexusErrorCode.RateLimited => StatusCodes.Status429TooManyRequests,
            NexusErrorCode.Standby or NexusErrorCode.NotCommitted => StatusCodes.Status503ServiceUnavailable,
            _ => StatusCodes.Status500InternalServerError
        });

    private static ReceiveMode ParseMode(string? mode)
    {
        if (string.IsNullOrEmpty(mode)) return ReceiveMode.PeekLock;
        return Enum.TryParse<ReceiveMode>(mode, ignoreCase: true, out var m)
            ? m
            : throw new NexusException(NexusErrorCode.BadRequest, $"Unknown receive mode '{mode}'. Use PeekLock or ReceiveAndDelete.");
    }

    private static TimeSpan Wait(int? seconds, IOptions<NexusServerOptions> opts) =>
        TimeSpan.FromSeconds(Math.Clamp(seconds ?? 0, 0, opts.Value.MaxWaitSeconds));

    // ------------------------------------------------------------------ management

    private static void MapManagement(RouteGroupBuilder api)
    {
        api.MapGet("/topology", (Broker b) => b.GetTopology());
        api.MapGet("/stats", (Broker b) => b.GetStats());

        api.MapPut("/queues/{name}", (Broker b, string name, QueueOptions? options) => b.CreateQueue(name, options));
        api.MapDelete("/queues/{name}", (Broker b, string name) => b.DeleteQueue(name) ? Results.NoContent() : Results.NotFound());

        api.MapPut("/topics/{name}", (Broker b, string name, CreateTopicRequest? req) => b.CreateTopic(name, req?.DuplicateDetectionWindow));
        api.MapDelete("/topics/{name}", (Broker b, string name) => b.DeleteTopic(name) ? Results.NoContent() : Results.NotFound());

        api.MapPut("/topics/{topic}/subscriptions/{name}", (Broker b, string topic, string name, CreateSubscriptionRequest? req) =>
            b.CreateSubscription(topic, name, req?.Options, req?.Rules));
        api.MapDelete("/topics/{topic}/subscriptions/{name}", (Broker b, string topic, string name) =>
            b.DeleteSubscription(topic, name) ? Results.NoContent() : Results.NotFound());

        api.MapPut("/exchanges/{name}", (Broker b, string name, CreateExchangeRequest? req) => b.CreateExchange(name, req?.Type ?? ExchangeType.Direct));
        api.MapDelete("/exchanges/{name}", (Broker b, string name) => b.DeleteExchange(name) ? Results.NoContent() : Results.NotFound());

        api.MapPost("/bindings", (Broker b, BindingDefinition binding) => b.Bind(binding));
        api.MapPost("/bindings/remove", (Broker b, BindingDefinition binding) => b.Unbind(binding) ? Results.NoContent() : Results.NotFound());
    }

    // ------------------------------------------------------------------ messaging

    private static void MapMessaging(RouteGroupBuilder api)
    {
        // Send to a queue ("orders"), topic ("events") or exchange ("exchanges/logs").
        // JSON (bodies as base64 or text) or, from 1.12 clients, a binary batch with raw bodies (see NexusBinaryBatch).
        api.MapPost("/send/{**path}", async (Broker b, HttpContext http, string path, string? routingKey) =>
        {
            var messages = await NexusBinaryBatch.ReadAsync(http.Request);
            if (messages.Count == 0) throw new NexusException(NexusErrorCode.BadRequest, "Send at least one message.");
            return b.Send(path, messages.Select(m => m.ToOutgoing()).ToList(), routingKey);
        });

        // Long-poll receive: waits up to `wait` seconds for messages.
        api.MapPost("/receive/{**path}", async (Broker b, IOptions<NexusServerOptions> opts, HttpContext ctx,
            string path, int? max, int? wait, string? mode) =>
        {
            var received = await b.ReceiveAsync(path, Math.Clamp(max ?? 1, 1, 1000), Wait(wait, opts), ParseMode(mode), ctx.RequestAborted);
            return received.Select(MessageResponse.From).ToList();
        });

        api.MapPost("/settle/{**path}", (Broker b, string path, SettleRequest req) =>
        {
            switch (req.Action)
            {
                case SettleAction.Complete: b.Complete(path, req.LockToken); return new SettleResponse();
                case SettleAction.Abandon: b.Abandon(path, req.LockToken, req.Properties); return new SettleResponse();
                case SettleAction.DeadLetter: b.DeadLetter(path, req.LockToken, req.Reason, req.Description, req.Properties); return new SettleResponse();
                case SettleAction.Defer: return new SettleResponse { SequenceNumber = b.Defer(path, req.LockToken) };
                case SettleAction.Renew: return new SettleResponse { LockedUntilUtc = b.RenewLock(path, req.LockToken) };
                default: throw new NexusException(NexusErrorCode.BadRequest, $"Unknown action '{req.Action}'.");
            }
        });

        api.MapPost("/deferred/{**path}", (Broker b, string path, List<long> sequenceNumbers) =>
            b.ReceiveDeferred(path, sequenceNumbers).Select(MessageResponse.From).ToList());

        api.MapGet("/peek/{**path}", (Broker b, string path, long? from, int? max) =>
            b.Peek(path, from ?? 0, Math.Clamp(max ?? 50, 1, 1000)).Select(m => MessageResponse.From(m)).ToList());

        // Purge the entity, or with ?seq= delete a single message.
        api.MapDelete("/messages/{**path}", (Broker b, string path, long? seq) =>
        {
            if (seq is null) return Results.Ok(new { purged = b.Purge(path) });
            b.DeleteMessage(path, seq.Value);
            return Results.Ok(new { deleted = seq.Value });
        });

        // Send one message again: ?seq=N [&to=destination] [&move=true|false].
        // From a dead-letter queue the default is "move back to where it failed"; elsewhere "duplicate in place".
        api.MapPost("/messages/resend/{**path}", (Broker b, NexusAuthorizer a, HttpContext h, string path, long seq, string? to, bool? move) =>
        {
            if (!string.IsNullOrEmpty(to)) a.Demand(a.Authenticate(NexusAuthorizer.PresentedKey(h)), NexusRights.Manage, to);
            return new { delivered = b.ResendMessage(path, seq, string.IsNullOrEmpty(to) ? null : to, move) };
        });

        api.MapDelete("/scheduled/{queue}/{sequenceNumber:long}", (Broker b, string queue, long sequenceNumber) =>
            b.CancelScheduled(queue, sequenceNumber) ? Results.NoContent() : Results.NotFound());

        api.MapPost("/resubmit/{**path}", (Broker b, string path) => new { resubmitted = b.ResubmitDeadLetters(path) });
    }

    // ------------------------------------------------------------------ access keys

    private static void MapKeys(RouteGroupBuilder api)
    {
        static IReadOnlyList<NexusAccessKey>? Caller(NexusAuthorizer a, HttpContext h) => a.Authenticate(NexusAuthorizer.PresentedKey(h));

        api.MapGet("/keys", (NexusAuthorizer a) => a.ListKeys());

        api.MapPost("/keys", (NexusAuthorizer a, HttpContext h, CreateKeyRequest req) =>
        {
            var (key, secret) = a.CreateKey(Caller(a, h), req.Name, req.Rights, req.Entities, req.RequestsPerSecond, req.Burst);
            return new KeySecretResponse { Key = key, Secret = secret };
        });

        // ?graceSeconds=300 keeps the old secret valid for 5 minutes so apps can switch without downtime.
        api.MapPost("/keys/{name}/rotate", (NexusAuthorizer a, HttpContext h, string name, int? graceSeconds) =>
        {
            var (key, secret) = a.RotateKey(Caller(a, h), name, TimeSpan.FromSeconds(Math.Clamp(graceSeconds ?? 0, 0, 30 * 24 * 3600)));
            return new KeySecretResponse { Key = key, Secret = secret };
        });

        api.MapPost("/keys/{name}/limits", (NexusAuthorizer a, HttpContext h, string name, KeyLimitsRequest req) =>
            a.SetLimits(Caller(a, h), name, req.RequestsPerSecond, req.Burst));

        api.MapGet("/audit", (NexusAuditLog log, int? max, string? actor, string? result) => log.Recent(max ?? 100, actor, result));

        api.MapDelete("/keys/{name}", (NexusAuthorizer a, HttpContext h, string name) =>
        {
            a.DeleteKey(Caller(a, h), name);
            return Results.NoContent();
        });
    }

    // ------------------------------------------------------------------ sessions

    private static void MapSessions(RouteGroupBuilder api)
    {
        // Lock a session: a specific one (?sessionId=) or the next available one. 204 when none arrived within `wait`.
        api.MapPost("/sessions/accept/{**path}", async (Broker b, IOptions<NexusServerOptions> opts, HttpContext ctx,
            string path, string? sessionId, int? wait) =>
        {
            var handle = await b.AcceptSessionAsync(path, string.IsNullOrEmpty(sessionId) ? null : sessionId, Wait(wait, opts), ctx.RequestAborted);
            return handle is null ? Results.NoContent() : Results.Ok(handle);
        });

        api.MapPost("/sessions/receive/{**path}", async (Broker b, IOptions<NexusServerOptions> opts, HttpContext ctx,
            string path, string sessionId, Guid token, int? max, int? wait, string? mode) =>
        {
            var received = await b.ReceiveFromSessionAsync(path, sessionId, token, Math.Clamp(max ?? 1, 1, 1000),
                Wait(wait, opts), ParseMode(mode), ctx.RequestAborted);
            return received.Select(MessageResponse.From).ToList();
        });

        api.MapPost("/sessions/renew/{**path}", (Broker b, string path, SessionRequest req) =>
            new SettleResponse { LockedUntilUtc = b.RenewSessionLock(path, req.SessionId, req.SessionLockToken) });

        api.MapPost("/sessions/release/{**path}", (Broker b, string path, SessionRequest req) =>
        {
            b.ReleaseSession(path, req.SessionId, req.SessionLockToken);
            return Results.NoContent();
        });

        api.MapPost("/sessions/state/get/{**path}", (Broker b, string path, SessionRequest req) =>
        {
            var state = b.GetSessionState(path, req.SessionId, req.SessionLockToken);
            return new SessionStateResponse { StateBase64 = state is null ? null : Convert.ToBase64String(state) };
        });

        api.MapPost("/sessions/state/set/{**path}", (Broker b, string path, SessionStateRequest req) =>
        {
            b.SetSessionState(path, req.SessionId, req.SessionLockToken,
                req.StateBase64 is null ? null : Convert.FromBase64String(req.StateBase64));
            return Results.NoContent();
        });
    }
}
