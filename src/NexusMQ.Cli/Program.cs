using System.Text.Json;
using NexusMQ.Cli;
using NexusMQ.Client;

// nexusmq — command-line tool for NexusMQ. Run "nexusmq help" for usage.
var a = new Args(args);
if (a.Has("version")) { Console.WriteLine(typeof(NexusClient).Assembly.GetName().Version); return 0; }

var url = a.Opt("url") ?? Environment.GetEnvironmentVariable("NEXUSMQ_URL") ?? "http://localhost:5680";
var key = a.Opt("key") ?? Environment.GetEnvironmentVariable("NEXUSMQ_KEY");
var json = a.Has("json");
var cmd = a.Next()?.ToLowerInvariant();

if (cmd is null or "help" or "-h" or "--help") { Help.Print(); return 0; }

using var cts = new CancellationTokenSource();
Console.CancelKeyPress += (_, e) => { e.Cancel = true; cts.Cancel(); };
await using var client = new NexusClient(url, key, new NexusClientOptions { AllowUntrustedCertificates = a.Has("insecure") });
var admin = client.Admin;
var jsonOut = new JsonSerializerOptions(JsonSerializerDefaults.Web) { WriteIndented = true, Converters = { new System.Text.Json.Serialization.JsonStringEnumConverter() } };

try
{
    switch (cmd)
    {
        case "demo":
            return await Demo.RunAsync(client, TimeSpan.FromMinutes(double.Parse(a.Opt("minutes") ?? "5")), cts.Token);

        case "stats":
        {
            var stats = await admin.GetStatsAsync(cts.Token);
            if (json) { Out.Json(stats, jsonOut); break; }
            Out.Table(["ENTITY", "KIND", "ACTIVE", "LOCKED", "SCHEDULED", "DEFERRED", "DLQ", "SESSIONS", "ENQUEUED", "COMPLETED"],
                stats.Select(s => new[] { s.Path, s.Kind, $"{s.Active}", $"{s.Locked}", $"{s.Scheduled}", $"{s.Deferred}", $"{s.DeadLetter}", $"{s.LockedSessions}", $"{s.TotalEnqueued}", $"{s.TotalCompleted}" }));
            break;
        }

        case "topology":
            Out.Json(await admin.GetTopologyAsync(cts.Token), jsonOut);
            break;

        case "metrics":
            Console.Write(await admin.GetMetricsTextAsync(cts.Token));
            break;

        case "send":
        {
            var entity = a.Required("entity");
            var body = a.Next() ?? a.Opt("body") ?? (Console.IsInputRedirected ? await Console.In.ReadToEndAsync(cts.Token) : null)
                       ?? throw new CliException("Give a body: nexusmq send <entity> <body | @file>, or pipe it in.");
            var bytes = body.StartsWith('@') ? await File.ReadAllBytesAsync(body[1..], cts.Token) : System.Text.Encoding.UTF8.GetBytes(body);
            var count = int.Parse(a.Opt("count") ?? "1");
            var sender = client.CreateSender(entity);
            var props = a.All("prop").Select(p => p.Split('=', 2)).ToDictionary(p => p[0], p => p.Length > 1 ? p[1] : "");
            NexusMessage Build(int i) => new(bytes)
            {
                Subject = a.Opt("subject"),
                SessionId = a.Opt("session"),
                CorrelationId = a.Opt("correlation-id"),
                ContentType = a.Opt("content-type") ?? (LooksLikeJson(bytes) ? "application/json" : "text/plain"),
                MessageId = a.Opt("message-id") is { } id ? (count > 1 ? $"{id}-{i}" : id) : null,
                ApplicationProperties = new Dictionary<string, string>(props),
                ScheduledEnqueueTime = a.Opt("delay") is { } d ? DateTimeOffset.UtcNow.AddSeconds(double.Parse(d)) : null,
                TimeToLive = a.Opt("ttl") is { } t ? TimeSpan.FromSeconds(double.Parse(t)) : null
            };
            var delivered = 0;
            if (a.Opt("routing-key") is { } routingKey)
            {
                for (var i = 0; i < count; i++) delivered += (await sender.PublishAsync(routingKey, Build(i), cts.Token)).Delivered;
            }
            else
            {
                for (var i = 0; i < count; i += 500)
                    delivered += (await sender.SendMessagesAsync(Enumerable.Range(i, Math.Min(500, count - i)).Select(Build).ToList(), cts.Token)).Delivered;
            }
            Console.WriteLine($"sent {count} message(s) to {entity}, delivered to {delivered} destination(s)");
            break;
        }

        case "peek":
        {
            var entity = a.Required("entity");
            var msgs = await client.CreateReceiver(entity).PeekMessagesAsync(int.Parse(a.Opt("max") ?? "20"), long.Parse(a.Opt("from") ?? "0"), cts.Token);
            Out.Messages(msgs, json, jsonOut);
            if (!json && msgs.Count == 0) Console.WriteLine("(empty)");
            break;
        }

        case "receive":
        {
            // Peek-lock, print, then complete (default) / abandon (--abandon) / dead-letter (--deadletter).
            var entity = a.Required("entity");
            var receiver = client.CreateReceiver(entity);
            var msgs = await receiver.ReceiveMessagesAsync(int.Parse(a.Opt("max") ?? "1"), TimeSpan.FromSeconds(double.Parse(a.Opt("wait") ?? "5")), cts.Token);
            Out.Messages(msgs, json, jsonOut);
            foreach (var m in msgs)
            {
                if (a.Has("abandon")) await receiver.AbandonMessageAsync(m, ct: cts.Token);
                else if (a.Has("deadletter")) await receiver.DeadLetterMessageAsync(m, "ManualDeadLetter", "Dead-lettered with nexusmq CLI", cts.Token);
                else await receiver.CompleteMessageAsync(m, cts.Token);
            }
            if (!json) Console.WriteLine(msgs.Count == 0 ? "(no messages)" : $"{msgs.Count} message(s) {(a.Has("abandon") ? "abandoned" : a.Has("deadletter") ? "dead-lettered" : "completed")}");
            break;
        }

        case "listen":
        {
            // Stream messages until Ctrl+C. Completes each one unless --abandon.
            var entity = a.Required("entity");
            await using var p = client.CreateProcessor(entity, new NexusProcessorOptions
            {
                MaxConcurrentCalls = 1,
                AutoCompleteMessages = !a.Has("abandon"),
                ReceiveMode = a.Has("delete") ? NexusReceiveMode.ReceiveAndDelete : NexusReceiveMode.PeekLock
            });
            p.ProcessMessageAsync += async args =>
            {
                Out.Messages([args.Message], json, jsonOut);
                if (a.Has("abandon")) await args.AbandonMessageAsync();
            };
            p.ProcessErrorAsync += e => { Console.Error.WriteLine("error: " + e.Exception.Message); return Task.CompletedTask; };
            Console.Error.WriteLine($"listening on {entity} (Ctrl+C to stop)...");
            await p.StartProcessingAsync(cts.Token);
            try { await Task.Delay(Timeout.Infinite, cts.Token); } catch (OperationCanceledException) { }
            await p.StopProcessingAsync();
            break;
        }

        case "create":
        {
            var kind = a.Required("kind (queue | topic | subscription | exchange)").ToLowerInvariant();
            switch (kind)
            {
                case "queue":
                {
                    var name = a.Required("name");
                    await admin.CreateQueueAsync(name, QueueOptions(a), cts.Token);
                    Console.WriteLine($"queue {name} ready");
                    break;
                }
                case "topic":
                {
                    var name = a.Required("name");
                    await admin.CreateTopicAsync(name, a.Opt("dedup") is { } d ? TimeSpan.FromSeconds(double.Parse(d)) : null, cts.Token);
                    Console.WriteLine($"topic {name} ready");
                    break;
                }
                case "subscription":
                {
                    var topic = a.Required("topic");
                    var name = a.Required("name");
                    var rule = new NexusSubscriptionRule { Name = "rule", SqlFilter = a.Opt("sql"), SqlAction = a.Opt("action"), SubjectPattern = a.Opt("subject") };
                    var hasRule = rule.SqlFilter is not null || rule.SqlAction is not null || rule.SubjectPattern is not null;
                    await admin.CreateSubscriptionAsync(topic, name, hasRule ? [rule] : null, QueueOptions(a), cts.Token);
                    Console.WriteLine($"subscription {topic}/subscriptions/{name} ready");
                    break;
                }
                case "exchange":
                {
                    var name = a.Required("name");
                    var type = Enum.Parse<NexusExchangeType>(a.Next() ?? a.Opt("type") ?? "Direct", ignoreCase: true);
                    await admin.CreateExchangeAsync(name, type, cts.Token);
                    Console.WriteLine($"{type} exchange {name} ready");
                    break;
                }
                default: throw new CliException($"Unknown kind '{kind}'.");
            }
            break;
        }

        case "delete":
        {
            var kind = a.Required("kind (queue | topic | subscription | exchange)").ToLowerInvariant();
            switch (kind)
            {
                case "queue": await admin.DeleteQueueAsync(a.Required("name"), cts.Token); break;
                case "topic": await admin.DeleteTopicAsync(a.Required("name"), cts.Token); break;
                case "subscription": await admin.DeleteSubscriptionAsync(a.Required("topic"), a.Required("name"), cts.Token); break;
                case "exchange": await admin.DeleteExchangeAsync(a.Required("name"), cts.Token); break;
                default: throw new CliException($"Unknown kind '{kind}'.");
            }
            Console.WriteLine("deleted");
            break;
        }

        case "bind":
        {
            var exchange = a.Required("exchange");
            var destination = a.Required("destination");
            var routingKey = a.Next() ?? a.Opt("routing-key") ?? "";
            await admin.BindAsync(exchange, destination, routingKey, ct: cts.Token);
            Console.WriteLine($"bound exchanges/{exchange} --[{routingKey}]--> {destination}");
            break;
        }

        case "unbind":
            await admin.UnbindAsync(a.Required("exchange"), a.Required("destination"), a.Next() ?? "", cts.Token);
            Console.WriteLine("unbound");
            break;

        case "purge":
            Console.WriteLine($"purged {await admin.PurgeAsync(a.Required("entity"), cts.Token)} message(s)");
            break;

        case "resend":
        {
            var entity = a.Required("entity");
            var seq = long.Parse(a.Required("sequence number"));
            bool? move = a.Has("move") ? true : a.Has("copy") ? false : null;
            var n = await admin.ResendMessageAsync(entity, seq, a.Opt("to"), move, cts.Token);
            Console.WriteLine($"resent message {seq} from {entity} to {a.Opt("to") ?? "its source"} ({n} destination(s))");
            break;
        }

        case "delete-message":
        {
            var entity = a.Required("entity");
            var seq = long.Parse(a.Required("sequence number"));
            await admin.DeleteMessageAsync(entity, seq, cts.Token);
            Console.WriteLine($"deleted message {seq} from {entity}");
            break;
        }

        case "audit":
        {
            var entries = await admin.GetAuditAsync(int.Parse(a.Opt("max") ?? "50"), a.Opt("actor"), a.Opt("result"), cts.Token);
            if (json) { Out.Json(entries, jsonOut); break; }
            Out.Table(["TIME", "RESULT", "ACTOR", "ACTION", "TARGET", "DETAIL"],
                entries.Select(e => new[] { e.TimeUtc.ToLocalTime().ToString("g"), e.Result, e.Actor, e.Action, e.Target ?? "", e.Detail ?? "" }));
            if (entries.Count == 0) Console.WriteLine("(no entries)");
            break;
        }

        case "cluster":
        {
            var sub = a.Next()?.ToLowerInvariant() ?? "members";
            System.Text.Json.JsonElement r;
            switch (sub)
            {
                case "members": r = await admin.GetClusterMembersAsync(cts.Token); break;
                case "add":
                {
                    var id = a.Required("server id");
                    var memberUrl = a.Required("server url (how the other servers reach it)");
                    Console.Error.WriteLine($"adding {id} ({memberUrl}): it catches up first, then becomes a voting member...");
                    r = await admin.AddClusterMemberAsync(id, memberUrl, cts.Token);
                    break;
                }
                case "remove": r = await admin.RemoveClusterMemberAsync(a.Required("server id"), cts.Token); break;
                case "transfer":
                {
                    var to = a.Next();
                    var t = await admin.TransferLeadershipAsync(to, cts.Token);
                    if (json) { Console.WriteLine(t.GetRawText()); return 0; }
                    Console.WriteLine($"{t.GetProperty("leader").GetString()} is now the leader ({t.GetProperty("url").GetString()})");
                    return 0;
                }
                default: throw new CliException($"Unknown cluster command '{sub}'. Use members, add, remove or transfer.");
            }
            if (json) { Console.WriteLine(r.GetRawText()); break; }
            var members = r.ValueKind == JsonValueKind.Array ? r : r.GetProperty("members");
            var leaderId = r.ValueKind == JsonValueKind.Object && r.TryGetProperty("leader", out var l) && l.ValueKind == JsonValueKind.String ? l.GetString() : null;
            foreach (var m in members.EnumerateArray())
            {
                var id = m.GetProperty("id").GetString();
                Console.WriteLine($"{id,-12} {m.GetProperty("url").GetString(),-36} {(id == leaderId ? "leader" : "")}");
            }
            if (r.ValueKind == JsonValueKind.Object && r.TryGetProperty("learners", out var ls))
                foreach (var m in ls.EnumerateArray()) Console.WriteLine($"{m.GetProperty("id").GetString(),-12} {m.GetProperty("url").GetString(),-36} learner (catching up)");
            break;
        }

        case "replication":
        {
            var sub = a.Next()?.ToLowerInvariant() ?? "status";
            if (sub == "promote")
            {
                // Promote the server given by --url (use a single URL here: the standby).
                using var http = new HttpClient();
                if (!string.IsNullOrEmpty(key)) http.DefaultRequestHeaders.Add("X-Api-Key", key);
                var resp = await http.PostAsync(client.CurrentEndpoint + "api/replication/promote", null, cts.Token);
                Console.WriteLine(resp.IsSuccessStatusCode ? $"{client.CurrentEndpoint} is now PRIMARY" : $"promote failed: {(int)resp.StatusCode} {await resp.Content.ReadAsStringAsync(cts.Token)}");
                return resp.IsSuccessStatusCode ? 0 : 1;
            }
            foreach (var endpoint in client.Endpoints)
            {
                await using var one = new NexusClient(endpoint.ToString(), key, new NexusClientOptions { AllowUntrustedCertificates = a.Has("insecure"), FailoverWaitTime = TimeSpan.Zero });
                try
                {
                    var st = await one.Admin.GetReplicationStatusAsync(cts.Token);
                    if (json) { Console.WriteLine(st.GetRawText()); continue; }
                    var role = st.GetProperty("role").GetString();
                    if (st.TryGetProperty("cluster", out var cl) && cl.ValueKind == JsonValueKind.Object)
                    {
                        Console.WriteLine($"{endpoint,-32} {cl.GetProperty("role").GetString(),-9} node={cl.GetProperty("nodeId").GetString()} term={cl.GetProperty("term").GetInt64()}"
                            + $" leader={(cl.TryGetProperty("leaderId", out var lid) && lid.ValueKind == JsonValueKind.String ? lid.GetString() : "-")}"
                            + $" log={cl.GetProperty("lastLogIndex").GetInt64()} committed={cl.GetProperty("commitIndex").GetInt64()} applied={cl.GetProperty("lastApplied").GetInt64()}");
                        continue;
                    }
                    var line = $"{endpoint,-32} {role,-8} position={st.GetProperty("position").GetInt64()}";
                    if (role == "Standby")
                        line += $"  primary={st.GetProperty("primaryUrl").GetString()} connected={st.GetProperty("connectedToPrimary").GetBoolean()}"
                              + (st.TryGetProperty("secondsSinceContact", out var sc) && sc.ValueKind == JsonValueKind.Number ? $" lastContact={sc.GetDouble()}s ago" : "")
                              + $" autoFailover={st.GetProperty("autoFailover").GetBoolean()}";
                    else
                        line += $"  standbys={st.GetProperty("standbys").GetArrayLength()}";
                    Console.WriteLine(line);
                }
                catch (Exception ex) when (ex is HttpRequestException or NexusClientException)
                {
                    Console.WriteLine($"{endpoint,-32} DOWN     {ex.Message}");
                }
            }
            break;
        }

        case "resubmit":
            Console.WriteLine($"resubmitted {await admin.ResubmitDeadLettersAsync(a.Required("entity"), cts.Token)} dead letter(s)");
            break;

        case "keys":
        {
            var sub = a.Next()?.ToLowerInvariant() ?? "list";
            switch (sub)
            {
                case "list":
                {
                    var keys = await admin.ListKeysAsync(cts.Token);
                    if (json) { Out.Json(keys, jsonOut); break; }
                    Out.Table(["NAME", "RIGHTS", "ENTITIES", "LIMIT", "SOURCE", "LAST USED"],
                        keys.Select(k => new[] { k.Name, k.Rights.ToString(), k.Entities.Count == 0 ? "*" : string.Join(",", k.Entities),
                            k.RequestsPerSecond is { } r ? $"{r}/s" : "-", k.Source, k.LastUsedUtc?.ToLocalTime().ToString("g") ?? "-" }));
                    if (keys.Count == 0) Console.WriteLine("(no keys: the broker is open)");
                    break;
                }
                case "create":
                {
                    var name = a.Required("name");
                    var rights = Enum.Parse<NexusKeyRights>(a.Opt("rights") ?? throw new CliException("--rights is required, e.g. --rights Send,Listen"), ignoreCase: true);
                    var r = await admin.CreateKeyAsync(name, rights, a.All("entity"),
                        a.Opt("rps") is { } rps ? double.Parse(rps, System.Globalization.CultureInfo.InvariantCulture) : null,
                        a.Opt("burst") is { } b ? int.Parse(b) : null, cts.Token);
                    PrintSecret(r);
                    break;
                }
                case "rotate":
                {
                    var name = a.Required("name");
                    var r = await admin.RotateKeyAsync(name, TimeSpan.FromSeconds(double.Parse(a.Opt("grace") ?? "0")), cts.Token);
                    PrintSecret(r);
                    if (r.Key.PreviousKeyExpiresUtc is { } until) Console.Error.WriteLine($"the old secret keeps working until {until.ToLocalTime():T}");
                    break;
                }
                case "delete":
                    await admin.DeleteKeyAsync(a.Required("name"), cts.Token);
                    Console.WriteLine("deleted");
                    break;
                case "limits":
                {
                    var name = a.Required("name");
                    var info = await admin.SetKeyLimitsAsync(name,
                        a.Has("none") ? null : double.Parse(a.Opt("rps") ?? throw new CliException("Give --rps n (and optional --burst n), or --none."), System.Globalization.CultureInfo.InvariantCulture),
                        a.Has("none") ? null : a.Opt("burst") is { } b ? int.Parse(b) : null, cts.Token);
                    Console.WriteLine(info.RequestsPerSecond is { } r ? $"{name}: {r} requests/second, burst {info.Burst?.ToString() ?? "default"}" : $"{name}: unlimited");
                    break;
                }
                default: throw new CliException($"Unknown keys command '{sub}'. Use list, create, rotate, limits or delete.");
            }
            break;
        }

        default:
            throw new CliException($"Unknown command '{cmd}'. Run 'nexusmq help'.");
    }
    return 0;
}
catch (CliException ex)
{
    Console.Error.WriteLine(ex.Message);
    return 2;
}
catch (NexusClientException ex)
{
    Console.Error.WriteLine($"{ex.Code}: {ex.Message}");
    return 1;
}
catch (HttpRequestException ex) when (ex.InnerException is System.Security.Authentication.AuthenticationException)
{
    Console.Error.WriteLine($"TLS certificate of {url} is not trusted: {ex.InnerException.Message}");
    Console.Error.WriteLine("Trust the certificate on this machine, or for a development self-signed certificate add --insecure.");
    return 1;
}
catch (HttpRequestException ex)
{
    Console.Error.WriteLine($"cannot reach NexusMQ at {url}: {ex.Message}");
    return 1;
}
catch (OperationCanceledException)
{
    return 130;
}

void PrintSecret(NexusKeySecret r)
{
    if (json) { Out.Json(r, jsonOut); return; }
    Console.Error.WriteLine($"key '{r.Key.Name}' ({r.Key.Rights}). Copy the secret now; it is not shown again:");
    Console.WriteLine(r.Secret);
}

static NexusQueueOptions? QueueOptions(Args a)
{
    if (!new[] { "session", "max-delivery", "lock", "ttl", "dedup", "max-size", "forward-to", "forward-dlq-to" }.Any(o => a.Has(o) || a.Opt(o) is not null)) return null;
    var o = new NexusQueueOptions { RequiresSession = a.Has("session") };
    if (a.Opt("max-delivery") is { } md) o.MaxDeliveryCount = int.Parse(md);
    if (a.Opt("lock") is { } l) o.LockDuration = TimeSpan.FromSeconds(double.Parse(l));
    if (a.Opt("ttl") is { } t) o.DefaultTimeToLive = TimeSpan.FromSeconds(double.Parse(t));
    if (a.Opt("dedup") is { } d) o.DuplicateDetectionWindow = TimeSpan.FromSeconds(double.Parse(d));
    if (a.Opt("max-size") is { } ms) o.MaxSizeInMessages = int.Parse(ms);
    o.ForwardTo = a.Opt("forward-to");
    o.ForwardDeadLetteredMessagesTo = a.Opt("forward-dlq-to");
    return o;
}

static bool LooksLikeJson(byte[] b)
{
    var s = System.Text.Encoding.UTF8.GetString(b).TrimStart();
    return s.StartsWith('{') || s.StartsWith('[');
}
