namespace NexusMQ.Core;

/// <summary>Routing helpers shared by exchanges and subscription rules.</summary>
public static class Routing
{
    /// <summary>
    /// RabbitMQ topic matching. Words are separated by '.', '*' matches exactly one word,
    /// '#' matches zero or more words.
    /// </summary>
    public static bool TopicMatches(string pattern, string? routingKey)
    {
        var p = pattern.Split('.');
        var k = string.IsNullOrEmpty(routingKey) ? [] : routingKey.Split('.');
        return Match(p, 0, k, 0);

        static bool Match(string[] p, int pi, string[] k, int ki)
        {
            while (true)
            {
                if (pi == p.Length) return ki == k.Length;
                if (p[pi] == "#")
                {
                    // '#' at the end matches the rest.
                    if (pi == p.Length - 1) return true;
                    for (var i = ki; i <= k.Length; i++)
                        if (Match(p, pi + 1, k, i)) return true;
                    return false;
                }
                if (ki == k.Length) return false;
                if (p[pi] != "*" && !string.Equals(p[pi], k[ki], StringComparison.Ordinal)) return false;
                pi++;
                ki++;
            }
        }
    }

    public static bool HeadersMatch(Dictionary<string, string>? bindingArgs, Dictionary<string, string> headers)
    {
        if (bindingArgs is null || bindingArgs.Count == 0) return true;
        var any = bindingArgs.TryGetValue("x-match", out var mode) && mode.Equals("any", StringComparison.OrdinalIgnoreCase);
        var pairs = bindingArgs.Where(kv => !kv.Key.StartsWith("x-", StringComparison.Ordinal)).ToList();
        if (pairs.Count == 0) return true;
        bool Has(KeyValuePair<string, string> kv) => headers.TryGetValue(kv.Key, out var v) && v == kv.Value;
        return any ? pairs.Any(Has) : pairs.All(Has);
    }

    public static bool RuleMatches(SubscriptionRule rule, BrokerMessage m)
    {
        if (rule.CorrelationId is not null && rule.CorrelationId != m.CorrelationId) return false;
        if (rule.Subject is not null && rule.Subject != m.Subject) return false;
        if (rule.SubjectPattern is not null && !TopicMatches(rule.SubjectPattern, m.Subject)) return false;
        if (rule.To is not null && rule.To != m.To) return false;
        if (rule.ContentType is not null && rule.ContentType != m.ContentType) return false;
        if (rule.SqlFilter is not null && !SqlFilter.Parse(rule.SqlFilter).Matches(m)) return false;
        if (rule.Properties is not null)
            foreach (var (key, value) in rule.Properties)
                if (!m.ApplicationProperties.TryGetValue(key, out var v) || v != value) return false;
        return true;
    }
}
