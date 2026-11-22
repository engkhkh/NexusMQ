using System.Collections.Concurrent;
using System.Globalization;
using System.Text;

namespace NexusMQ.Core;

/// <summary>
/// Azure-style rule action that modifies the copy of a message delivered to a subscription.
/// Statements are separated by ';':
/// <list type="bullet">
/// <item><c>SET name = 'value'</c> / <c>SET name = 42</c> / <c>SET name = otherProperty</c></item>
/// <item><c>SET sys.Subject = 'billing.' + region</c> (use + to concatenate)</item>
/// <item><c>REMOVE name</c></item>
/// </list>
/// Targets may be application properties or <c>sys.Subject, sys.CorrelationId, sys.To, sys.ReplyTo, sys.ContentType</c>.
/// </summary>
public sealed class SqlRuleAction
{
    private static readonly ConcurrentDictionary<string, SqlRuleAction> Cache = new(StringComparer.Ordinal);
    private readonly List<Action<BrokerMessage>> _statements;

    public string Expression { get; }

    private SqlRuleAction(string expression, List<Action<BrokerMessage>> statements)
    {
        Expression = expression;
        _statements = statements;
    }

    public static SqlRuleAction Parse(string expression) => Cache.GetOrAdd(expression, e => new SqlRuleAction(e, ParseStatements(e)));

    public void Apply(BrokerMessage message)
    {
        foreach (var s in _statements) s(message);
    }

    private static NexusException Error(string msg) => new(NexusErrorCode.BadRequest, $"SQL action error: {msg}.");

    private static List<Action<BrokerMessage>> ParseStatements(string text)
    {
        if (string.IsNullOrWhiteSpace(text)) throw Error("empty action");
        var result = new List<Action<BrokerMessage>>();
        foreach (var raw in SplitOutsideQuotes(text, ';'))
        {
            var stmt = raw.Trim();
            if (stmt.Length == 0) continue;

            if (stmt.StartsWith("REMOVE ", StringComparison.OrdinalIgnoreCase))
            {
                var name = Unbracket(stmt[7..].Trim());
                if (name.StartsWith("sys.", StringComparison.OrdinalIgnoreCase)) throw Error("system properties cannot be removed");
                var key = StripUser(name);
                result.Add(m => m.ApplicationProperties.Remove(key));
                continue;
            }

            if (!stmt.StartsWith("SET ", StringComparison.OrdinalIgnoreCase)) throw Error($"expected SET or REMOVE in '{stmt}'");
            var eq = IndexOutsideQuotes(stmt, '=');
            if (eq < 0) throw Error($"expected '=' in '{stmt}'");
            var target = Unbracket(stmt[4..eq].Trim());
            if (target.Length == 0) throw Error("missing property name after SET");
            var parts = SplitOutsideQuotes(stmt[(eq + 1)..], '+').Select(p => p.Trim()).ToList();
            if (parts.Count == 0 || parts.Any(p => p.Length == 0)) throw Error($"missing value in '{stmt}'");
            var getters = parts.Select(Operand).ToList();
            var setter = Setter(target);
            result.Add(m =>
            {
                var sb = new StringBuilder();
                foreach (var g in getters) sb.Append(g(m));
                setter(m, sb.ToString());
            });
        }
        if (result.Count == 0) throw Error("no statements");
        return result;
    }

    private static Func<BrokerMessage, string?> Operand(string token)
    {
        if (token.StartsWith('\''))
        {
            if (token.Length < 2 || !token.EndsWith('\'')) throw Error($"unterminated string {token}");
            var value = token[1..^1].Replace("''", "'");
            return _ => value;
        }
        if (double.TryParse(token, NumberStyles.Float, CultureInfo.InvariantCulture, out _)) return _ => token;
        if (token.Equals("TRUE", StringComparison.OrdinalIgnoreCase)) return _ => "true";
        if (token.Equals("FALSE", StringComparison.OrdinalIgnoreCase)) return _ => "false";

        var name = Unbracket(token);
        if (name.StartsWith("sys.", StringComparison.OrdinalIgnoreCase))
        {
            return name[4..].ToLowerInvariant() switch
            {
                "messageid" => m => m.MessageId,
                "correlationid" => m => m.CorrelationId,
                "subject" or "label" => m => m.Subject,
                "to" => m => m.To,
                "replyto" => m => m.ReplyTo,
                "contenttype" => m => m.ContentType,
                "sessionid" => m => m.SessionId,
                _ => throw Error($"unknown system property '{name}'")
            };
        }
        var key = StripUser(name);
        return m => m.ApplicationProperties.GetValueOrDefault(key);
    }

    private static Action<BrokerMessage, string> Setter(string target)
    {
        if (target.StartsWith("sys.", StringComparison.OrdinalIgnoreCase))
        {
            return target[4..].ToLowerInvariant() switch
            {
                "subject" or "label" => (m, v) => m.Subject = v,
                "correlationid" => (m, v) => m.CorrelationId = v,
                "to" => (m, v) => m.To = v,
                "replyto" => (m, v) => m.ReplyTo = v,
                "contenttype" => (m, v) => m.ContentType = v,
                _ => throw Error($"system property '{target}' cannot be set")
            };
        }
        var key = StripUser(target);
        return (m, v) => m.ApplicationProperties[key] = v;
    }

    private static string StripUser(string name) => name.StartsWith("user.", StringComparison.OrdinalIgnoreCase) ? name[5..] : name;
    private static string Unbracket(string s) => s.StartsWith('[') && s.EndsWith(']') ? s[1..^1] : s;

    private static int IndexOutsideQuotes(string s, char c)
    {
        var inQuote = false;
        for (var i = 0; i < s.Length; i++)
        {
            if (s[i] == '\'') inQuote = !inQuote;
            else if (s[i] == c && !inQuote) return i;
        }
        return -1;
    }

    private static List<string> SplitOutsideQuotes(string s, char sep)
    {
        var parts = new List<string>();
        var inQuote = false;
        var start = 0;
        for (var i = 0; i < s.Length; i++)
        {
            if (s[i] == '\'') inQuote = !inQuote;
            else if (s[i] == sep && !inQuote)
            {
                parts.Add(s[start..i]);
                start = i + 1;
            }
        }
        parts.Add(s[start..]);
        return parts;
    }
}
