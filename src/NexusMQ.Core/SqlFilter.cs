using System.Collections.Concurrent;
using System.Globalization;
using System.Text;
using System.Text.RegularExpressions;

namespace NexusMQ.Core;

/// <summary>
/// Azure Service Bus-style SQL filter (subset of SQL-92) evaluated against message properties.
/// <para>Identifiers: <c>sys.MessageId, sys.CorrelationId, sys.Subject (sys.Label), sys.To, sys.ReplyTo,
/// sys.ContentType, sys.SessionId</c>; anything else (optionally prefixed <c>user.</c>, or written <c>[with spaces]</c>)
/// is an application property.</para>
/// <para>Operators: <c>= &lt;&gt; != &gt; &gt;= &lt; &lt;= AND OR NOT LIKE (with % and _ and ESCAPE) IN (...) IS [NOT] NULL EXISTS(prop)</c>,
/// parentheses, literals <c>'text'</c>, numbers, TRUE, FALSE, NULL. Values that look like numbers compare numerically.</para>
/// </summary>
public sealed class SqlFilter
{
    private static readonly ConcurrentDictionary<string, SqlFilter> Cache = new(StringComparer.Ordinal);

    private readonly Func<BrokerMessage, bool?> _eval;

    public string Expression { get; }

    private SqlFilter(string expression, Func<BrokerMessage, bool?> eval)
    {
        Expression = expression;
        _eval = eval;
    }

    /// <summary>Parse (and cache) a filter. Throws <see cref="NexusException"/> (BadRequest) on syntax errors.</summary>
    public static SqlFilter Parse(string expression) =>
        Cache.GetOrAdd(expression, e => new SqlFilter(e, new Parser(e).ParseAll()));

    /// <summary>True only when the expression evaluates to TRUE (UNKNOWN/NULL counts as no match, like SQL).</summary>
    public bool Matches(BrokerMessage message) => _eval(message) == true;

    // ===================================================================== tokenizer

    private enum T { Ident, String, Number, Op, LParen, RParen, Comma, End }

    private readonly record struct Token(T Kind, string Text, int Pos);

    private static List<Token> Tokenize(string s)
    {
        var tokens = new List<Token>();
        var i = 0;
        while (i < s.Length)
        {
            var c = s[i];
            if (char.IsWhiteSpace(c)) { i++; continue; }
            var start = i;
            if (c == '\'')
            {
                var sb = new StringBuilder();
                i++;
                while (true)
                {
                    if (i >= s.Length) throw Error("Unterminated string literal", start);
                    if (s[i] == '\'')
                    {
                        if (i + 1 < s.Length && s[i + 1] == '\'') { sb.Append('\''); i += 2; continue; }
                        i++;
                        break;
                    }
                    sb.Append(s[i++]);
                }
                tokens.Add(new Token(T.String, sb.ToString(), start));
            }
            else if (c == '[')
            {
                var close = s.IndexOf(']', i);
                if (close < 0) throw Error("Unterminated [identifier]", start);
                tokens.Add(new Token(T.Ident, s[(i + 1)..close], start));
                i = close + 1;
            }
            else if (char.IsDigit(c) || (c == '.' && i + 1 < s.Length && char.IsDigit(s[i + 1])) ||
                     (c == '-' && i + 1 < s.Length && char.IsDigit(s[i + 1]) && PrevAllowsSign(tokens)))
            {
                i++;
                while (i < s.Length && (char.IsDigit(s[i]) || s[i] == '.' || s[i] == 'e' || s[i] == 'E' ||
                                        ((s[i] == '+' || s[i] == '-') && (s[i - 1] == 'e' || s[i - 1] == 'E')))) i++;
                tokens.Add(new Token(T.Number, s[start..i], start));
            }
            else if (char.IsLetter(c) || c == '_' || c == '$')
            {
                while (i < s.Length && (char.IsLetterOrDigit(s[i]) || s[i] is '_' or '.' or '$' or '-')) i++;
                tokens.Add(new Token(T.Ident, s[start..i], start));
            }
            else if (c == '(') { tokens.Add(new Token(T.LParen, "(", i++)); }
            else if (c == ')') { tokens.Add(new Token(T.RParen, ")", i++)); }
            else if (c == ',') { tokens.Add(new Token(T.Comma, ",", i++)); }
            else
            {
                var two = i + 1 < s.Length ? s.Substring(i, 2) : "";
                if (two is "<>" or "!=" or ">=" or "<=") { tokens.Add(new Token(T.Op, two, i)); i += 2; }
                else if (c is '=' or '<' or '>') { tokens.Add(new Token(T.Op, c.ToString(), i++)); }
                else throw Error($"Unexpected character '{c}'", i);
            }
        }
        tokens.Add(new Token(T.End, "", s.Length));
        return tokens;

        static bool PrevAllowsSign(List<Token> t) => t.Count == 0 || t[^1].Kind is T.Op or T.LParen or T.Comma ||
                                                     (t[^1].Kind == T.Ident && IsKeyword(t[^1].Text));
    }

    private static readonly HashSet<string> Keywords = new(StringComparer.OrdinalIgnoreCase)
        { "AND", "OR", "NOT", "LIKE", "ESCAPE", "IN", "IS", "NULL", "TRUE", "FALSE", "EXISTS" };

    private static bool IsKeyword(string s) => Keywords.Contains(s);

    private static NexusException Error(string message, int pos) =>
        new(NexusErrorCode.BadRequest, $"SQL filter error at position {pos}: {message}.");

    // ===================================================================== parser → closures

    private delegate object? Value(BrokerMessage m);

    private sealed class Parser(string text)
    {
        private readonly List<Token> _t = Tokenize(text);
        private int _p;

        private Token Peek => _t[_p];
        private Token Next() => _t[_p++];
        private bool IsWord(string w) => Peek.Kind == T.Ident && Peek.Text.Equals(w, StringComparison.OrdinalIgnoreCase);
        private bool AcceptWord(string w) { if (!IsWord(w)) return false; _p++; return true; }
        private void ExpectWord(string w) { if (!AcceptWord(w)) throw Error($"Expected {w}", Peek.Pos); }
        private void Expect(T kind, string what) { if (Peek.Kind != kind) throw Error($"Expected {what}", Peek.Pos); _p++; }

        public Func<BrokerMessage, bool?> ParseAll()
        {
            if (string.IsNullOrWhiteSpace(text)) throw Error("Empty expression", 0);
            var e = ParseOr();
            if (Peek.Kind != T.End) throw Error($"Unexpected '{Peek.Text}'", Peek.Pos);
            return e;
        }

        private Func<BrokerMessage, bool?> ParseOr()
        {
            var left = ParseAnd();
            while (AcceptWord("OR"))
            {
                var l = left; var r = ParseAnd();
                left = m =>
                {
                    var a = l(m);
                    if (a == true) return true;
                    var b = r(m);
                    if (b == true) return true;
                    return a is null || b is null ? null : false;
                };
            }
            return left;
        }

        private Func<BrokerMessage, bool?> ParseAnd()
        {
            var left = ParseNot();
            while (AcceptWord("AND"))
            {
                var l = left; var r = ParseNot();
                left = m =>
                {
                    var a = l(m);
                    if (a == false) return false;
                    var b = r(m);
                    if (b == false) return false;
                    return a is null || b is null ? null : true;
                };
            }
            return left;
        }

        private Func<BrokerMessage, bool?> ParseNot()
        {
            if (AcceptWord("NOT"))
            {
                var inner = ParseNot();
                return m => inner(m) is { } b ? !b : null;
            }
            return ParsePredicate();
        }

        private Func<BrokerMessage, bool?> ParsePredicate()
        {
            if (Peek.Kind == T.LParen)
            {
                _p++;
                var inner = ParseOr();
                Expect(T.RParen, ")");
                return inner;
            }

            if (AcceptWord("EXISTS"))
            {
                Expect(T.LParen, "(");
                var tok = Next();
                if (tok.Kind != T.Ident) throw Error("EXISTS needs a property name", tok.Pos);
                Expect(T.RParen, ")");
                var get = Property(tok.Text);
                return m => get(m) is not null;
            }

            var left = ParseOperand();

            if (Peek.Kind == T.Op)
            {
                var op = Next().Text;
                var right = ParseOperand();
                return m => Compare(left(m), right(m), op);
            }

            var negate = AcceptWord("NOT");
            if (AcceptWord("LIKE"))
            {
                var pattern = Next();
                if (pattern.Kind != T.String) throw Error("LIKE needs a string pattern", pattern.Pos);
                char? escape = null;
                if (AcceptWord("ESCAPE"))
                {
                    var esc = Next();
                    if (esc.Kind != T.String || esc.Text.Length != 1) throw Error("ESCAPE needs a one-character string", esc.Pos);
                    escape = esc.Text[0];
                }
                var regex = LikeToRegex(pattern.Text, escape);
                return m => left(m) is { } v ? regex.IsMatch(AsString(v)) != negate : null;
            }
            if (AcceptWord("IN"))
            {
                Expect(T.LParen, "(");
                var items = new List<Value> { ParseOperand() };
                while (Peek.Kind == T.Comma) { _p++; items.Add(ParseOperand()); }
                Expect(T.RParen, ")");
                return m =>
                {
                    var v = left(m);
                    if (v is null) return null;
                    var found = items.Any(i => Compare(v, i(m), "=") == true);
                    return found != negate;
                };
            }
            if (negate) throw Error("Expected LIKE or IN after NOT", Peek.Pos);

            if (AcceptWord("IS"))
            {
                var not = AcceptWord("NOT");
                ExpectWord("NULL");
                return m => (left(m) is null) != not;
            }

            // Bare operand used as a boolean, e.g. "TRUE" or a property holding "true".
            return m => left(m) switch
            {
                null => null,
                bool b => b,
                string s when bool.TryParse(s, out var b) => b,
                _ => false
            };
        }

        private Value ParseOperand()
        {
            var tok = Next();
            switch (tok.Kind)
            {
                case T.String: { var s = tok.Text; return _ => s; }
                case T.Number:
                {
                    if (!double.TryParse(tok.Text, NumberStyles.Float, CultureInfo.InvariantCulture, out var d))
                        throw Error($"Invalid number '{tok.Text}'", tok.Pos);
                    return _ => d;
                }
                case T.Ident when tok.Text.Equals("NULL", StringComparison.OrdinalIgnoreCase): return _ => null;
                case T.Ident when tok.Text.Equals("TRUE", StringComparison.OrdinalIgnoreCase): return _ => true;
                case T.Ident when tok.Text.Equals("FALSE", StringComparison.OrdinalIgnoreCase): return _ => false;
                case T.Ident when !IsKeyword(tok.Text): { var get = Property(tok.Text); return m => get(m); }
                default: throw Error(tok.Kind == T.End ? "Unexpected end of expression" : $"Unexpected '{tok.Text}'", tok.Pos);
            }
        }

        private static Func<BrokerMessage, string?> Property(string name)
        {
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
                    _ => throw new NexusException(NexusErrorCode.BadRequest, $"Unknown system property '{name}'.")
                };
            }
            var key = name.StartsWith("user.", StringComparison.OrdinalIgnoreCase) ? name[5..] : name;
            return m => m.ApplicationProperties.TryGetValue(key, out var v) ? v : null;
        }
    }

    // ===================================================================== evaluation helpers

    private static string AsString(object v) => v switch
    {
        double d => d.ToString(CultureInfo.InvariantCulture),
        bool b => b ? "true" : "false",
        _ => v.ToString() ?? ""
    };

    private static double? AsNumber(object v) => v switch
    {
        double d => d,
        string s when double.TryParse(s, NumberStyles.Float, CultureInfo.InvariantCulture, out var d) => d,
        _ => null
    };

    private static bool? AsBool(object v) => v switch
    {
        bool b => b,
        string s when bool.TryParse(s, out var b) => b,
        _ => null
    };

    private static bool? Compare(object? a, object? b, string op)
    {
        if (a is null || b is null) return null;

        int cmp;
        if (a is bool || b is bool)
        {
            var (x, y) = (AsBool(a), AsBool(b));
            if (x is null || y is null) return op is "<>" or "!=" ? true : false;
            cmp = x.Value.CompareTo(y.Value);
        }
        else if ((a is double || b is double || (AsNumber(a) is not null && AsNumber(b) is not null))
                 && AsNumber(a) is { } na && AsNumber(b) is { } nb)
        {
            cmp = na.CompareTo(nb);
        }
        else if (a is double || b is double)
        {
            return op is "<>" or "!=" ? true : false; // number vs non-numeric text
        }
        else
        {
            cmp = string.CompareOrdinal(AsString(a), AsString(b));
        }

        return op switch
        {
            "=" => cmp == 0,
            "<>" or "!=" => cmp != 0,
            ">" => cmp > 0,
            ">=" => cmp >= 0,
            "<" => cmp < 0,
            "<=" => cmp <= 0,
            _ => throw new NexusException(NexusErrorCode.BadRequest, $"Unknown operator '{op}'.")
        };
    }

    private static Regex LikeToRegex(string pattern, char? escape)
    {
        var sb = new StringBuilder("^");
        for (var i = 0; i < pattern.Length; i++)
        {
            var c = pattern[i];
            if (escape is { } e && c == e && i + 1 < pattern.Length)
            {
                sb.Append(Regex.Escape(pattern[++i].ToString()));
                continue;
            }
            sb.Append(c switch { '%' => ".*", '_' => ".", _ => Regex.Escape(c.ToString()) });
        }
        sb.Append('$');
        return new Regex(sb.ToString(), RegexOptions.Singleline | RegexOptions.CultureInvariant);
    }
}
