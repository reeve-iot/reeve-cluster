namespace Reeve.Cluster.Core;

internal sealed class SemanticVersion : IComparable<SemanticVersion>
{
    private readonly string text;
    private readonly string[] core;
    private readonly string[] preRelease;

    private SemanticVersion(string text, string[] core, string[] preRelease)
    {
        this.text = text;
        this.core = core;
        this.preRelease = preRelease;
    }

    public static SemanticVersion Parse(string text) =>
        TryParse(text, out var version) ? version! : throw new FormatException($"'{text}' is not a SemVer 2.0 version.");

    public static bool TryParse(string? text, out SemanticVersion? version)
    {
        version = null;
        if (string.IsNullOrEmpty(text))
        {
            return false;
        }

        var rest = text;
        var plus = rest.IndexOf('+', StringComparison.Ordinal);
        if (plus >= 0)
        {
            if (!rest[(plus + 1)..].Split('.').All(IsIdentifier))
            {
                return false;
            }

            rest = rest[..plus];
        }

        string[] pre = [];
        var dash = rest.IndexOf('-', StringComparison.Ordinal);
        if (dash >= 0)
        {
            pre = rest[(dash + 1)..].Split('.');
            if (!pre.All(p => IsIdentifier(p) && (!IsNumeric(p) || NoLeadingZero(p))))
            {
                return false;
            }

            rest = rest[..dash];
        }

        var parts = rest.Split('.');
        if (parts.Length != 3 || !parts.All(p => IsNumeric(p) && NoLeadingZero(p)))
        {
            return false;
        }

        version = new SemanticVersion(text, parts, pre);
        return true;
    }

    public int CompareTo(SemanticVersion? other)
    {
        if (other is null)
        {
            return 1;
        }

        for (var i = 0; i < 3; i++)
        {
            var c = CompareNumeric(core[i], other.core[i]);
            if (c != 0)
            {
                return c;
            }
        }

        switch (preRelease.Length == 0, other.preRelease.Length == 0)
        {
            case (true, true):
                return 0;
            case (true, false):
                return 1;
            case (false, true):
                return -1;
        }

        for (var i = 0; i < Math.Min(preRelease.Length, other.preRelease.Length); i++)
        {
            var c = CompareIdentifier(preRelease[i], other.preRelease[i]);
            if (c != 0)
            {
                return c;
            }
        }

        return preRelease.Length.CompareTo(other.preRelease.Length);
    }

    public override string ToString() => text;

    private static bool IsIdentifier(string part) =>
        part.Length > 0 && part.All(c => c is (>= '0' and <= '9') or (>= 'A' and <= 'Z') or (>= 'a' and <= 'z') or '-');

    private static bool IsNumeric(string part) => part.Length > 0 && part.All(char.IsAsciiDigit);

    private static bool NoLeadingZero(string digits) => digits.Length == 1 || digits[0] != '0';

    private static int CompareNumeric(string a, string b) =>
        a.Length != b.Length ? a.Length.CompareTo(b.Length) : string.CompareOrdinal(a, b);

    private static int CompareIdentifier(string a, string b) => (IsNumeric(a), IsNumeric(b)) switch
    {
        (true, true) => CompareNumeric(a, b),
        (true, false) => -1,
        (false, true) => 1,
        _ => Math.Sign(string.CompareOrdinal(a, b)),
    };
}
