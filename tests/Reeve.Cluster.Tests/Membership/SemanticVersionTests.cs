using Reeve.Cluster.Core;

namespace Unit.Membership;

public sealed class SemanticVersionTests
{
    [Theory]
    [InlineData("1.9.0", "1.10.0")]
    [InlineData("1.10.0", "2.0.0")]
    [InlineData("1.0.0-alpha", "1.0.0")]
    [InlineData("1.0.0-rc.1", "1.0.0")]
    [InlineData("1.0.0-alpha", "1.0.0-alpha.1")]
    [InlineData("1.0.0-alpha.1", "1.0.0-alpha.beta")]
    [InlineData("1.0.0-alpha.beta", "1.0.0-beta")]
    [InlineData("1.0.0-beta", "1.0.0-beta.2")]
    [InlineData("1.0.0-beta.2", "1.0.0-beta.11")]
    [InlineData("1.0.0-beta.11", "1.0.0-rc.1")]
    [InlineData("0.9.99", "0.10.0")]
    [InlineData("2.0.0", "10.0.0")]
    [InlineData("1.0.0-9", "1.0.0-10")]
    [InlineData("1.0.0-99", "1.0.0-a")]
    [InlineData("99999999999999999999999.0.0", "100000000000000000000000.0.0")]
    public void Precedence_orders_by_SemVer_where_text_order_may_disagree(string lower, string higher)
    {
        var a = SemanticVersion.Parse(lower);
        var b = SemanticVersion.Parse(higher);
        Assert.True(a.CompareTo(b) < 0, $"{lower} must precede {higher}");
        Assert.True(b.CompareTo(a) > 0, $"{higher} must follow {lower}");
    }

    [Fact]
    public void Text_order_disagrees_with_SemVer_in_the_cases_the_spec_names()
    {
        Assert.True(string.CompareOrdinal("1.9.0", "1.10.0") > 0);
        Assert.True(string.CompareOrdinal("1.0.0-rc.1", "1.0.0") > 0);
    }

    [Theory]
    [InlineData("1.2.3", "1.2.3+build.5")]
    [InlineData("1.2.3-rc.1+a", "1.2.3-rc.1+b")]
    [InlineData("1.2.3", "1.2.3")]
    public void Build_metadata_is_ignored(string a, string b)
    {
        Assert.Equal(0, SemanticVersion.Parse(a).CompareTo(SemanticVersion.Parse(b)));
    }

    [Theory]
    [InlineData("")]
    [InlineData("1")]
    [InlineData("1.2")]
    [InlineData("1.2.3.4")]
    [InlineData("v1.2.3")]
    [InlineData("01.2.3")]
    [InlineData("1.02.3")]
    [InlineData("1.2.03")]
    [InlineData("1.2.3-")]
    [InlineData("1.2.3-01")]
    [InlineData("1.2.3-a..b")]
    [InlineData("1.2.3+")]
    [InlineData("1.2.3+a..b")]
    [InlineData("1.2.3-a_b")]
    [InlineData(" 1.2.3")]
    [InlineData("-1.2.3")]
    [InlineData("1..3")]
    [InlineData("1.2.")]
    [InlineData(".2.3")]
    public void A_string_that_is_not_SemVer_does_not_parse(string text)
    {
        Assert.False(SemanticVersion.TryParse(text, out _));
        Assert.Throws<FormatException>(() => SemanticVersion.Parse(text));
    }

    [Theory]
    [InlineData("1.2.3", "1.2.4")]
    [InlineData("1.2.9", "1.2.10")]
    [InlineData("0.0.1", "0.0.2")]
    public void A_version_below_the_floor_by_its_patch_alone_is_refused(string lower, string higher)
    {
        Assert.True(SemanticVersion.Parse(lower).CompareTo(SemanticVersion.Parse(higher)) < 0, $"{lower} must precede {higher}");
        Assert.True(SemanticVersion.Parse(higher).CompareTo(SemanticVersion.Parse(lower)) > 0, $"{higher} must follow {lower}");
        Assert.Equal(Admission.BelowVersionFloor, ActivationRules.Admit(lower, higher));
        Assert.Equal(Admission.Admitted, ActivationRules.Admit(higher, lower));
    }

    [Fact]
    public void Identifiers_with_upper_case_letters_parse_and_compare_in_ASCII_order()
    {
        Assert.True(SemanticVersion.TryParse("1.0.0-RC.1", out _));
        Assert.True(SemanticVersion.TryParse("1.0.0+Build.ABC", out _));
        Assert.True(SemanticVersion.Parse("1.0.0-RC.1").CompareTo(SemanticVersion.Parse("1.0.0-rc.1")) < 0);
        Assert.True(SemanticVersion.Parse("1.0.0-Alpha").CompareTo(SemanticVersion.Parse("1.0.0-alpha")) < 0);
        Assert.Equal(Admission.BelowVersionFloor, ActivationRules.Admit("1.0.0-RC.1", "1.0.0"));
    }

    [Theory]
    [InlineData("0.0.0")]
    [InlineData("1.2.3-0")]
    [InlineData("1.2.3-0a")]
    [InlineData("1.2.3-alpha-1.x")]
    [InlineData("1.2.3+001")]
    [InlineData("1.2.3-rc.1+sha.5114f85")]
    public void Valid_SemVer_parses(string text)
    {
        Assert.True(SemanticVersion.TryParse(text, out var version));
        Assert.Equal(text, version!.ToString());
    }
}
