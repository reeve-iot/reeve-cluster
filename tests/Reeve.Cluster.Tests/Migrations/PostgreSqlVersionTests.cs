using Reeve.Cluster;
using Reeve.Cluster.Migrations;

namespace Unit.Migrations;

public sealed class PostgreSqlVersionTests
{
    [Theory]
    [InlineData(180000)]
    [InlineData(180003)]
    [InlineData(190000)]
    public void Version_18_and_later_passes(int serverVersionNum)
    {
        Assert.Null(PostgreSqlVersion.Refusal(serverVersionNum));
    }

    [Theory]
    [InlineData(170009)]
    [InlineData(160000)]
    [InlineData(179999)]
    [InlineData(0)]
    public void Anything_below_18_is_refused_with_its_named_reason(int serverVersionNum)
    {
        var fault = PostgreSqlVersion.Refusal(serverVersionNum);

        Assert.NotNull(fault);
        Assert.Equal(ClusterConfigurationReason.PostgreSqlTooOld, fault.Reason);
        Assert.Contains("18", fault.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void The_message_names_the_version_found()
    {
        Assert.Contains("17.9", PostgreSqlVersion.Refusal(170009)!.Message, StringComparison.Ordinal);
    }
}
