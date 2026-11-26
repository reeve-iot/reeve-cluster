using Reeve.Cluster.Core;

namespace Unit.Core;

public sealed class ReplicationCheckTests
{
    private static ReplicationOutcome Evaluate(int walsenders, string standbyNames, string synchronousCommit) =>
        ReplicationCheck.Evaluate(new ReplicationSettings(walsenders, standbyNames, synchronousCommit)).Outcome;

    [Theory]
    [InlineData("", "off")]
    [InlineData("FIRST 1 (s1)", "on")]
    public void A_single_instance_passes_whatever_the_settings(string standbyNames, string synchronousCommit)
    {
        Assert.Equal(ReplicationOutcome.NoStandby, Evaluate(0, standbyNames, synchronousCommit));
    }

    [Theory]
    [InlineData("FIRST 1 (s1)", "on")]
    [InlineData("ANY 1 (s1, s2)", "remote_write")]
    [InlineData("*", "remote_apply")]
    public void Standbys_with_names_and_a_commit_that_waits_for_one_pass(string standbyNames, string synchronousCommit)
    {
        Assert.Equal(ReplicationOutcome.Synchronous, Evaluate(2, standbyNames, synchronousCommit));
    }

    [Theory]
    [InlineData("", "on")]
    [InlineData("  ", "remote_apply")]
    [InlineData("FIRST 1 (s1)", "local")]
    [InlineData("FIRST 1 (s1)", "off")]
    public void Standbys_without_names_or_with_a_commit_that_does_not_wait_are_not_synchronous(string standbyNames, string synchronousCommit)
    {
        Assert.Equal(ReplicationOutcome.NotSynchronous, Evaluate(1, standbyNames, synchronousCommit));
    }

    [Fact]
    public void The_finding_carries_what_was_read()
    {
        var settings = new ReplicationSettings(1, "", "on");

        Assert.Equal(new ReplicationFinding(ReplicationOutcome.NotSynchronous, settings), ReplicationCheck.Evaluate(settings));
    }
}
