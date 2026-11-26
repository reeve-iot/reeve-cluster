using Reeve.Cluster;
using Reeve.Cluster.Membership;

namespace Unit.Membership;

public sealed class MemberIdentityTests
{
    [Fact]
    public void Informational_version_with_build_metadata_is_accepted()
    {
        var identity = MemberIdentity.Create(Guid.NewGuid(), "1.2.3+commit.abc123", "test-assembly");
        Assert.Equal("1.2.3+commit.abc123", identity.Version);
    }

    [Fact]
    public void Informational_version_that_is_not_semver_fails_closed()
    {
        var exception = Assert.Throws<ClusterConfigurationException>(() => MemberIdentity.Create(Guid.NewGuid(), "version-one", "test-assembly"));
        Assert.Equal(ClusterConfigurationReason.VersionNotSemVer, exception.Reason);
        Assert.Contains("version-one", exception.Message, StringComparison.Ordinal);
        Assert.Contains("test-assembly", exception.Message, StringComparison.Ordinal);
    }
}
