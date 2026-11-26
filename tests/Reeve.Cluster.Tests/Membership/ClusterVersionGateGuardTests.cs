using Microsoft.Extensions.Options;
using Reeve.Cluster;
using Reeve.Cluster.Floors;
using Reeve.Cluster.Membership;

namespace Unit.Membership;

public sealed class ClusterVersionGateGuardTests
{
    private static readonly MemberIdentity Self = new(Guid.NewGuid(), "2.1.0", 3, []);

    [Fact]
    public async Task A_protocol_raise_above_the_callers_own_protocol_is_refused_before_any_statement()
    {
        var store = new RecordingStore();
        var gate = Gate(store);

        var thrown = await Assert.ThrowsAsync<ArgumentOutOfRangeException>(() => gate.RequireProtocolAsync(Self.Protocol + 1));

        Assert.Equal("protocol", thrown.ParamName);
        Assert.Equal(0, store.Raises);
    }

    [Fact]
    public async Task A_protocol_raise_to_the_callers_own_protocol_reaches_the_store()
    {
        var store = new RecordingStore();

        Assert.True(await Gate(store).RequireProtocolAsync(Self.Protocol));
        Assert.Equal(1, store.Raises);
    }

    [Theory]
    [InlineData("2.1.1")]
    [InlineData("2.2.0")]
    [InlineData("3.0.0-alpha")]
    public async Task A_version_raise_above_the_callers_own_version_is_refused_before_any_statement(string version)
    {
        var store = new RecordingStore();

        var thrown = await Assert.ThrowsAsync<ArgumentException>(() => Gate(store).RequireVersionAsync(version));

        Assert.Equal("version", thrown.ParamName);
        Assert.Equal(0, store.Raises);
    }

    [Theory]
    [InlineData("2.1.0")]
    [InlineData("2.1.0-rc.1")]
    [InlineData("1.0.0")]
    public async Task A_version_raise_to_or_below_the_callers_own_version_reaches_the_store(string version)
    {
        var store = new RecordingStore();

        Assert.True(await Gate(store).RequireVersionAsync(version));
        Assert.Equal(1, store.Raises);
    }

    private static ClusterVersionGate Gate(RecordingStore store) =>
        new(store, Options.Create(new ClusterOptions { ReplicaId = "r1" }), Self);

    private sealed class RecordingStore : IFloorStore
    {
        public int Raises { get; private set; }

        public Task<bool> RaiseProtocolAsync(int protocol, string by, CancellationToken cancellationToken)
        {
            Raises++;
            return Task.FromResult(true);
        }

        public Task<bool> RaiseVersionAsync(string version, string by, CancellationToken cancellationToken)
        {
            Raises++;
            return Task.FromResult(true);
        }

        public Task<FloorRow> ReadAsync(CancellationToken cancellationToken) => throw new NotSupportedException();

        public Task<FloorLowering> LowerProtocolAsync(int protocol, string by, CancellationToken cancellationToken) => throw new NotSupportedException();

        public Task<FloorLowering> LowerVersionAsync(string? version, string by, CancellationToken cancellationToken) => throw new NotSupportedException();

        public Task<bool> AllMembersSpeakAsync(int protocol, CancellationToken cancellationToken) => throw new NotSupportedException();

        public Task<bool> AllMembersAtLeastAsync(string version, CancellationToken cancellationToken) => throw new NotSupportedException();

        public Task<OlderActiveMember?> FindOlderActiveMemberAsync(string selfReplicaId, int minimumProtocol, CancellationToken cancellationToken) =>
            throw new NotSupportedException();
    }
}
