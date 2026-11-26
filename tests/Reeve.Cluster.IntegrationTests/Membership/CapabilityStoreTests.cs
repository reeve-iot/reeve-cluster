using Reeve.Cluster.Harness.Containers;
using Reeve.Cluster.Harness.Timing;
using Reeve.Cluster.Membership;

namespace Int.Membership;

public sealed class CapabilityStoreTests : IClassFixture<PostgresFixture>, IAsyncLifetime
{
    private readonly PostgresFixture postgres;
    private MemberDatabase db = null!;

    public CapabilityStoreTests(PostgresFixture postgres)
    {
        this.postgres = postgres;
    }

    public async Task InitializeAsync() => db = await MemberDatabase.CreateAsync(postgres);

    public async Task DisposeAsync() => await db.DisposeAsync();

    private async Task<long> JoinAsync(params string[] capabilities)
    {
        var identity = new MemberIdentity(Guid.NewGuid(), "1.2.3", MemberIdentity.CurrentProtocol, capabilities);
        var answer = await db.Store.JoinAsync(new JoinRequest("r1", TestProfile.LeaseTtl, identity), CancellationToken.None)
            .WaitAsync(TimeSpan.FromSeconds(30));
        return answer!.Value.Epoch;
    }

    [Fact]
    public async Task An_update_replaces_the_capabilities_of_the_joined_row_and_the_next_read_sees_it()
    {
        var epoch = await JoinAsync("degraded-mode", "unit:widget");

        Assert.True(await db.Capabilities.WriteAsync("r1", epoch, ["gpu", "unit:widget"], CancellationToken.None));

        Assert.Equal(["gpu", "unit:widget"], (await db.ReadRowAsync("r1")).Capabilities);
    }

    [Fact]
    public async Task An_update_with_another_epoch_changes_nothing()
    {
        var epoch = await JoinAsync("unit:widget");

        Assert.False(await db.Capabilities.WriteAsync("r1", epoch - 1, ["gpu"], CancellationToken.None));

        Assert.Equal(["unit:widget"], (await db.ReadRowAsync("r1")).Capabilities);
    }

    [Theory]
    [InlineData("fenced")]
    [InlineData("draining")]
    public async Task An_update_of_a_row_that_is_no_longer_active_changes_nothing(string state)
    {
        var epoch = await JoinAsync("unit:widget");
        await db.ExecuteAsync($"UPDATE reeve_cluster.member SET state = '{state}' WHERE replica_id = 'r1'");

        Assert.False(await db.Capabilities.WriteAsync("r1", epoch, ["gpu"], CancellationToken.None));

        Assert.Equal(["unit:widget"], (await db.ReadRowAsync("r1")).Capabilities);
    }

    [Fact]
    public async Task An_update_after_the_lease_ran_out_changes_nothing()
    {
        var epoch = await JoinAsync("unit:widget");
        await db.ExecuteAsync("UPDATE reeve_cluster.member SET lease_expires_at = clock_timestamp() - interval '1 second' WHERE replica_id = 'r1'");

        Assert.False(await db.Capabilities.WriteAsync("r1", epoch, ["gpu"], CancellationToken.None));

        Assert.Equal(["unit:widget"], (await db.ReadRowAsync("r1")).Capabilities);
    }
}
