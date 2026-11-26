using Reeve.Cluster;
using Reeve.Cluster.Ask;
using Reeve.Cluster.Core;
using Reeve.Cluster.Harness.Containers;
using Int.Owned;
using static Int.Owned.OwnedDatabase;

namespace Int.Ask;

public sealed class AskStoreTests : IClassFixture<PostgresFixture>, IAsyncLifetime
{
    private const string Kind = "scheduler";
    private const string Key = "k1";

    private readonly PostgresFixture postgres;
    private OwnedDatabase db = null!;
    private ClusterDataSource data = null!;
    private AskStore store = null!;

    public AskStoreTests(PostgresFixture postgres)
    {
        this.postgres = postgres;
    }

    public async Task InitializeAsync()
    {
        db = await OwnedDatabase.CreateAsync(postgres);
        data = new ClusterDataSource(new ClusterOptions { ConnectionString = db.ConnectionString });
        store = new AskStore(data);
    }

    public async Task DisposeAsync() => await data.DisposeAsync();

    private async Task ExecuteAsync(string sql, params (string Name, object Value)[] parameters)
    {
        await using var connection = await db.OpenAsync();
        await ExecAsync(connection, sql, parameters);
    }

    private async Task<Guid> ProcessOfAsync(string replica)
    {
        await using var connection = await db.OpenAsync();
        return await ScalarAsync<Guid>(connection, "SELECT process_id FROM reeve_cluster.member WHERE replica_id = @r", ("r", replica));
    }

    private async Task<long> ServingOwnerAsync(string replica = "a")
    {
        await db.AddMemberAsync(replica);
        await ExecuteAsync("UPDATE reeve_cluster.member SET serving_since = clock_timestamp() WHERE replica_id = @r", ("r", replica));
        return await db.CreateOwnedAsync(Kind, Key, replica);
    }

    [Fact]
    public async Task A_serving_owner_resolves_with_its_process_and_the_rows_token()
    {
        var token = await ServingOwnerAsync();

        var owner = await store.ResolveOwnerAsync(Kind, Key, CancellationToken.None);

        Assert.Equal(new AskOwner("a", await ProcessOfAsync("a"), token), owner);
    }

    [Fact]
    public async Task An_absent_key_has_no_owner()
    {
        await db.AddMemberAsync("a");

        Assert.Null(await store.ResolveOwnerAsync(Kind, Key, CancellationToken.None));
    }

    [Fact]
    public async Task A_free_row_has_no_owner()
    {
        await ServingOwnerAsync();
        await ExecuteAsync("UPDATE reeve_cluster.owned SET owner_replica = NULL, owner_epoch = NULL WHERE kind = @k AND key = @key", ("k", Kind), ("key", Key));

        Assert.Null(await store.ResolveOwnerAsync(Kind, Key, CancellationToken.None));
    }

    [Fact]
    public async Task An_owner_that_has_not_set_serving_since_is_not_resolved()
    {
        await ServingOwnerAsync();
        await ExecuteAsync("UPDATE reeve_cluster.member SET serving_since = NULL WHERE replica_id = 'a'");

        Assert.Null(await store.ResolveOwnerAsync(Kind, Key, CancellationToken.None));
    }

    [Fact]
    public async Task An_owner_whose_lease_expired_is_not_resolved()
    {
        await ServingOwnerAsync();
        await ExecuteAsync("UPDATE reeve_cluster.member SET lease_expires_at = clock_timestamp() - interval '1 millisecond' WHERE replica_id = 'a'");

        Assert.Null(await store.ResolveOwnerAsync(Kind, Key, CancellationToken.None));
    }

    [Theory]
    [InlineData("joining")]
    [InlineData("fenced")]
    [InlineData("draining")]
    public async Task An_owner_whose_row_is_not_active_is_not_resolved(string state)
    {
        await ServingOwnerAsync();
        await ExecuteAsync($"UPDATE reeve_cluster.member SET state = '{state}' WHERE replica_id = 'a'");

        Assert.Null(await store.ResolveOwnerAsync(Kind, Key, CancellationToken.None));
    }

    [Fact]
    public async Task An_owner_that_rejoined_under_another_epoch_is_not_resolved_for_the_old_tenure()
    {
        await ServingOwnerAsync();
        await ExecuteAsync("UPDATE reeve_cluster.member SET epoch = nextval('reeve_cluster.member_epoch') WHERE replica_id = 'a'");

        Assert.Null(await store.ResolveOwnerAsync(Kind, Key, CancellationToken.None));
    }

    [Theory]
    [InlineData("joining", "1 minute")]
    [InlineData("active", "1 minute")]
    [InlineData("fenced", "1 minute")]
    [InlineData("active", "-1 minute")]
    [InlineData("fenced", "-1 minute")]
    public async Task A_named_replica_resolves_in_any_row_state_with_no_lease_filter(string state, string lease)
    {
        await db.AddMemberAsync("b", state, lease);

        var row = await store.ResolveMemberAsync("b", CancellationToken.None);

        Assert.Equal(new AskMemberRow(await ProcessOfAsync("b"), Enum.Parse<MemberRowState>(state, ignoreCase: true)), row);
    }

    [Fact]
    public async Task A_draining_row_resolves_as_draining_and_a_missing_row_as_none()
    {
        await db.AddMemberAsync("b", "draining");

        Assert.Equal(MemberRowState.Draining, (await store.ResolveMemberAsync("b", CancellationToken.None))!.State);
        Assert.Null(await store.ResolveMemberAsync("nobody", CancellationToken.None));
    }
}
