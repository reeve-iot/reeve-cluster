using Npgsql;
using Reeve.Cluster;
using Reeve.Cluster.Commands;
using Reeve.Cluster.Harness.Containers;
using static Int.Owned.OwnedDatabase;

namespace Int.Commands;

public sealed class CommandCommitTests : IClassFixture<PostgresFixture>, IAsyncLifetime
{
    private readonly PostgresFixture postgres;
    private CommandsRig rig = null!;
    private string database = string.Empty;

    public CommandCommitTests(PostgresFixture postgres)
    {
        this.postgres = postgres;
    }

    public async Task InitializeAsync()
    {
        rig = await CommandsRig.CreateAsync(postgres);
        database = new NpgsqlConnectionStringBuilder(rig.Db.ConnectionString).Database!;
    }

    public async Task DisposeAsync()
    {
        await SyncStandby.SetAsync(postgres, string.Empty);
        await rig.DisposeAsync();
    }

    [Fact]
    public async Task I33_a_take_cancelled_in_its_wait_for_the_standby_is_unknown_and_does_not_run_the_handler_although_the_call_returned_success()
    {
        var command = await rig.InsertAsync("i1");
        await StallAsync();

        using var cancel = new CancellationTokenSource();
        var take = rig.Store.TakeAsync(rig.Owner, command.Id, cancel.Token);
        await SyncStandby.UntilAWaiterAsync(postgres, database);
        await cancel.CancelAsync();
        var result = await take.WaitAsync(Poll.Bound);

        Assert.Equal(TakeAction.Unknown, result.Action);
        Assert.Equal(("taken", rig.Token), await rig.RowAsync(command.Id));
    }

    [Fact]
    public async Task I33_the_next_poll_reads_the_taken_row_and_runs_it_without_a_new_take()
    {
        var command = await rig.InsertAsync("i1");
        await StallAsync();
        using var cancel = new CancellationTokenSource();
        var take = rig.Store.TakeAsync(rig.Owner, command.Id, cancel.Token);
        await SyncStandby.UntilAWaiterAsync(postgres, database);
        await cancel.CancelAsync();
        Assert.Equal(TakeAction.Unknown, (await take.WaitAsync(Poll.Bound)).Action);

        var active = await rig.Store.ReadActiveAsync(CommandsRig.Kind, CommandsRig.Key, CancellationToken.None);
        var step = PickupDecisions.Next([.. active.Select(r => r.Active)], rig.Token, flight: null);

        Assert.Equal(new PickupStep(PickupAction.Run, command.Id), step);
    }

    [Fact]
    public async Task I33_a_completion_cancelled_in_its_wait_for_the_standby_is_unknown_although_it_landed()
    {
        var command = await rig.InsertAsync("i1");
        Assert.Equal(TakeAction.Run, (await rig.Store.TakeAsync(rig.Owner, command.Id, CancellationToken.None)).Action);
        await StallAsync();

        using var cancel = new CancellationTokenSource();
        var completion = rig.Store.CompleteAsync(rig.Owner, command.Id, CommandOutcome.Returned(new byte[] { 1 }), cancel.Token);
        await SyncStandby.UntilAWaiterAsync(postgres, database);
        await cancel.CancelAsync();

        Assert.Equal(CompleteAction.Unknown, await completion.WaitAsync(Poll.Bound));
        Assert.Equal(("done", rig.Token), await rig.RowAsync(command.Id));
    }

    private async Task StallAsync()
    {
        await using (var connection = await rig.Db.OpenAsync())
        {
            await ExecAsync(connection, $"ALTER DATABASE \"{database}\" SET synchronous_commit = on");
        }

        await SyncStandby.StallAsync(postgres);
    }
}
