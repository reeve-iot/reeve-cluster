using Microsoft.Extensions.Time.Testing;
using Npgsql;
using Reeve.Cluster;
using Reeve.Cluster.Harness.Containers;

namespace Int.Log;

public sealed class AckWaitCommitTests : IClassFixture<PostgresFixture>, IAsyncLifetime
{
    private static readonly TimeSpan AckTimeout = TimeSpan.FromMinutes(1);

    private readonly PostgresFixture postgres;
    private readonly FakeTimeProvider time = new();
    private AckRig rig = null!;

    public AckWaitCommitTests(PostgresFixture postgres)
    {
        this.postgres = postgres;
    }

    public async Task InitializeAsync() => rig = await AckRig.CreateAsync(postgres, AckTimeout, time: time);

    public async Task DisposeAsync()
    {
        await SetStandbyAsync(string.Empty);
        await rig.DisposeAsync();
    }

    [Fact]
    public async Task I33_a_record_held_in_its_wait_for_the_standby_past_AckTimeout_answers_Unconfirmed_Timeout_although_its_commit_lands()
    {
        var seq = await rig.WriteAsync();
        await rig.LocalAtAsync(seq);
        await StallTheStandbyAsync();
        var commitStarted = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        rig.Skip.Faults.Observe(LogFaultPoints.RecordBeforeCommit, _ =>
        {
            commitStarted.TrySetResult();
            return ValueTask.CompletedTask;
        });

        var wait = rig.Ack.WaitAppliedAsync(seq);
        await commitStarted.Task.WaitAsync(Poll.Bound);
        await SyncStandby.UntilAWaiterAsync(postgres, new NpgsqlConnectionStringBuilder(rig.Skip.ConnectionString).Database!);

        await AckWaitTests.AssertNotBeforeAsync(wait, time, AckTimeout);
        var outcome = await wait.WaitAsync(Poll.Bound);

        Assert.Equal(new AckOutcome.Unconfirmed(UnconfirmedReason.Timeout), outcome);
        Assert.Empty(rig.EventsOf(LogActions.AckDone));

        await Poll.UntilAsync(async () => await rig.AckedSeqAsync() == seq, "the cancelled record's commit landing");

        Assert.Empty(rig.EventsOf(LogActions.AckDone));
    }

    private async Task StallTheStandbyAsync()
    {
        var database = new NpgsqlConnectionStringBuilder(rig.Skip.ConnectionString).Database;
        await using (var connection = await rig.Log.OpenAsync())
        {
            await using var alter = new NpgsqlCommand($"ALTER DATABASE \"{database}\" SET synchronous_commit = on", connection);
            await alter.ExecuteNonQueryAsync();
        }

        await SyncStandby.StallAsync(postgres);
    }

    private Task SetStandbyAsync(string names) => SyncStandby.SetAsync(postgres, names);
}
