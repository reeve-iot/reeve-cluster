using System.Diagnostics;
using Npgsql;
using Reeve.Cluster;
using Reeve.Cluster.Harness.Containers;
using Xunit.Abstractions;

namespace Int.Log;

public sealed class AckCommitTests : IClassFixture<PostgresFixture>, IAsyncLifetime
{
    private static readonly TimeSpan NoAnswer = TimeSpan.FromSeconds(5);

    private readonly PostgresFixture postgres;
    private readonly ITestOutputHelper output;
    private SkipRig rig = null!;

    public AckCommitTests(PostgresFixture postgres, ITestOutputHelper output)
    {
        this.postgres = postgres;
        this.output = output;
    }

    public async Task InitializeAsync() =>
        rig = await SkipRig.CreateAsync(postgres, connectionString: c => new NpgsqlConnectionStringBuilder(c) { CommandTimeout = 1 }.ConnectionString);

    public async Task DisposeAsync()
    {
        await SetStandbyAsync(string.Empty);
        await rig.DisposeAsync();
    }

    [Fact]
    public async Task I33_a_Command_Timeout_in_the_connection_string_does_not_cancel_the_records_wait_for_the_standby()
    {
        var seq = await rig.WriteAsync("widget");
        await StallTheStandbyAsync();
        var commit = CommitStarts(LogFaultPoints.RecordBeforeCommit);

        var record = rig.RecordAsync(seq, seq);

        await AssertNoAnswerAsync(record, commit.Task);
        Assert.Empty(await record.WaitAsync(TimeSpan.FromSeconds(30)));
    }

    [Fact]
    public async Task I33_a_Command_Timeout_in_the_connection_string_does_not_cancel_the_opens_wait_for_the_standby()
    {
        await StallTheStandbyAsync();
        var commit = CommitStarts(LogFaultPoints.OpenAfterInsert);

        var open = rig.OpenAsync("gadget", 1);

        await AssertNoAnswerAsync(open, commit.Task);
        Assert.IsType<SkipOpenResult.Opened>(await open.WaitAsync(TimeSpan.FromSeconds(30)));
    }

    [Fact]
    public async Task I33_a_record_whose_wait_for_the_standby_its_callers_token_cancels_throws_although_the_commit_landed()
    {
        var seq = await rig.WriteAsync("widget");
        await StallTheStandbyAsync();
        using var cancel = new CancellationTokenSource();
        var commit = CommitStarts(LogFaultPoints.RecordBeforeCommit, () => cancel.CancelAfter(TimeSpan.FromSeconds(1)));

        var record = rig.RecordAsync(seq, seq, cancellationToken: cancel.Token);

        var error = await Record.ExceptionAsync(() => record.WaitAsync(TimeSpan.FromSeconds(30)));
        output.WriteLine($"the record threw {error?.GetType().Name}: {error?.Message}");
        Assert.True(Stopwatch.GetElapsedTime(await commit.Task) >= TimeSpan.FromMilliseconds(900), "The commit did not wait for the standby.");
        Assert.IsAssignableFrom<OperationCanceledException>(error);
        Assert.Equal(seq, await rig.AckedSeqAsync());
    }

    [Fact]
    public async Task I33_a_second_close_of_a_closed_skip_commits_and_answers_only_after_the_standby()
    {
        var opened = Assert.IsType<SkipOpenResult.Opened>(await rig.OpenAsync("widget", 1));
        var first = await rig.Skip.CloseAsync(opened.SkipId, "first");
        await StallTheStandbyAsync();
        rig.DataSource.Source.DataSource.Clear();

        var second = rig.Skip.CloseAsync(opened.SkipId, "second");

        await UntilTheCallWaitsAsync();
        var ended = await Task.WhenAny(second, Task.Delay(NoAnswer));
        await SetStandbyAsync(string.Empty);
        Assert.NotSame(second, ended);
        Assert.Equal(first, await second.WaitAsync(TimeSpan.FromSeconds(30)));
    }

    private TaskCompletionSource<long> CommitStarts(FaultPoint point, Action? then = null)
    {
        var started = new TaskCompletionSource<long>(TaskCreationOptions.RunContinuationsAsynchronously);
        rig.Faults.Observe(point, _ =>
        {
            started.TrySetResult(Stopwatch.GetTimestamp());
            then?.Invoke();
            return ValueTask.CompletedTask;
        });
        return started;
    }

    private async Task AssertNoAnswerAsync(Task call, Task<long> commitStarted)
    {
        await commitStarted.WaitAsync(TimeSpan.FromSeconds(30));
        await UntilTheCallWaitsAsync();
        var ended = await Task.WhenAny(call, Task.Delay(NoAnswer));
        output.WriteLine(ended == call ? $"the call ended: {call.Status}, {call.Exception?.GetBaseException().Message}" : $"no answer within {NoAnswer}");
        Assert.NotSame(call, ended);
        await SetStandbyAsync(string.Empty);
    }

    private Task UntilTheCallWaitsAsync() =>
        SyncStandby.UntilAWaiterAsync(postgres, new NpgsqlConnectionStringBuilder(rig.ConnectionString).Database!);

    private async Task StallTheStandbyAsync()
    {
        var database = new NpgsqlConnectionStringBuilder(rig.ConnectionString).Database;
        await using (var connection = await rig.Log.OpenAsync())
        {
            await using var alter = new NpgsqlCommand($"ALTER DATABASE \"{database}\" SET synchronous_commit = on", connection);
            await alter.ExecuteNonQueryAsync();
        }

        await SyncStandby.StallAsync(postgres);
    }

    private Task SetStandbyAsync(string names) => SyncStandby.SetAsync(postgres, names);
}
