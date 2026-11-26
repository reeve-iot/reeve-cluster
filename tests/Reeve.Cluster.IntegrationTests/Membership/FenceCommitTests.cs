using System.Diagnostics;
using Npgsql;
using Reeve.Cluster.Harness.Containers;
using Reeve.Cluster.Membership;
using Xunit.Abstractions;

namespace Int.Membership;

public sealed class FenceCommitTests : IClassFixture<PostgresFixture>, IAsyncLifetime
{
    private static readonly TimeSpan NoAnswer = TimeSpan.FromSeconds(5);

    private static readonly TimeSpan Wait = TimeSpan.FromSeconds(30);

    private static readonly FenceRecord Fence = new("r1", 40, "fence-start", TimeSpan.FromSeconds(12));

    private readonly PostgresFixture postgres;
    private readonly ITestOutputHelper output;
    private MemberDatabase db = null!;

    public FenceCommitTests(PostgresFixture postgres, ITestOutputHelper output)
    {
        this.postgres = postgres;
        this.output = output;
    }

    public async Task InitializeAsync() =>
        db = await MemberDatabase.CreateAsync(postgres, c => new NpgsqlConnectionStringBuilder(c) { CommandTimeout = 1 }.ConnectionString);

    public async Task DisposeAsync()
    {
        await SetStandbyAsync(string.Empty);
        await db.DisposeAsync();
    }

    [Fact]
    public async Task I33_a_Command_Timeout_in_the_connection_string_does_not_cancel_the_fence_records_wait_for_the_standby()
    {
        await StallTheStandbyAsync();

        var record = db.Fences.RecordFenceAsync(Fence, TimeSpan.Zero, CancellationToken.None);

        await AssertNoAnswerAsync(record);
        await record.WaitAsync(TimeSpan.FromSeconds(30));
        Assert.Equal(40, Assert.Single(await db.ReadFenceHistoryAsync("r1")).Epoch);
    }

    [Fact]
    public async Task I33_a_fence_record_whose_wait_for_the_standby_its_callers_token_cancels_throws_although_the_row_was_written()
    {
        await StallTheStandbyAsync();
        using var cancel = new CancellationTokenSource();

        var record = db.Fences.RecordFenceAsync(Fence, TimeSpan.Zero, cancel.Token);
        var clock = await CancelOnceTheCommitWaitsAsync(cancel);
        var error = await Record.ExceptionAsync(() => record.WaitAsync(Wait));

        output.WriteLine($"the record threw {error?.GetType().Name} after {clock.Elapsed}: {error?.Message}");
        Assert.IsAssignableFrom<OperationCanceledException>(error);
        Assert.True(clock.Elapsed >= TimeSpan.FromSeconds(1.9), $"The record ended after {clock.Elapsed}, before its token.");

        Assert.Equal(40, Assert.Single(await db.ReadFenceHistoryAsync("r1")).Epoch);
    }

    private async Task AssertNoAnswerAsync(Task call)
    {
        await UntilTheCommitWaitsAsync();
        var ended = await Task.WhenAny(call, Task.Delay(NoAnswer));
        output.WriteLine(ended == call ? $"the call ended: {call.Status}, {call.Exception?.GetBaseException().Message}" : $"no answer within {NoAnswer}");
        var waiting = await SyncStandby.WaitersAsync(postgres, Database);
        await SetStandbyAsync(string.Empty);
        Assert.NotSame(call, ended);
        Assert.Equal(1, waiting);
    }

    private string Database => new NpgsqlConnectionStringBuilder(db.ConnectionString).Database!;

    private Task UntilTheCommitWaitsAsync() => SyncStandby.UntilAWaiterAsync(postgres, Database);

    private async Task<Stopwatch> CancelOnceTheCommitWaitsAsync(CancellationTokenSource cancel)
    {
        await UntilTheCommitWaitsAsync();
        var clock = Stopwatch.StartNew();
        cancel.CancelAfter(TimeSpan.FromSeconds(2));
        return clock;
    }

    private async Task StallTheStandbyAsync()
    {
        await db.ExecuteAsync($"ALTER DATABASE \"{Database}\" SET synchronous_commit = on");
        await SyncStandby.StallAsync(postgres);
    }

    private Task SetStandbyAsync(string names) => SyncStandby.SetAsync(postgres, names);
}
