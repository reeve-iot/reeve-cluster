using System.Diagnostics;
using Npgsql;
using Reeve.Cluster.Harness.Containers;
using Reeve.Cluster.Harness.Timing;
using Reeve.Cluster.Membership;
using Xunit.Abstractions;

namespace Int.Membership;

public sealed class MemberCommitTests : IClassFixture<PostgresFixture>, IAsyncLifetime
{
    private static readonly TimeSpan NoAnswer = TimeSpan.FromSeconds(5);

    private static readonly TimeSpan Wait = TimeSpan.FromSeconds(30);

    private readonly PostgresFixture postgres;
    private readonly ITestOutputHelper output;
    private MemberDatabase db = null!;

    public MemberCommitTests(PostgresFixture postgres, ITestOutputHelper output)
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

    private Task<JoinAnswer?> Join(CancellationToken cancellationToken = default) =>
        db.Store.JoinAsync(new JoinRequest("r1", TestProfile.LeaseTtl, MemberIdentity.ForThisProcess()), cancellationToken);

    [Fact]
    public async Task I33_a_Command_Timeout_in_the_connection_string_does_not_cancel_the_joins_wait_for_the_standby()
    {
        await StallTheStandbyAsync();

        var join = Join();

        await AssertNoAnswerAsync(join);
        var answer = await join.WaitAsync(TimeSpan.FromSeconds(30));
        Assert.NotNull(answer);
        Assert.Equal(answer.Value.Epoch, (await db.ReadRowAsync("r1")).Epoch);
    }

    [Fact]
    public async Task I33_a_Command_Timeout_in_the_connection_string_does_not_cancel_the_draining_writes_wait_for_the_standby()
    {
        await db.InsertRowAsync("r1", epoch: 40, "joining", leaseSeconds: 30);
        await StallTheStandbyAsync();

        var write = db.Store.WriteDrainingAsync("r1", 40, CancellationToken.None);

        await AssertNoAnswerAsync(write);
        Assert.True(await write.WaitAsync(TimeSpan.FromSeconds(30)));
        Assert.Equal("draining", (await db.ReadRowAsync("r1")).State);
    }

    [Fact]
    public async Task I33_a_draining_write_whose_wait_for_the_standby_its_callers_token_cancels_throws_although_the_row_was_written()
    {
        await db.InsertRowAsync("r1", epoch: 40, "joining", leaseSeconds: 30);
        await StallTheStandbyAsync();
        using var cancel = new CancellationTokenSource();

        var write = db.Store.WriteDrainingAsync("r1", 40, cancel.Token);
        var clock = await CancelOnceTheCommitWaitsAsync(cancel);
        var error = await Record.ExceptionAsync(() => write.WaitAsync(Wait));

        output.WriteLine($"the write threw {error?.GetType().Name} after {clock.Elapsed}: {error?.Message}");
        Assert.IsAssignableFrom<OperationCanceledException>(error);
        Assert.True(clock.Elapsed >= TimeSpan.FromSeconds(1.9), $"The write ended after {clock.Elapsed}, before its token.");
        Assert.Equal("draining", (await db.ReadRowAsync("r1")).State);
    }

    [Fact]
    public async Task I33_M01_a_join_whose_wait_for_the_standby_its_callers_token_cancels_keeps_its_epoch()
    {
        await StallTheStandbyAsync();
        using var cancel = new CancellationTokenSource();

        var join = Join(cancel.Token);
        var clock = await CancelOnceTheCommitWaitsAsync(cancel);
        var answer = await join.WaitAsync(Wait);

        Assert.True(clock.Elapsed >= TimeSpan.FromSeconds(1.9), $"The join ended after {clock.Elapsed}, before its token.");
        Assert.NotNull(answer);
        Assert.Equal(answer.Value.Epoch, (await db.ReadRowAsync("r1")).Epoch);
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
