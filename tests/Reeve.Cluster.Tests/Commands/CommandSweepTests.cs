using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using Reeve.Cluster;
using Reeve.Cluster.Commands;
using Reeve.Cluster.Wakeup;
using Unit.Claims;

namespace Unit.Commands;

public sealed class CommandSweepTests
{
    private const string Kind = "scheduler";

    private readonly FakeCommandRows rows = new();
    private readonly InProcessWakeupChannel wakeups = new(NullLogger<InProcessWakeupChannel>.Instance);
    private readonly List<string?> nudged = [];

    public CommandSweepTests()
    {
        wakeups.Subscribe(WakeupKind.CommandNudge, w => nudged.Add(w.Target));
    }

    private CommandSweep Sweep(int limit = CommandSweep.PassLimit) => new(
        rows, wakeups, Options.Create(new ClusterOptions { ReplicaId = "a", FencedTransactionTimeout = TimeSpan.FromSeconds(1) }), rows.Time,
        NullLogger<CommandSweep>.Instance, limit);

    [Fact]
    public async Task A_pending_command_older_than_the_ttl_whose_key_has_no_owner_expires()
    {
        var command = rows.Add();
        rows.Time.Advance(TimeSpan.FromMinutes(11));

        var run = await Sweep().PassAsync(CancellationToken.None);

        Assert.Equal(1, run.Expired);
        Assert.Equal(CommandState.Expired, command.State);
        Assert.Null(command.TakenToken);
    }

    [Fact]
    public async Task A_pending_command_younger_than_the_ttl_does_not_expire()
    {
        var command = rows.Add();
        rows.Time.Advance(TimeSpan.FromMinutes(9));

        var run = await Sweep().PassAsync(CancellationToken.None);

        Assert.Equal(0, run.Expired);
        Assert.Equal(CommandState.Pending, command.State);
    }

    [Fact]
    public async Task A_slow_but_present_owners_backlog_is_not_expired()
    {
        var command = rows.Add();
        rows.ValidHolds.Add((Kind, "k1"));
        rows.Time.Advance(TimeSpan.FromHours(3));

        var run = await Sweep().PassAsync(CancellationToken.None);

        Assert.Equal(0, run.Expired);
        Assert.Equal(CommandState.Pending, command.State);
    }

    [Fact]
    public async Task A_command_whose_key_was_adopted_between_the_sweeps_read_and_its_statement_does_not_expire()
    {
        var adopted = rows.Add(key: "k1");
        var orphan = rows.Add(key: "k2");
        rows.Time.Advance(TimeSpan.FromMinutes(11));
        rows.BeforeExpire = id =>
        {
            if (id == adopted.Id)
            {
                rows.ValidHolds.Add((Kind, "k1"));
            }
        };

        var run = await Sweep().PassAsync(CancellationToken.None);

        Assert.Equal(1, run.Expired);
        Assert.Equal(CommandState.Pending, adopted.State);
        Assert.Equal(CommandState.Expired, orphan.State);
    }

    [Theory]
    [InlineData("taken")]
    [InlineData("done")]
    [InlineData("in_doubt")]
    [InlineData("expired")]
    public async Task Only_a_pending_command_expires(string sql)
    {
        var state = CommandStates.Parse(sql);
        var command = rows.Add(token: state is CommandState.Expired ? 0 : 5, state: state);
        rows.Time.Advance(TimeSpan.FromHours(1));

        var run = await Sweep().PassAsync(CancellationToken.None);

        Assert.Equal(0, run.Expired);
        Assert.Equal(state, command.State);
    }

    [Fact]
    public async Task An_expired_command_is_nudged_to_its_waiters_and_a_resend_pends_it_again()
    {
        var command = rows.Add();
        rows.Time.Advance(TimeSpan.FromMinutes(11));

        await Sweep().PassAsync(CancellationToken.None);

        Assert.Equal([CommandNudges.Completed(Kind, "k1")], nudged);
        Assert.True(await rows.ResendAsync(command.Id, CancellationToken.None));
        Assert.Equal(CommandState.Pending, command.State);
        var run = await Sweep().PassAsync(CancellationToken.None);
        Assert.Equal(0, run.Expired);
    }

    [Fact]
    public async Task A_pass_expires_at_most_its_limit_and_the_next_pass_does_the_rest()
    {
        var all = new[] { rows.Add(), rows.Add(), rows.Add() };
        rows.Time.Advance(TimeSpan.FromMinutes(11));

        var first = await Sweep(limit: 2).PassAsync(CancellationToken.None);
        var second = await Sweep(limit: 2).PassAsync(CancellationToken.None);

        Assert.Equal(2, first.Expired);
        Assert.Equal(1, second.Expired);
        Assert.All(all, c => Assert.Equal(CommandState.Expired, c.State));
    }

    [Fact]
    public async Task Two_copies_of_the_sweep_expire_each_command_once()
    {
        rows.Add();
        rows.Time.Advance(TimeSpan.FromMinutes(11));
        var one = Sweep();
        var other = Sweep();

        var runs = await Task.WhenAll(one.PassAsync(CancellationToken.None), other.PassAsync(CancellationToken.None));

        Assert.Equal(1, runs.Sum(r => r.Expired));
        Assert.Single(rows.Expired);
    }

    [Theory]
    [InlineData("done")]
    [InlineData("in_doubt")]
    [InlineData("expired")]
    public async Task Retention_deletes_a_finished_command_older_than_the_retention_and_keeps_a_younger_one(string sql)
    {
        var state = CommandStates.Parse(sql);
        var old = rows.Add(token: state is CommandState.Expired ? 0 : 5, state: state);
        old.CompletedAt = rows.Time.GetUtcNow();
        rows.Time.Advance(TimeSpan.FromHours(25));
        var young = rows.Add(token: state is CommandState.Expired ? 0 : 5, state: state);
        young.CompletedAt = rows.Time.GetUtcNow();
        rows.Time.Advance(TimeSpan.FromHours(1));

        var run = await Sweep().PassAsync(CancellationToken.None);

        Assert.Equal(1, run.Deleted);
        Assert.Equal([young], rows.Table);
    }

    [Theory]
    [InlineData("pending")]
    [InlineData("taken")]
    public async Task Retention_never_deletes_an_active_command(string sql)
    {
        var state = CommandStates.Parse(sql);
        rows.Add(token: state is CommandState.Pending ? 0 : 5, state: state);
        rows.Time.Advance(TimeSpan.FromDays(3));
        rows.ValidHolds.Add((Kind, "k1"));

        var run = await Sweep().PassAsync(CancellationToken.None);

        Assert.Equal(0, run.Deleted);
        Assert.Single(rows.Table);
    }

    [Fact]
    public async Task A_pass_whose_expire_half_cannot_reach_the_table_still_runs_retention_and_the_next_pass_expires()
    {
        var old = rows.Add(token: 5, state: CommandState.Done);
        old.CompletedAt = rows.Time.GetUtcNow();
        var pending = rows.Add(key: "k2");
        rows.Time.Advance(TimeSpan.FromHours(25));
        rows.ExpireFailure = new DbFailure("no answer");
        var sweep = Sweep();

        var run = await sweep.PassAsync(CancellationToken.None);

        Assert.Equal(0, run.Expired);
        Assert.Equal(1, run.Deleted);
        rows.ExpireFailure = null;
        run = await sweep.PassAsync(CancellationToken.None);
        Assert.Equal(1, run.Expired);
        Assert.Equal(CommandState.Expired, pending.State);
    }

    [Fact]
    public async Task A_row_whose_expire_fails_does_not_stop_the_rest_of_the_batch_or_the_nudges_of_the_rows_already_expired()
    {
        var first = rows.Add(key: "k1");
        var failing = rows.Add(key: "k2");
        var last = rows.Add(key: "k3");
        rows.Time.Advance(TimeSpan.FromMinutes(11));
        rows.BeforeExpire = id =>
        {
            if (id == failing.Id)
            {
                throw new DbFailure("no answer");
            }
        };

        var run = await Sweep().PassAsync(CancellationToken.None);

        Assert.Equal(2, run.Expired);
        Assert.Equal(CommandState.Expired, first.State);
        Assert.Equal(CommandState.Pending, failing.State);
        Assert.Equal(CommandState.Expired, last.State);
        Assert.Equal([CommandNudges.Completed(Kind, "k1"), CommandNudges.Completed(Kind, "k3")], nudged);
    }

    [Fact]
    public async Task A_pass_whose_retention_half_fails_still_expires()
    {
        var pending = rows.Add();
        rows.Time.Advance(TimeSpan.FromMinutes(11));
        rows.RetentionFailure = new DbFailure("no answer");

        var run = await Sweep().PassAsync(CancellationToken.None);

        Assert.Equal(1, run.Expired);
        Assert.Equal(0, run.Deleted);
        Assert.Equal(CommandState.Expired, pending.State);
    }

    [Fact]
    public async Task A_cancel_of_the_pass_still_throws()
    {
        rows.Add();
        rows.Time.Advance(TimeSpan.FromMinutes(11));
        using var cancelled = new CancellationTokenSource();
        await cancelled.CancelAsync();

        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => Sweep().PassAsync(cancelled.Token));
    }
}
