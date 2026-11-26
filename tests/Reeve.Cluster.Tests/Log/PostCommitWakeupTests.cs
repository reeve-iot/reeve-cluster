using Reeve.Cluster;
using Reeve.Cluster.Log;
using Reeve.Cluster.Wakeup;
using Unit.Generations;

namespace Unit.Log;

public sealed class PostCommitWakeupTests
{
    [Fact]
    public async Task After_a_commit_it_emits_WCommit_holds_at_its_point_and_then_wakes_the_appliers()
    {
        var channel = new RecordingChannel();
        var events = new RecordingModelEvents();
        var point = new HeldPoint(LogFaultPoints.AfterCommitBeforeWakeup);
        point.Arm();
        var hook = new PostCommitWakeup(channel, point, events);

        var running = hook.AfterCommitAsync(42, "1234").AsTask();
        await point.Reached;

        var commit = Assert.Single(events.Events);
        Assert.Equal(LogActions.WCommit, commit.Action);
        Assert.Equal(42L, commit.State![ObservationKeys.Seq]);
        Assert.Equal("1234", commit.State[ObservationKeys.Xid]);
        Assert.Empty(channel.Published);

        point.Release();
        await running;

        Assert.Equal([new Wakeup(WakeupKind.Appended, 42)], channel.Published);
    }

    [Fact]
    public async Task A_cancelled_caller_after_the_commit_gets_no_exception_and_the_appliers_are_still_woken()
    {
        var channel = new RecordingChannel();
        var point = new HeldPoint(LogFaultPoints.AfterCommitBeforeWakeup);
        point.Arm();
        var hook = new PostCommitWakeup(channel, point, NoModelEvents.Instance);
        using var cancellation = new CancellationTokenSource();

        var running = hook.AfterCommitAsync(7, "99", cancellation.Token).AsTask();
        await point.Reached;
        await cancellation.CancelAsync();
        await running;

        Assert.Equal([new Wakeup(WakeupKind.Appended, 7)], channel.Published);
    }

    [Fact]
    public async Task A_seq_is_positive()
    {
        var hook = new PostCommitWakeup(new RecordingChannel(), NoFaultPoints.Instance, NoModelEvents.Instance);

        await Assert.ThrowsAsync<ArgumentOutOfRangeException>(() => hook.AfterCommitAsync(0, null).AsTask());
    }
}
