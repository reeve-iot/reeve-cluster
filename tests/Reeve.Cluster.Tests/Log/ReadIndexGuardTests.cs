using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using Microsoft.Extensions.Time.Testing;
using Reeve.Cluster;
using Reeve.Cluster.Core;
using Reeve.Cluster.Log;
using Reeve.Cluster.Membership;
using Reeve.Cluster.Wakeup;
using Unit.Generations;
using Unit.Testing;

namespace Unit.Log;

internal sealed class RiRig : IDisposable
{
    public static readonly TimeSpan Timeout = TimeSpan.FromSeconds(10);
    public static readonly TimeSpan Poll = TimeSpan.FromSeconds(1);
    public static readonly TimeSpan Wait = TimeSpan.FromSeconds(30);

    private static readonly LeaseTiming Timing = new(TimeSpan.FromSeconds(30), TimeSpan.FromSeconds(3), TimeSpan.FromSeconds(5), TimeSpan.FromSeconds(2));

    public RiRig()
    {
        FenceTimer.Arm();
        Gate = new LocalPhaseGate(Timing, Time, NoModelEvents.Instance, FenceTimer, NullLogger<LocalPhaseGate>.Instance);
        Source = new RiCountingSource(Generations);
        var options = new ClusterOptions { ReadIndexTimeout = Timeout, ApplyPollInterval = Poll };
        Index = new ReadIndex(Gate, Source, Store, Channel, Options.Create(options), Time, NoFaultPoints.Instance, Events, NullLogger<ReadIndex>.Instance);
    }

    public CountingTimeProvider Time { get; } = new();

    public RecordingModelEvents Events { get; } = new();

    public HeldPoint FenceTimer { get; } = new(MembershipFaultPoints.FenceBeforeStep0);

    public LocalPhaseGate Gate { get; }

    public GenerationStore Generations { get; } = Stores.Create("a", "b");

    public RiCountingSource Source { get; }

    public RiStore Store { get; } = new();

    public InProcessWakeupChannel Channel { get; } = new(NullLogger<InProcessWakeupChannel>.Instance);

    public ReadIndex Index { get; }

    public void Dispose()
    {
        Index.Dispose();
        Gate.Dispose();
    }

    public static Guid Token(long seq) => new((int)seq, 0, 0, [0, 0, 0, 0, 0, 0, 0, 0]);

    public void ToServing()
    {
        Assert.True(Gate.Joined(1, 1, Gate.Now));
        Gate.Apply(MemberTrigger.StateLoaded);
        Gate.Apply(MemberTrigger.ActivationCommitted);
        Gate.Apply(MemberTrigger.AppliedToActivationHead);
        Assert.Equal(ReplicaPhase.Serving, Gate.Phase);
    }

    public Task<IGeneration> PublishAsync(long head) => Generations.PublishAsync(head, Token(head), ("a", $"a{head}"), ("b", $"b{head}"));

    public Task NotifyAsync(long seq) => Channel.PublishAsync(new Wakeup(WakeupKind.Applied, seq)).AsTask();

    public async Task ReadsAsync(int n)
    {
        using var bound = new CancellationTokenSource(Wait);
        while (Store.Reads < n)
        {
            await Task.Delay(1, bound.Token);
        }
    }
}

internal sealed class RiCountingSource(IGenerationSource inner) : IGenerationSource
{
    private int reads;

    public int Reads => Volatile.Read(ref reads);

    public IGeneration? Current
    {
        get
        {
            Interlocked.Increment(ref reads);
            return inner.Current;
        }
    }

    public void EnsureNotInsideTheSwap() => inner.EnsureNotInsideTheSwap();
}

internal sealed class RiStore : IReadIndexStore
{
    private readonly TaskCompletionSource started = new(TaskCreationOptions.RunContinuationsAsynchronously);
    private readonly ManualResetEventSlim callbackGate = new(false);
    private readonly TaskCompletionSource callbackEntered = new(TaskCreationOptions.RunContinuationsAsynchronously);
    private TaskCompletionSource<ReadIndexRead>? blocked;
    private int reads;

    public long Head { get; set; }

    public Func<Cursor, bool> Valid { get; set; } = _ => true;

    public bool BlockNext { get; set; }

    public bool HoldCancelCallback { get; set; }

    public Task CancelCallbackEntered => callbackEntered.Task;

    public void ReleaseCancelCallback() => callbackGate.Set();

    public int Reads => Volatile.Read(ref reads);

    public Task Started => started.Task;

    public void Answer(long head, bool valid) => Volatile.Read(ref blocked)?.TrySetResult(new ReadIndexRead(head, valid));

    public async Task<ReadIndexRead> ReadAsync(Cursor cursor, CancellationToken cancellationToken)
    {
        Interlocked.Increment(ref reads);
        if (BlockNext)
        {
            BlockNext = false;
            var answer = new TaskCompletionSource<ReadIndexRead>(TaskCreationOptions.RunContinuationsAsynchronously);
            Volatile.Write(ref blocked, answer);
            using var registration = HoldCancelCallback
                ? cancellationToken.Register(() =>
                {
                    callbackEntered.TrySetResult();
                    callbackGate.Wait(TimeSpan.FromSeconds(60));
                })
                : default;
            started.TrySetResult();
            return await answer.Task;
        }

        return new ReadIndexRead(Head, Valid(cursor));
    }
}

public sealed class ReadIndexGuardTests : IDisposable
{
    private readonly RiRig rig = new();

    public void Dispose() => rig.Dispose();

    [Fact]
    public async Task the_head_is_read_once_and_the_wait_does_not_chase_a_moving_head()
    {
        rig.ToServing();
        await rig.PublishAsync(3);
        rig.Store.Head = 5;
        var scope = new PinScope();
        var entering = rig.Index.EnterAsync(scope, wait: true, CancellationToken.None);
        await rig.ReadsAsync(1);
        Assert.False(entering.IsCompleted);

        rig.Store.Head = 9;
        var caughtUp = await rig.PublishAsync(5);
        await rig.NotifyAsync(5);
        var result = await entering.WaitAsync(RiRig.Wait);

        Assert.Equal(ReadIndexOutcome.Pinned, result.Outcome);
        Assert.Same(caughtUp, scope.PinnedGeneration);
    }

    [Fact]
    public async Task a_replica_that_leaves_Serving_during_the_wait_refuses_at_the_next_wake_up()
    {
        rig.ToServing();
        await rig.PublishAsync(3);
        rig.Store.Head = 5;
        var scope = new PinScope();
        var entering = rig.Index.EnterAsync(scope, wait: true, CancellationToken.None);
        await rig.ReadsAsync(1);

        rig.Gate.RequestFence(new FenceRequest(FenceTrigger.ApplierStall));
        Assert.Equal(ReplicaPhase.Fencing, rig.Gate.Phase);
        await rig.NotifyAsync(3);
        var result = await entering.WaitAsync(RiRig.Wait);

        Assert.Equal(ReadIndexOutcome.NotServing, result.Outcome);
        Assert.Equal(ServingCheckOutcome.NotServing, result.Refusal!.Value.Outcome);
        Assert.Null(scope.PinnedGeneration);
        Assert.Equal(TimeSpan.Zero, rig.Time.GetUtcNow() - rig.Time.Start);
    }

    [Fact]
    public async Task one_notice_that_changes_nothing_costs_one_wake_up_and_the_wait_does_not_spin()
    {
        rig.ToServing();
        await rig.PublishAsync(3);
        rig.Store.Head = 5;
        var armed = rig.Time.TimersCreated;
        var entering = rig.Index.EnterAsync(new PinScope(), wait: true, CancellationToken.None);
        await rig.ReadsAsync(1);
        await rig.Time.TimersAsync(armed + 2);

        var before = rig.Source.Reads;
        const int Notices = 3;
        for (var i = 1; i <= Notices; i++)
        {
            await rig.NotifyAsync(3);
            await rig.Time.TimersAsync(armed + 2 + i);
        }

        var wakeUps = rig.Source.Reads - before;
        var completedEarly = entering.IsCompleted;
        var storeReads = rig.Store.Reads;

        await rig.PublishAsync(5);
        await rig.NotifyAsync(5);
        await entering.WaitAsync(RiRig.Wait);

        Assert.False(completedEarly);
        Assert.Equal(1, storeReads);
        Assert.True(wakeUps == Notices, $"{Notices} notices that changed nothing cost {wakeUps} reads of the current generation: a notice is one wake-up and the wait does not spin");
    }

    [Fact]
    public async Task after_NotServing_the_same_scope_pins_with_the_wait_off()
    {
        var current = await rig.PublishAsync(5);
        var scope = new PinScope();

        var refused = await rig.Index.EnterAsync(scope, wait: true, CancellationToken.None).WaitAsync(RiRig.Wait);
        Assert.Equal(ReadIndexOutcome.NotServing, refused.Outcome);
        Assert.Null(scope.PinnedGeneration);

        var pinned = await rig.Index.EnterAsync(scope, wait: false, CancellationToken.None).WaitAsync(RiRig.Wait);
        Assert.Equal(ReadIndexOutcome.Pinned, pinned.Outcome);
        Assert.Same(current, scope.PinnedGeneration);
    }
}

public sealed class ReadIndexBoundTests : IDisposable
{
    private readonly RiRig rig = new();

    public void Dispose() => rig.Dispose();

    [Fact]
    public async Task the_answer_comes_at_the_bound_while_a_read_has_not_returned()
    {
        rig.ToServing();
        await rig.PublishAsync(3);
        rig.Store.Head = 5;
        rig.Store.BlockNext = true;
        var scope = new PinScope();
        var entering = rig.Index.EnterAsync(scope, wait: true, CancellationToken.None);
        await rig.Store.Started.WaitAsync(RiRig.Wait);

        rig.Time.Advance(RiRig.Timeout + TimeSpan.FromSeconds(1));
        var result = await entering.WaitAsync(RiRig.Wait);
        rig.Store.Answer(5, valid: false);

        Assert.Equal(ReadIndexOutcome.Behind, result.Outcome);
    }

    [Fact]
    public async Task a_read_that_returns_valid_after_the_bound_fired_is_not_pinned()
    {
        rig.ToServing();
        await rig.PublishAsync(5);
        rig.Store.Head = 5;
        rig.Store.BlockNext = true;
        var scope = new PinScope();
        var entering = rig.Index.EnterAsync(scope, wait: true, CancellationToken.None);
        await rig.Store.Started.WaitAsync(RiRig.Wait);

        rig.Time.Advance(RiRig.Timeout + TimeSpan.FromSeconds(1));
        rig.Store.Answer(5, valid: true);
        var result = await entering.WaitAsync(RiRig.Wait);

        Assert.True(
            result.Outcome != ReadIndexOutcome.Pinned && scope.PinnedGeneration is null,
            $"the request answered {result.Outcome} at {(rig.Time.GetUtcNow() - rig.Time.Start).TotalSeconds:F1} s (fake), ReadIndexTimeout {RiRig.Timeout.TotalSeconds:F0} s; pinned: {scope.PinnedGeneration is not null}");
    }

    [Fact]
    public async Task the_bound_does_not_run_the_reads_cancel_callbacks_on_the_thread_that_fires_it()
    {
        rig.ToServing();
        await rig.PublishAsync(3);
        rig.Store.Head = 5;
        rig.Store.BlockNext = true;
        rig.Store.HoldCancelCallback = true;
        var entering = rig.Index.EnterAsync(new PinScope(), wait: true, CancellationToken.None);
        await rig.Store.Started.WaitAsync(RiRig.Wait);

        var advancing = Task.Run(() => rig.Time.Advance(RiRig.Timeout + TimeSpan.FromSeconds(1)));
        try
        {
            await advancing.WaitAsync(RiRig.Wait);
        }
        catch (TimeoutException)
        {
            Assert.Fail("firing the bound did not return while the read's cancel callback blocked: the callback ran on the thread that fired the bound");
        }
        finally
        {
            rig.Store.ReleaseCancelCallback();
        }

        await rig.Store.CancelCallbackEntered.WaitAsync(RiRig.Wait);
        rig.Store.Answer(5, valid: false);
        await entering.WaitAsync(RiRig.Wait);
    }

    [Fact]
    public async Task a_scope_that_pinned_already_is_refused_before_any_read()
    {
        rig.ToServing();
        var current = await rig.PublishAsync(5);
        rig.Store.Head = 5;
        var scope = new PinScope();
        scope.Pin(current);

        await Assert.ThrowsAsync<InvalidOperationException>(() => rig.Index.EnterAsync(scope, wait: true, CancellationToken.None).WaitAsync(RiRig.Wait));
        Assert.Equal(0, rig.Store.Reads);
    }
}
