using System.Collections.Concurrent;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using Microsoft.Extensions.Time.Testing;
using Reeve.Cluster;
using Reeve.Cluster.Core;
using Reeve.Cluster.Membership;

namespace Unit.Membership;

public sealed class RenewalLoopTests : IAsyncDisposable
{
    private const long Epoch = 7;
    private const long Timeline = 1;

    private readonly FakeTimeProvider time = new();
    private readonly RecordingModelEvents events = new();
    private readonly ScriptedMemberStore store = new();
    private readonly LocalPhaseGate gate;
    private readonly RenewalLoop loop;
    private readonly List<FenceRequest> fences = [];

    public RenewalLoopTests()
    {
        var configured = MembershipTestOptions.Default();
        configured.RenewTimeout = TimeSpan.FromSeconds(2);
        var options = Options.Create(configured);
        gate = new LocalPhaseGate(options, time, events, NullLogger<LocalPhaseGate>.Instance);
        gate.OnFenceRequested(fences.Add);
        loop = new RenewalLoop(gate, store, options, time, NoFaultPoints.Instance, events, NullLogger<RenewalLoop>.Instance);
    }

    public async ValueTask DisposeAsync()
    {
        store.ReleaseAll();
        await loop.DisposeAsync();
        gate.Dispose();
    }

    private static TimeSpan S(double seconds) => TimeSpan.FromSeconds(seconds);

    private void Join()
    {
        Assert.True(gate.Joined(Epoch, Timeline, gate.Now));
        loop.Restart();
    }

    [Fact]
    public async Task I23_a_one_row_answer_on_the_joined_timeline_within_RenewTimeout_advances_the_deadline_from_t0()
    {
        Join();
        time.Advance(S(3));
        var attempt = Assert.Single(store.Attempts);
        Assert.Equal(Epoch, attempt.Epoch);

        time.Advance(S(1));
        attempt.Answer(new RenewalAnswer(1, Timeline));
        await loop.WhenIdle();

        Assert.Equal(S(3) + S(27), gate.Deadline);
        Assert.Equal(
            [MembershipActions.Join, MembershipActions.RenewSend, MembershipActions.RenewExec, MembershipActions.RenewAnswer],
            events.All.Select(e => e.Action));
        Assert.Equal(Epoch, Assert.Single(events.Of(MembershipActions.RenewSend)).State!["epoch"]);
        var exec = Assert.Single(events.Of(MembershipActions.RenewExec));
        Assert.Equal(Epoch, exec.State!["epoch"]);
        Assert.Equal(1, exec.State["rows"]);
        Assert.Equal(Timeline, exec.State["timeline"]);
        var answer = Assert.Single(events.Of(MembershipActions.RenewAnswer));
        Assert.Equal(Epoch, answer.State!["epoch"]);
        Assert.Equal("one", answer.State["res"]);
    }

    [Fact]
    public void Attempts_run_every_RenewInterval_and_the_timer_never_waits_for_the_previous_attempt()
    {
        store.IgnoreCancel = true;
        Join();

        time.Advance(S(3));
        time.Advance(S(3));
        time.Advance(S(3));

        Assert.Equal(3, store.Attempts.Count);
        Assert.Equal(3, loop.InFlight);
        Assert.Equal(S(27), gate.Deadline);
    }

    [Fact]
    public async Task I23_I33_one_row_returned_after_the_cancel_at_RenewTimeout_is_not_counted()
    {
        store.ReturnRowOnCancel = true;
        Join();
        time.Advance(S(3));
        Assert.Single(store.Attempts);

        time.Advance(S(2));
        await loop.WhenIdle();

        var exec = Assert.Single(events.Of(MembershipActions.RenewExec));
        Assert.Equal(1, exec.State!["rows"]);
        Assert.Empty(events.Of(MembershipActions.RenewAnswer));
        Assert.Equal(S(27), gate.Deadline);
        Assert.Equal(ReplicaPhase.Loading, gate.Phase);
    }

    [Fact]
    public async Task I23_an_answer_just_inside_RenewTimeout_counts_and_one_just_after_does_not()
    {
        Join();
        time.Advance(S(3));
        time.Advance(S(2) - TimeSpan.FromTicks(1));
        store.Attempts[0].Answer(new RenewalAnswer(1, Timeline));
        await loop.WhenIdle();
        Assert.Equal(S(3) + S(27), gate.Deadline);

        time.Advance(S(1) + TimeSpan.FromTicks(1));
        time.Advance(S(2));
        store.Attempts[1].Answer(new RenewalAnswer(1, Timeline));
        await loop.WhenIdle();
        Assert.Equal(S(3) + S(27), gate.Deadline);
    }

    [Fact]
    public async Task I23_a_zero_row_renewal_fences_at_once_skipping_step_3()
    {
        Join();
        time.Advance(S(3));
        store.Attempts[0].Answer(new RenewalAnswer(0, null));
        await loop.WhenIdle();

        Assert.Equal(ReplicaPhase.Fencing, gate.Phase);
        Assert.Equal(FenceTrigger.ZeroRowRenewal, Assert.Single(fences).Trigger);
        Assert.Equal(S(27), gate.Deadline);
    }

    [Fact]
    public async Task I32_a_one_row_answer_on_another_timeline_raises_the_fence_trigger_and_does_not_advance_the_deadline()
    {
        Join();
        time.Advance(S(3));
        store.Attempts[0].Answer(new RenewalAnswer(1, Timeline + 1));
        await loop.WhenIdle();

        Assert.Equal(ReplicaPhase.Fencing, gate.Phase);
        Assert.Equal(FenceTrigger.Timeline, Assert.Single(fences).Trigger);
        Assert.Equal(S(27), gate.Deadline);
    }

    [Fact]
    public async Task I23_a_database_error_is_no_answer_and_the_deadline_stands()
    {
        Join();
        time.Advance(S(3));
        store.Attempts[0].Fail(new TimeoutException("network"));
        await loop.WhenIdle();

        Assert.Equal(S(27), gate.Deadline);
        Assert.Equal(ReplicaPhase.Loading, gate.Phase);
    }

    [Fact]
    public async Task I23_no_attempt_starts_after_fence_step_0_and_an_answer_in_flight_at_step_0_counts_for_nothing()
    {
        Join();
        time.Advance(S(3));
        gate.RequestFence(new FenceRequest(FenceTrigger.FatalFault));

        store.Attempts[0].Answer(new RenewalAnswer(0, null));
        await loop.WhenIdle();
        time.Advance(S(3));

        Assert.Single(store.Attempts);
        Assert.Single(fences);
        Assert.Empty(events.Of(MembershipActions.RenewZeroL));
    }

    [Fact]
    public async Task I13_an_attempt_of_an_earlier_epoch_answering_after_a_rejoin_counts_for_nothing()
    {
        Join();
        time.Advance(S(3));
        var stale = store.Attempts[0];
        gate.RequestFence(new FenceRequest(FenceTrigger.FenceStart));
        gate.CompleteFence();
        gate.Apply(MemberTrigger.RejoinPermitted);
        Assert.True(gate.Joined(Epoch + 1, Timeline, gate.Now));

        stale.Answer(new RenewalAnswer(0, null));
        await loop.WhenIdle();

        Assert.Equal(ReplicaPhase.Loading, gate.Phase);
        Assert.Single(fences);
    }

    [Fact]
    public async Task I13_an_attempt_held_before_its_send_while_the_replica_rejoined_sends_nothing()
    {
        var points = new HeldPoint(MembershipFaultPoints.RenewSend.Name);
        var options = Options.Create(MembershipTestOptions.Default());
        await using var held = new RenewalLoop(gate, store, options, time, points, events, NullLogger<RenewalLoop>.Instance);
        Assert.True(gate.Joined(Epoch, Timeline, gate.Now));
        held.Restart();
        time.Advance(S(3));
        Assert.Equal(1, held.InFlight);

        gate.RequestFence(new FenceRequest(FenceTrigger.FenceStart));
        gate.CompleteFence();
        gate.Apply(MemberTrigger.RejoinPermitted);
        Assert.True(gate.Joined(Epoch + 1, Timeline, gate.Now));
        points.Release();
        await held.WhenIdle();

        Assert.Empty(store.Attempts);
        Assert.Empty(events.Of(MembershipActions.RenewSend));
    }

    private sealed class HeldPoint(string point) : IFaultPoints
    {
        private readonly TaskCompletionSource release = new(TaskCreationOptions.RunContinuationsAsynchronously);

        public void Release() => release.TrySetResult();

        public ValueTask ReachAsync(string reached, CancellationToken cancellationToken = default) =>
            reached == point ? new ValueTask(release.Task.WaitAsync(cancellationToken)) : ValueTask.CompletedTask;
    }
}

internal static class MembershipTestOptions
{
    public static ClusterOptions Default() => new()
    {
        ReplicaId = "r1",
        ConnectionString = "Host=unused",
        ReconcileTimeout = TimeSpan.FromSeconds(10),
    };
}

internal sealed class ScriptedMemberStore : IMemberStore
{
    private readonly ConcurrentQueue<Func<JoinRequest, JoinAnswer?>> joins = new();

    public List<RenewalCall> Attempts { get; } = [];

    public System.Threading.Channels.Channel<RenewalCall> AttemptStarted { get; } = System.Threading.Channels.Channel.CreateUnbounded<RenewalCall>();

    public List<JoinRequest> JoinRequests { get; } = [];

    public List<long> DrainingWrites { get; } = [];

    public SemaphoreSlim JoinCalled { get; } = new(0);

    public bool IgnoreCancel { get; set; }

    public bool ReturnRowOnCancel { get; set; }

    public System.Threading.Channels.Channel<PendingJoin>? PendingJoins { get; set; }

    public void ScriptJoins(params Func<JoinRequest, JoinAnswer?>[] answers)
    {
        foreach (var answer in answers)
        {
            joins.Enqueue(answer);
        }
    }

    public void ReleaseAll()
    {
        lock (Attempts)
        {
            foreach (var attempt in Attempts)
            {
                attempt.Fail(new OperationCanceledException());
            }
        }
    }

    public Task<JoinAnswer?> JoinAsync(JoinRequest request, CancellationToken cancellationToken)
    {
        lock (JoinRequests)
        {
            JoinRequests.Add(request);
        }

        if (PendingJoins is { } pending)
        {
            var call = new PendingJoin(request);
            pending.Writer.TryWrite(call);
            return call.Task;
        }

        var answer = joins.TryDequeue(out var next) ? next(request) : null;
        JoinCalled.Release();
        return Task.FromResult(answer);
    }

    public Task<RenewalAnswer> RenewAsync(string replicaId, long epoch, TimeSpan leaseTtl, CancellationToken cancellationToken)
    {
        var call = new RenewalCall(epoch);
        lock (Attempts)
        {
            Attempts.Add(call);
        }
        AttemptStarted.Writer.TryWrite(call);

        if (!IgnoreCancel)
        {
            cancellationToken.Register(() =>
            {
                if (ReturnRowOnCancel)
                {
                    call.Answer(new RenewalAnswer(1, 1));
                }
                else
                {
                    call.Fail(new OperationCanceledException(cancellationToken));
                }
            });
        }

        return call.Task;
    }

    public List<long> ServingSinceClears { get; } = [];

    public Action<string>? OnStatement { get; set; }

    public Exception? DrainingFailure { get; set; }

    public bool DrainingRowGone { get; set; }

    public Task<bool> WriteDrainingAsync(string replicaId, long epoch, CancellationToken cancellationToken)
    {
        lock (DrainingWrites)
        {
            DrainingWrites.Add(epoch);
        }

        OnStatement?.Invoke("draining");
        return DrainingFailure is { } failure ? Task.FromException<bool>(failure) : Task.FromResult(!DrainingRowGone);
    }

    public Task<bool> ClearServingSinceAsync(string replicaId, long epoch, CancellationToken cancellationToken)
    {
        lock (ServingSinceClears)
        {
            ServingSinceClears.Add(epoch);
        }

        OnStatement?.Invoke("clear-serving-since");
        return Task.FromResult(true);
    }
}

internal sealed class PendingJoin(JoinRequest request)
{
    private readonly TaskCompletionSource<JoinAnswer?> answer = new();

    public JoinRequest Request { get; } = request;

    public Task<JoinAnswer?> Task => answer.Task;

    public void Answer(JoinAnswer? value) => answer.SetResult(value);

    public void Fail(Exception exception) => answer.SetException(exception);
}

internal sealed class RenewalCall(long epoch)
{
    private readonly TaskCompletionSource<RenewalAnswer> answer = new();

    public long Epoch { get; } = epoch;

    public Task<RenewalAnswer> Task => answer.Task;

    public void Answer(RenewalAnswer value) => answer.TrySetResult(value);

    public void Fail(Exception exception) => answer.TrySetException(exception);
}
