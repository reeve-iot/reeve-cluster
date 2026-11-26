using System.Diagnostics.CodeAnalysis;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using Reeve.Cluster;
using Reeve.Cluster.Ask;
using Reeve.Cluster.Core;
using Reeve.Cluster.Log;
using Reeve.Cluster.Membership;
using Unit.Generations;
using Unit.Testing;

namespace Unit.Ask;

public sealed class AskBoundaryTests : IDisposable
{
    private const string Kind = "scheduler";
    private const string Key = "k1";

    private static readonly LeaseTiming Timing = new(TimeSpan.FromSeconds(30), TimeSpan.FromSeconds(3), TimeSpan.FromSeconds(5), TimeSpan.FromSeconds(2));
    private static readonly TimeSpan PastFenceStart = TimeSpan.FromSeconds(23);
    private static readonly byte[] Request = [9];

    private readonly CountingTimeProvider time = new();
    private readonly HeldPoint fenceTimer = new(MembershipFaultPoints.FenceBeforeStep0);
    private readonly LocalPhaseGate gate;
    private readonly FenceSteps steps = new();
    private readonly ClusterScopeSource scopes;
    private readonly CommitRefusal refusal;
    private readonly ClusterGenerations generations;
    private readonly GenerationStore store = Stores.Create("a");
    private readonly FakeTenures tenures = new();
    private readonly MemberIdentity self = new(Guid.NewGuid(), "1.0.0", MemberIdentity.CurrentProtocol, []);

    public AskBoundaryTests()
    {
        fenceTimer.Arm();
        gate = new LocalPhaseGate(Timing, time, NoModelEvents.Instance, fenceTimer, NullLogger<LocalPhaseGate>.Instance);
        scopes = new ClusterScopeSource(steps);
        refusal = new CommitRefusal(gate, steps);
        generations = new ClusterGenerations(store, gate, scopes);
    }

    public void Dispose()
    {
        refusal.Dispose();
        fenceTimer.Release();
        gate.Dispose();
    }

    private void ToServing(long epoch = 1)
    {
        Assert.True(gate.Joined(epoch, 1, gate.Now));
        gate.Apply(MemberTrigger.StateLoaded);
        gate.Apply(MemberTrigger.ActivationCommitted);
        gate.Apply(MemberTrigger.AppliedToActivationHead);
        Assert.Equal(ReplicaPhase.Serving, gate.Phase);
    }

    private void ToFenced()
    {
        gate.RequestFence(new FenceRequest(FenceTrigger.FenceStart));
        refusal.Start();
        Assert.Equal(ReplicaPhase.Fenced, gate.CompleteFence());
    }

    private AskInbound Inbound(IAskHandler? member = null, params (string Kind, IAskHandler Handler)[] keyed) =>
        new(gate, scopes, tenures, new AskHandlers(keyed.ToDictionary(k => k.Kind, k => k.Handler, StringComparer.Ordinal), member), NullLogger<AskInbound>.Instance);

    private static AskDelivery Keyed(long token) => new(Kind, Key, token, Request);

    private static AskDelivery Member() => new(null, null, null, Request);

    private static IAskHandler Echo() => new LambdaHandler((request, _) => Task.FromResult<ReadOnlyMemory<byte>>(request.Payload.ToArray()));

    [Fact]
    public async Task A_reservation_starts_no_handler_until_it_is_started()
    {
        ToServing();
        tenures.Add(Kind, Key, 7);
        var started = 0;
        var handler = new LambdaHandler((_, _) =>
        {
            Interlocked.Increment(ref started);
            return Task.FromResult<ReadOnlyMemory<byte>>(Request);
        });

        var keyed = await Inbound(null, (Kind, handler)).ReserveAsync(Keyed(7), CancellationToken.None);
        var member = await Inbound(handler).ReserveAsync(Member(), CancellationToken.None);

        Assert.True(keyed.IsReserved);
        Assert.True(member.IsReserved);
        await Task.Delay(50);
        Assert.Equal(0, started);

        var answer = await keyed.Start().WaitAsync(TimeSpan.FromSeconds(30));
        Assert.Equal(AskAnswerKind.Response, answer.Kind);
        Assert.Equal(1, started);
        Assert.Throws<InvalidOperationException>(() => { _ = keyed.Start(); });
    }

    [Fact]
    public async Task A_declined_reservation_has_no_handler_to_start_and_the_reasons_are_the_takes()
    {
        ToServing();
        tenures.Add(Kind, Key, 7);
        var inbound = Inbound(null, (Kind, Echo()));
        using var ended = new CancellationTokenSource();
        await ended.CancelAsync();

        var wrongToken = await inbound.ReserveAsync(Keyed(8), CancellationToken.None);
        var waitEnded = await inbound.ReserveAsync(Keyed(7), ended.Token);
        var noMemberHandler = await inbound.ReserveAsync(Member(), CancellationToken.None);

        Assert.False(wrongToken.IsReserved);
        Assert.Equal(DeclineReason.Absent, wrongToken.Reason);
        Assert.Equal(DeclineReason.WaitEnded, waitEnded.Reason);
        Assert.Equal(DeclineReason.NoHandler, noMemberHandler.Reason);
        Assert.Throws<InvalidOperationException>(() => { _ = wrongToken.Start(); });
    }

    [Fact]
    public async Task A_reservation_is_decided_by_the_serving_check_before_it_is_started()
    {
        ToServing();
        tenures.Add(Kind, Key, 7);
        time.Advance(PastFenceStart);

        var reservation = await Inbound(null, (Kind, Echo())).ReserveAsync(Keyed(7), CancellationToken.None);

        Assert.False(reservation.IsReserved);
        Assert.Equal(DeclineReason.NotServing, reservation.Reason);
    }

    [Fact]
    public async Task A_serving_owner_with_the_tenure_takes_a_keyed_request_and_the_handler_answers()
    {
        ToServing();
        tenures.Add(Kind, Key, 7);

        var taking = await Inbound(null, (Kind, Echo())).TakeAsync(Keyed(7), CancellationToken.None);

        Assert.True(taking.IsTaken);
        var answer = await taking.Answer.WaitAsync(TimeSpan.FromSeconds(30));
        Assert.Equal(AskAnswerKind.Response, answer.Kind);
        Assert.Equal(Request, answer.Response.ToArray());
    }

    [Fact]
    public async Task A_target_past_fence_start_declines_a_keyed_request_and_starts_the_fence()
    {
        ToServing();
        tenures.Add(Kind, Key, 7);
        time.Advance(PastFenceStart);
        Assert.Equal(ReplicaPhase.Serving, gate.Phase);

        var taking = await Inbound(null, (Kind, Echo())).TakeAsync(Keyed(7), CancellationToken.None);

        Assert.False(taking.IsTaken);
        Assert.Equal(ReplicaPhase.Fencing, gate.Phase);
    }

    [Theory]
    [InlineData(ReplicaPhase.Starting)]
    [InlineData(ReplicaPhase.Activated)]
    [InlineData(ReplicaPhase.Fenced)]
    public async Task A_replica_that_does_not_serve_declines_a_keyed_request(ReplicaPhase phase)
    {
        tenures.Add(Kind, Key, 7);
        switch (phase)
        {
            case ReplicaPhase.Activated:
                Assert.True(gate.Joined(1, 1, gate.Now));
                gate.Apply(MemberTrigger.StateLoaded);
                gate.Apply(MemberTrigger.ActivationCommitted);
                break;
            case ReplicaPhase.Fenced:
                ToServing();
                ToFenced();
                break;
        }

        Assert.Equal(phase, gate.Phase);

        var taking = await Inbound(null, (Kind, Echo())).TakeAsync(Keyed(7), CancellationToken.None);

        Assert.False(taking.IsTaken);
    }

    [Fact]
    public async Task A_member_request_is_taken_in_every_phase_a_fenced_replica_included()
    {
        var inbound = Inbound(Echo());
        Assert.True((await inbound.TakeAsync(Member(), CancellationToken.None)).IsTaken);

        ToServing();
        Assert.True((await inbound.TakeAsync(Member(), CancellationToken.None)).IsTaken);

        ToFenced();
        Assert.Equal(ReplicaPhase.Fenced, gate.Phase);
        var taking = await inbound.TakeAsync(Member(), CancellationToken.None);

        Assert.True(taking.IsTaken);
        Assert.Equal(AskAnswerKind.Response, (await taking.Answer.WaitAsync(TimeSpan.FromSeconds(30))).Kind);
    }

    [Fact]
    public async Task A_keyed_request_for_a_tenure_this_replica_does_not_hold_is_declined()
    {
        ToServing();
        tenures.Add(Kind, Key, 7);
        var inbound = Inbound(null, (Kind, Echo()));

        Assert.False((await inbound.TakeAsync(Keyed(8), CancellationToken.None)).IsTaken);
        Assert.False((await inbound.TakeAsync(new AskDelivery(Kind, "other", 7, Request), CancellationToken.None)).IsTaken);
    }

    [Fact]
    public async Task A_keyed_request_for_a_tenure_that_is_stopping_is_declined()
    {
        ToServing();
        tenures.Add(Kind, Key, 7).Cancel();

        var taking = await Inbound(null, (Kind, Echo())).TakeAsync(Keyed(7), CancellationToken.None);

        Assert.False(taking.IsTaken);
        Assert.Equal("Stopping", taking.Reason.ToString());
    }

    [Fact]
    public async Task The_reason_of_a_decline_is_the_first_check_that_failed()
    {
        ToServing();
        tenures.Add(Kind, Key, 7);
        var inbound = Inbound(null, (Kind, Echo()));

        Assert.Equal("Absent", (await inbound.TakeAsync(Keyed(8), CancellationToken.None)).Reason.ToString());

        time.Advance(PastFenceStart);
        Assert.Equal("NotServing", (await inbound.TakeAsync(Keyed(7), CancellationToken.None)).Reason.ToString());
    }

    [Fact]
    public async Task A_request_nothing_is_registered_to_handle_is_declined()
    {
        ToServing();
        tenures.Add(Kind, Key, 7);

        Assert.False((await Inbound().TakeAsync(Keyed(7), CancellationToken.None)).IsTaken);
        Assert.False((await Inbound().TakeAsync(Member(), CancellationToken.None)).IsTaken);
    }

    [Fact]
    public async Task A_handler_exception_is_an_answer_carrying_the_error()
    {
        ToServing();
        var failing = new LambdaHandler((_, _) => throw new InvalidOperationException("boom"));

        var taking = await Inbound(failing).TakeAsync(Member(), CancellationToken.None);

        var answer = await taking.Answer.WaitAsync(TimeSpan.FromSeconds(30));
        Assert.Equal(AskAnswerKind.HandlerError, answer.Kind);
        Assert.Contains("boom", answer.Error);
    }

    [Fact]
    public async Task A_tenure_that_stops_under_the_handler_is_OwnerLost_without_waiting_for_the_handler()
    {
        ToServing();
        var stop = tenures.Add(Kind, Key, 7);
        var started = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var hung = new LambdaHandler(async (_, ct) =>
        {
            started.SetResult();
            await Task.Delay(Timeout.Infinite, ct);
            return default;
        });

        var taking = await Inbound(null, (Kind, hung)).TakeAsync(Keyed(7), CancellationToken.None);
        await started.Task.WaitAsync(TimeSpan.FromSeconds(30));
        await stop.CancelAsync();

        Assert.Equal(AskAnswerKind.OwnerLost, (await taking.Answer.WaitAsync(TimeSpan.FromSeconds(30))).Kind);
    }

    [Fact]
    public async Task A_member_request_has_no_tenure_to_lose()
    {
        ToServing();
        var release = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var slow = new LambdaHandler(async (_, _) =>
        {
            await release.Task;
            return new byte[] { 1 };
        });

        var taking = await Inbound(slow).TakeAsync(Member(), CancellationToken.None);
        release.SetResult();

        Assert.Equal(AskAnswerKind.Response, (await taking.Answer.WaitAsync(TimeSpan.FromSeconds(30))).Kind);
    }

    private sealed record Seen(IClusterScope? Scope, bool WasRefused, IGeneration? Pin, string? Marker, ClusterScopeKind? Kind);

    private ClusterAsk LocalAsk(IAskHandler member, ScriptedDirectory directory) => new(
        directory,
        Inbound(member),
        new ScriptedTransport(),
        self,
        [],
        Options.Create(new ClusterOptions { ReplicaId = "a" }),
        time,
        NullLogger<ClusterAsk>.Instance);

    private (LambdaHandler Handler, Func<Seen> Seen, Func<IClusterScope?> Own) Writer(AsyncLocal<string?> marker)
    {
        Seen? seen = null;
        IClusterScope? own = null;
        var handler = new LambdaHandler((_, _) =>
        {
            var writer = scopes.CurrentWriter;
            own = writer;
            seen = new Seen(writer, refusal.Refuses(writer), generations.TryPin(), marker.Value, writer?.Kind);
            writer?.RecordSeq(42);
            return Task.FromResult<ReadOnlyMemory<byte>>(new byte[] { 1 });
        });
        return (handler, () => seen!, () => own);
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task A_handler_dispatched_locally_from_a_request_running_at_the_fence_has_a_scope_of_its_own(bool fenced)
    {
        ToServing();
        var pinned = await store.PublishAsync(1, null, ("a", "a1"));
        var marker = new AsyncLocal<string?>();
        var (handler, seen, own) = Writer(marker);
        var directory = new ScriptedDirectory().Member("a", new AskMemberRow(self.ProcessId, MemberRowState.Active));

        using var request = scopes.Begin(ClusterScopeKind.ClientRequest);
        scopes.CurrentWriter!.Pin(pinned);
        var newer = await store.PublishAsync(2, null, ("a", "a2"));
        marker.Value = "the caller's";
        if (fenced)
        {
            ToFenced();
            Assert.True(refusal.Refuses(scopes.CurrentWriter));
        }

        var result = await LocalAsk(handler, directory).AskMemberAsync("a", Request, TimeSpan.FromSeconds(5));

        Assert.Equal(AskOutcome.Answered, result.Outcome);

        var inside = seen();
        Assert.NotNull(inside.Scope);
        Assert.NotSame(request.Scope, inside.Scope);
        Assert.Equal(ClusterScopeKind.Background, inside.Kind);
        Assert.Null(inside.Marker);

        Assert.False(inside.WasRefused);
        if (fenced)
        {
            Assert.Null(inside.Pin);
        }
        else
        {
            Assert.Same(newer, inside.Pin);
        }

        Assert.Equal([42L], own()!.Seqs);
        Assert.Empty(request.Scope.Seqs);
        Assert.False(request.Scope.CommitRefused);
        Assert.Same(request.Scope, scopes.Current);
        Assert.Equal("the caller's", marker.Value);
        Assert.True(((IClusterScopeWriter)own()!).Ended);
    }

    [Fact]
    public async Task A_keyed_handler_dispatched_locally_has_a_scope_of_its_own_too()
    {
        ToServing();
        tenures.Add(Kind, Key, 7);
        var marker = new AsyncLocal<string?>();
        var (handler, seen, own) = Writer(marker);
        var directory = new ScriptedDirectory().Then(new AskOwner("a", self.ProcessId, 7));
        var ask = new ClusterAsk(
            directory,
            Inbound(null, (Kind, handler)),
            new ScriptedTransport(),
            self,
            [],
            Options.Create(new ClusterOptions { ReplicaId = "a" }),
            time,
            NullLogger<ClusterAsk>.Instance);

        using var request = scopes.Begin(ClusterScopeKind.ClientRequest);
        marker.Value = "the caller's";
        var result = await ask.AskAsync(Kind, Key, Request, TimeSpan.FromSeconds(5));

        Assert.Equal(AskOutcome.Answered, result.Outcome);
        Assert.NotSame(request.Scope, seen().Scope);
        Assert.Null(seen().Marker);
        Assert.Empty(request.Scope.Seqs);
        Assert.Equal([42L], own()!.Seqs);
    }

    [Fact]
    public async Task A_handler_started_with_the_callers_flow_already_suppressed_still_has_a_scope_of_its_own()
    {
        ToServing();
        var (handler, seen, _) = Writer(new AsyncLocal<string?>());

        using var request = scopes.Begin(ClusterScopeKind.ClientRequest);
        ValueTask<AskTaking> pending;
        using (ExecutionContext.SuppressFlow())
        {
            pending = Inbound(handler).TakeAsync(Member(), CancellationToken.None);
        }

        var taking = await pending;
        await taking.Answer.WaitAsync(TimeSpan.FromSeconds(30));
        Assert.NotSame(request.Scope, seen().Scope);
    }
}

internal sealed class FakeTenures : IRunningTenures
{
    private readonly Dictionary<(string Kind, string Key, long Token), CancellationTokenSource> held = [];
    private readonly Dictionary<(string Kind, string Key, long Token), OwnedContext> contexts = [];

    public CancellationTokenSource Add(string kind, string key, long token)
    {
        var stop = new CancellationTokenSource();
        held[(kind, key, token)] = stop;
        contexts[(kind, key, token)] = new FakeContext(kind, key, token);
        return stop;
    }

    public OwnedContext ContextOf(string kind, string key, long token) => contexts[(kind, key, token)];

    public bool TryGetRunning(string kind, string key, long token, out CancellationToken stopped, [NotNullWhen(true)] out OwnedContext? owner)
    {
        if (held.TryGetValue((kind, key, token), out var source) && !source.IsCancellationRequested)
        {
            stopped = source.Token;
            owner = contexts[(kind, key, token)];
            return true;
        }

        stopped = default;
        owner = null;
        return false;
    }

    public bool IsStopping(string kind, string key, long token) =>
        held.TryGetValue((kind, key, token), out var source) && source.IsCancellationRequested;
}

internal sealed class LambdaHandler(Func<AskRequest, CancellationToken, Task<ReadOnlyMemory<byte>>> handle) : IAskHandler
{
    public Task<ReadOnlyMemory<byte>> HandleAsync(AskRequest request, CancellationToken cancellationToken) => handle(request, cancellationToken);
}

internal sealed class FakeContext(string kind, string key, long token) : OwnedContext
{
    public override string Kind => kind;

    public override string Key => key;

    public override long Token => token;

    public override IGeneration Configuration => throw new NotSupportedException();

    public override void EnsureOwner()
    {
    }

    public override Task EndAsync(CancellationToken cancellationToken = default) => throw new NotSupportedException();
}
