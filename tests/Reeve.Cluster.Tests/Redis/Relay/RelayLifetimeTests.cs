using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using Microsoft.Extensions.Time.Testing;
using Reeve.Cluster;
using Reeve.Cluster.Ask;
using Reeve.Cluster.Core;
using Reeve.Cluster.Membership;
using Reeve.Cluster.Redis.Relay;
using Unit.Ask;

namespace Unit.Redis.Relay;

public sealed class RelayLifetimeTests
{
    private static readonly AsyncLocal<string?> Marker = new();

    private static byte[] Bytes(string text) => System.Text.Encoding.UTF8.GetBytes(text);

    [Fact]
    public async Task With_Redis_down_at_start_the_relay_starts_and_takes_requests_once_Redis_returns()
    {
        var inbound = ReservingInbound.Echoing();
        var wire = new FakeWire { Down = true };
        await using var rig = new RelayRig(inbound, wire);

        rig.Target.Start();
        rig.Caller.Start();
        await Task.Delay(150);
        Assert.False(rig.Target.Subscribed.IsCompleted);
        var whileDown = await rig.AskTargetAsync(timeout: TimeSpan.FromMilliseconds(300));

        wire.Down = false;
        await rig.Target.Subscribed.WaitAsync(TimeSpan.FromSeconds(10));
        await rig.Caller.Subscribed.WaitAsync(TimeSpan.FromSeconds(10));
        var afterwards = await rig.AskTargetAsync(Bytes("back"));

        Assert.Equal(DeliveryKind.NotTaken, whileDown.Kind);
        Assert.Equal(DeliveryKind.Answered, afterwards.Kind);
    }

    [Fact]
    public async Task A_subscription_that_fails_once_is_made_again_until_it_holds()
    {
        var wire = new FakeWire();
        wire.Fail("Subscribe", FakeWire.Lost(), times: 3);
        await using var rig = new RelayRig(ReservingInbound.Echoing(), wire);

        await rig.StartAsync();

        Assert.Equal(DeliveryKind.Answered, (await rig.AskTargetAsync()).Kind);
    }

    [Fact]
    public async Task Starting_twice_subscribes_once()
    {
        var wire = new FakeWire();
        await using var rig = new RelayRig(ReservingInbound.Echoing(), wire);

        rig.Target.Start();
        rig.Target.Start();
        await rig.Target.Subscribed.WaitAsync(TimeSpan.FromSeconds(10));

        Assert.Equal(2, wire.Commands.Count(c => c == "Subscribe"));
    }

    [Fact]
    public async Task The_receive_loop_starts_handlers_from_no_callers_flow()
    {
        string? atReserve = "unset";
        string? inHandler = "unset";
        var inbound = new ReservingInbound((delivery, _) =>
        {
            atReserve = Marker.Value;
            return ValueTask.FromResult(AskReservation.Reserve(() =>
            {
                inHandler = Marker.Value;
                return Task.FromResult(new AskAnswer(AskAnswerKind.Response, default, null));
            }));
        });

        Marker.Value = "host-start";
        await using var rig = new RelayRig(inbound);
        await rig.StartAsync();
        Marker.Value = "request-scope";

        var outcome = await rig.AskTargetAsync();
        Marker.Value = null;

        Assert.Equal(DeliveryKind.Answered, outcome.Kind);
        Assert.Null(atReserve);
        Assert.Null(inHandler);
    }

    [Fact]
    public void The_relay_is_not_a_fence_participant()
    {
        Assert.False(typeof(IFenceParticipant).IsAssignableFrom(typeof(AskRelay)));
        Assert.False(typeof(IFenceParticipant).IsAssignableFrom(typeof(AskRelayStartup)));
    }

    [Fact]
    public async Task A_fenced_replica_answers_a_request_to_it_and_answers_again_after_its_rejoin_with_the_same_process_id()
    {
        var handler = new LambdaHandler((request, _) => Task.FromResult<ReadOnlyMemory<byte>>(request.Payload.ToArray()));
        using var targetRig = new AskRig(TimeProvider.System, member: handler, keyed: handler);
        await using var rig = new RelayRig(targetRig.Inbound);
        await rig.StartAsync();
        var processId = rig.TargetIdentity.ProcessId;

        targetRig.Gate.RequestFence(new FenceRequest(FenceTrigger.FenceStart));
        Assert.Equal(ReplicaPhase.Fenced, targetRig.Gate.CompleteFence());
        var whileFenced = await rig.AskTargetAsync(Bytes("fenced"));

        targetRig.Gate.Apply(MemberTrigger.RejoinPermitted);
        Assert.True(targetRig.Gate.Joined(2, 1, targetRig.Gate.Now));
        targetRig.Gate.Apply(MemberTrigger.StateLoaded);
        targetRig.Gate.Apply(MemberTrigger.ActivationCommitted);
        targetRig.Gate.Apply(MemberTrigger.AppliedToActivationHead);
        Assert.Equal(ReplicaPhase.Serving, targetRig.Gate.Phase);
        var afterRejoin = await rig.AskTargetAsync(Bytes("rejoined"));

        Assert.Equal(DeliveryKind.Answered, whileFenced.Kind);
        Assert.Equal("fenced", System.Text.Encoding.UTF8.GetString(whileFenced.Answer!.Response.Span));
        Assert.Equal(DeliveryKind.Answered, afterRejoin.Kind);
        Assert.Equal("rejoined", System.Text.Encoding.UTF8.GetString(afterRejoin.Answer!.Response.Span));
        Assert.Equal(processId, rig.TargetIdentity.ProcessId);

        Assert.Equal(4, rig.Wire.Commands.Count(c => c == "Subscribe"));
    }

    [Fact]
    public async Task A_fenced_replica_declines_a_keyed_request_and_the_decline_reaches_the_caller()
    {
        var handler = new LambdaHandler((request, _) => Task.FromResult<ReadOnlyMemory<byte>>(request.Payload.ToArray()));
        using var targetRig = new AskRig(TimeProvider.System, member: handler, keyed: handler);
        targetRig.Tenures.Add(AskRig.Kind, AskRig.Key, 7);
        await using var rig = new RelayRig(targetRig.Inbound);
        await rig.StartAsync();
        targetRig.Gate.RequestFence(new FenceRequest(FenceTrigger.FenceStart));
        Assert.Equal(ReplicaPhase.Fenced, targetRig.Gate.CompleteFence());

        var outcome = await rig.Caller.DeliverAsync(rig.TargetAddress, new AskDelivery(AskRig.Kind, AskRig.Key, 7, Bytes("k")), RelayRig.Patience, CancellationToken.None);

        Assert.Equal(DeliveryKind.Declined, outcome.Kind);
        Assert.Equal(DeclineReason.NotServing, outcome.Reason);
    }

    [Fact]
    public async Task A_replaced_process_ends_NotTaken_and_the_new_one_answers()
    {
        var clock = new FakeTimeProvider();
        var oldInbound = ReservingInbound.Echoing();
        await using var rig = new RelayRig(oldInbound, time: clock);
        await rig.StartAsync();
        var replacement = new MemberIdentity(Guid.NewGuid(), "1.0.0", MemberIdentity.CurrentProtocol, []);
        var newInbound = ReservingInbound.Echoing();
        await using var replaced = new AskRelay(
            rig.Wire, newInbound, replacement, Options.Create(new ClusterOptions { ReplicaId = "b" }), RelayRig.Timings, clock, NullLogger<AskRelay>.Instance);
        replaced.Start();
        await replaced.Subscribed.WaitAsync(TimeSpan.FromSeconds(10));

        await rig.Target.DisposeAsync();
        var toOld = await rig.AskUntilTheWindowEndsAsync(clock, timeout: TimeSpan.FromMilliseconds(400));
        var toNew = await rig.Caller.DeliverAsync(new AskTarget("b", replacement.ProcessId), new AskDelivery(null, null, null, Bytes("n")), RelayRig.Patience, CancellationToken.None);

        Assert.Equal(DeliveryKind.NotTaken, toOld.Kind);
        Assert.Equal(DeliveryKind.Answered, toNew.Kind);
        Assert.Equal(0, oldInbound.Reserved);
        Assert.Equal(1, newInbound.Started);
    }
}
