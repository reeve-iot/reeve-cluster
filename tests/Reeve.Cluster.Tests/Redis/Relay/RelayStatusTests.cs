using Reeve.Cluster.Ask;
using Reeve.Cluster.Redis.Relay;
using Unit.Ask;

namespace Unit.Redis.Relay;

public sealed class RelayStatusTests
{
    private static readonly TimeSpan Wait = TimeSpan.FromSeconds(10);

    private static async Task<(string State, bool Reachable)> ReadAsync(AskRelay relay)
    {
        var section = await new AskRelayStatusSection(relay).CollectAsync(CancellationToken.None);
        Assert.Equal(AskRelayStatusSection.Name, section.Name);
        return ((string)section.Values["subscriptions"]!, (bool)section.Values["reachable"]!);
    }

    [Fact]
    public async Task The_ask_section_says_not_subscribed_lost_and_holding()
    {
        var wire = new FakeWire { Down = true };
        await using var rig = new RelayRig(ReservingInbound.Echoing(), wire);

        Assert.Equal(("not-subscribed", false), await ReadAsync(rig.Target));
        rig.Target.Start();
        await Task.Delay(150);
        Assert.Equal(("not-subscribed", false), await ReadAsync(rig.Target));

        wire.Down = false;
        await rig.Target.Subscribed.WaitAsync(Wait);
        Assert.Equal(("holding", true), await ReadAsync(rig.Target));

        wire.LoseSubscriptions();
        Assert.Equal(("lost", false), await ReadAsync(rig.Target));

        wire.RestoreSubscriptions();
        Assert.Equal(("holding", true), await ReadAsync(rig.Target));
    }

    [Fact]
    public async Task A_relay_whose_subscribe_is_refused_while_Redis_answers_is_not_subscribed()
    {
        var wire = new FakeWire();
        wire.FailSubscribe("ask:q:", int.MaxValue);
        await using var rig = new RelayRig(ReservingInbound.Echoing(), wire);

        rig.Target.Start();
        await Task.Delay(300);

        Assert.False(rig.Target.Subscribed.IsCompleted);
        Assert.Equal(("not-subscribed", false), await ReadAsync(rig.Target));
    }

    [Fact]
    public async Task The_reply_channel_lost_leaves_the_process_reachable_and_the_request_channel_lost_does_not()
    {
        var wire = new FakeWire();
        await using var rig = new RelayRig(ReservingInbound.Echoing(), wire);
        await rig.StartAsync();
        var processId = rig.TargetIdentity.ProcessId;

        wire.LoseChannel(AskAddress.ReplyChannel(processId));
        Assert.Equal(("lost", true), await ReadAsync(rig.Target));
        Assert.Equal(("holding", true), await ReadAsync(rig.Caller));

        wire.LoseChannel(AskAddress.RequestChannel("b", processId));
        Assert.Equal(("lost", false), await ReadAsync(rig.Target));
    }
}
