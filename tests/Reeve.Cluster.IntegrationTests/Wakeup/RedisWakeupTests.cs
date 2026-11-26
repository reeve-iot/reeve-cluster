using System.Diagnostics;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using Reeve.Cluster.Harness.Containers;
using Reeve.Cluster.Redis;
using Reeve.Cluster.Wakeup;
using Wakeup = Reeve.Cluster.Wakeup.Wakeup;
using StackExchange.Redis;

namespace Int.Redis;

public sealed class RedisWakeupTests : IClassFixture<RedisFixture>
{
    private static readonly TimeSpan Answer = TimeSpan.FromSeconds(30);

    private readonly RedisFixture redis;

    public RedisWakeupTests(RedisFixture redis)
    {
        this.redis = redis;
    }

    [Fact]
    public void A_wake_up_survives_the_wire_format_with_a_target_that_holds_the_separator()
    {
        var wakeup = Wakeup.Nudge(WakeupKind.OwnerNudge, "kind|key:7");

        Assert.True(RedisWakeupChannel.TryDecode(RedisWakeupChannel.Encode(wakeup, "o1"), out var decoded, out var origin));
        Assert.Equal(wakeup, decoded);
        Assert.Equal("o1", origin);
        Assert.True(RedisWakeupChannel.TryDecode(RedisWakeupChannel.Encode(new Wakeup(WakeupKind.Applied, 42), "o2"), out var applied, out _));
        Assert.Equal(new Wakeup(WakeupKind.Applied, 42), applied);
    }

    [Theory]
    [InlineData("")]
    [InlineData("Appended|1|o")]
    [InlineData("Nope|1|o|")]
    [InlineData("Appended|x|o|")]
    [InlineData("Appended|-1|o|")]
    [InlineData("99|1|o|")]
    public void A_malformed_message_is_not_a_wake_up(string message) => Assert.False(RedisWakeupChannel.TryDecode(message, out _, out _));

    [Fact]
    public async Task A_wake_up_reaches_the_other_replica_and_the_sender_hears_it_once()
    {
        await using var a = await ChannelAsync("t1:");
        await using var b = await ChannelAsync("t1:");
        var heardByA = new List<Wakeup>();
        var heardByB = new TaskCompletionSource<Wakeup>(TaskCreationOptions.RunContinuationsAsynchronously);
        var sentinel = new TaskCompletionSource<Wakeup>(TaskCreationOptions.RunContinuationsAsynchronously);
        using var _1 = a.Channel.Subscribe(WakeupKind.Appended, wakeup => { lock (heardByA) { heardByA.Add(wakeup); } });
        using var _2 = b.Channel.Subscribe(WakeupKind.Appended, wakeup => heardByB.TrySetResult(wakeup));
        using var _3 = a.Channel.Subscribe(WakeupKind.Applied, wakeup => sentinel.TrySetResult(wakeup));

        var published = await PublishUntilAsync(a.Channel, new Wakeup(WakeupKind.Appended, 7), heardByB.Task);

        Assert.Equal(new Wakeup(WakeupKind.Appended, 7), await heardByB.Task);

        await PublishUntilAsync(b.Channel, new Wakeup(WakeupKind.Applied, 1), sentinel.Task);
        lock (heardByA)
        {
            Assert.Equal(published, heardByA.Count);
        }
    }

    [Fact]
    public async Task Each_kind_goes_to_its_own_subscribers_and_a_nudge_keeps_its_target()
    {
        await using var a = await ChannelAsync("t2:");
        await using var b = await ChannelAsync("t2:");
        var nudge = new TaskCompletionSource<Wakeup>(TaskCreationOptions.RunContinuationsAsynchronously);
        var applied = new List<Wakeup>();
        using var _1 = b.Channel.Subscribe(WakeupKind.OwnerNudge, wakeup => nudge.TrySetResult(wakeup));
        using var _2 = b.Channel.Subscribe(WakeupKind.Applied, wakeup => { lock (applied) { applied.Add(wakeup); } });

        await PublishUntilAsync(a.Channel, Wakeup.Nudge(WakeupKind.OwnerNudge, "widget|k1"), nudge.Task);

        Assert.Equal(Wakeup.Nudge(WakeupKind.OwnerNudge, "widget|k1"), await nudge.Task);
        lock (applied)
        {
            Assert.Empty(applied);
        }
    }

    [Fact]
    public async Task A_cluster_with_another_prefix_does_not_hear_it()
    {
        await using var a = await ChannelAsync("t3a:");
        await using var b = await ChannelAsync("t3a:");
        await using var other = await ChannelAsync("t3b:");
        await using var otherSender = await ChannelAsync("t3b:");
        var heardByB = new TaskCompletionSource<Wakeup>(TaskCreationOptions.RunContinuationsAsynchronously);
        var heardByOtherToo = new TaskCompletionSource<Wakeup>(TaskCreationOptions.RunContinuationsAsynchronously);
        var strayed = new List<Wakeup>();
        using var _1 = b.Channel.Subscribe(WakeupKind.Appended, wakeup => heardByB.TrySetResult(wakeup));
        using var _2 = other.Channel.Subscribe(WakeupKind.Appended, wakeup =>
        {
            lock (strayed) { strayed.Add(wakeup); }
            heardByOtherToo.TrySetResult(wakeup);
        });

        await PublishUntilAsync(otherSender.Channel, new Wakeup(WakeupKind.Appended, 1), heardByOtherToo.Task);
        await PublishUntilAsync(a.Channel, new Wakeup(WakeupKind.Appended, 2), heardByB.Task);

        Assert.Equal(2, (await heardByB.Task).Seq);

        lock (strayed)
        {
            Assert.All(strayed, wakeup => Assert.Equal(1, wakeup.Seq));
        }
    }

    [Fact]
    public async Task I28_a_cut_Redis_costs_the_publisher_neither_time_nor_an_exception_and_local_subscribers_still_hear()
    {
        var options = new ConfigurationOptions { AbortOnConnectFail = false, ConnectTimeout = 5000, AsyncTimeout = 60_000 };
        options.EndPoints.Add("127.0.0.1:1");
        await using var multiplexer = await ConnectionMultiplexer.ConnectAsync(options);
        Assert.False(multiplexer.IsConnected);
        using var channel = new RedisWakeupChannel(multiplexer, Options.Create(new ClusterRedisOptions()), NullLogger<RedisWakeupChannel>.Instance);
        var heard = new List<Wakeup>();
        using var _ = channel.Subscribe(WakeupKind.Applied, heard.Add);

        var clock = Stopwatch.StartNew();
        for (var seq = 1; seq <= 50; seq++)
        {
            await channel.PublishAsync(new Wakeup(WakeupKind.Applied, seq));
        }

        Assert.True(clock.Elapsed < TimeSpan.FromSeconds(10), $"50 publishes to a cut Redis took {clock.Elapsed}");
        Assert.Equal(50, heard.Count);
    }

    [Fact]
    public async Task I28_a_stalled_Redis_costs_the_publisher_no_time_and_the_newest_wake_up_arrives_when_it_answers_again()
    {
        await using var a = await ChannelAsync("t4:");
        await using var b = await ChannelAsync("t4:");
        const int Count = 20_000;
        var last = new TaskCompletionSource<Wakeup>(TaskCreationOptions.RunContinuationsAsynchronously);
        using var _ = b.Channel.Subscribe(WakeupKind.Applied, wakeup =>
        {
            if (wakeup.Seq == Count)
            {
                last.TrySetResult(wakeup);
            }
        });
        var live = new TaskCompletionSource<Wakeup>(TaskCreationOptions.RunContinuationsAsynchronously);
        using var _2 = b.Channel.Subscribe(WakeupKind.Appended, wakeup => live.TrySetResult(wakeup));
        await PublishUntilAsync(a.Channel, new Wakeup(WakeupKind.Appended, 1), live.Task);

        await using var control = await ConnectionMultiplexer.ConnectAsync(redis.ConnectionString + ",allowAdmin=true");
        await control.GetDatabase().ExecuteAsync("CLIENT", "PAUSE", "20000", "ALL");

        var heard = 0;
        using var _3 = a.Channel.Subscribe(WakeupKind.Applied, _ => heard++);
        var clock = Stopwatch.StartNew();
        for (var seq = 1; seq <= Count; seq++)
        {
            await a.Channel.PublishAsync(new Wakeup(WakeupKind.Applied, seq));
        }

        Assert.True(clock.Elapsed < TimeSpan.FromSeconds(10), $"{Count} publishes to a stalled Redis took {clock.Elapsed}");
        Assert.False(last.Task.IsCompleted, "the newest wake-up arrived while Redis was stalled");
        Assert.Equal(Count, heard);
        Assert.Equal(Count, (await last.Task.WaitAsync(Answer)).Seq);
    }

    private async Task<Rig> ChannelAsync(string prefix)
    {
        var multiplexer = await redis.ConnectAsync();
        var channel = new RedisWakeupChannel(
            multiplexer, Options.Create(new ClusterRedisOptions { KeyPrefix = prefix }), NullLogger<RedisWakeupChannel>.Instance);
        return new Rig(multiplexer, channel);
    }

    private static async Task<int> PublishUntilAsync(IWakeupChannel sender, Wakeup wakeup, Task arrived)
    {
        var sent = 0;
        using var limit = new CancellationTokenSource(Answer);
        while (!arrived.IsCompleted)
        {
            limit.Token.ThrowIfCancellationRequested();
            await sender.PublishAsync(wakeup);
            sent++;
            await Task.WhenAny(arrived, Task.Delay(50, limit.Token));
        }

        return sent;
    }

    private sealed class Rig(ConnectionMultiplexer multiplexer, RedisWakeupChannel channel) : IAsyncDisposable
    {
        public RedisWakeupChannel Channel => channel;

        public async ValueTask DisposeAsync()
        {
            channel.Dispose();
            await multiplexer.DisposeAsync();
        }
    }
}
