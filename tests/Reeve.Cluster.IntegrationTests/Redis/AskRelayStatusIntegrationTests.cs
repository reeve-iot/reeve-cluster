using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using Reeve.Cluster;
using Reeve.Cluster.Ask;
using Reeve.Cluster.Core;
using Reeve.Cluster.Harness.Containers;
using Reeve.Cluster.Membership;
using Reeve.Cluster.Redis;
using Reeve.Cluster.Redis.Relay;
using StackExchange.Redis;

namespace Int.Redis;

public sealed class AskRelayStatusIntegrationTests : IClassFixture<RedisFixture>
{
    private static readonly TimeSpan Answer = TimeSpan.FromSeconds(30);

    private readonly RedisFixture redis;

    public AskRelayStatusIntegrationTests(RedisFixture redis)
    {
        this.redis = redis;
    }

    private static IOptions<ClusterRedisOptions> Options(string prefix) => Microsoft.Extensions.Options.Options.Create(new ClusterRedisOptions { KeyPrefix = prefix });

    private async Task<ConnectionMultiplexer> AsUserAsync(string user, string password)
    {
        var configuration = ConfigurationOptions.Parse(redis.ConnectionString);
        configuration.User = user;
        configuration.Password = password;
        configuration.AbortOnConnectFail = false;
        return await ConnectionMultiplexer.ConnectAsync(configuration);
    }

    private async Task SetUserAsync(string user, string password, string channels)
    {
        await using var admin = await redis.ConnectAsync();
        await admin.GetDatabase().ExecuteAsync("ACL", "SETUSER", user, "on", ">" + password, "~*", "+@all", channels);
    }

    private static AskRelay NewRelay(IAskWire wire, string replicaId) => new(
        wire,
        new NeverInbound(),
        new MemberIdentity(Guid.NewGuid(), "1.0.0", MemberIdentity.CurrentProtocol, []),
        Microsoft.Extensions.Options.Options.Create(new ClusterOptions { ReplicaId = replicaId }),
        AskRelayTimings.Default with { SubscribeRetry = TimeSpan.FromMilliseconds(100) },
        TimeProvider.System,
        NullLogger<AskRelay>.Instance);

    [Fact]
    public async Task A_channel_that_never_subscribed_is_not_held_while_Redis_answers_and_a_subscribed_one_is()
    {
        await using var connection = await redis.ConnectAsync();
        var wire = new RedisAskWire(connection, Options("st1:"));

        Assert.False(wire.IsSubscribed("ask:never"));

        await wire.SubscribeAsync("ask:held", _ => { }, CancellationToken.None);
        Assert.True(wire.IsSubscribed("ask:held"));
        Assert.False(wire.IsSubscribed("ask:never"));

        wire.Unsubscribe("ask:held");
        Assert.False(wire.IsSubscribed("ask:held"));
    }

    [Fact]
    public async Task A_relay_connected_as_a_user_without_channel_permission_stays_not_subscribed_while_Redis_answers()
    {
        await SetUserAsync("relay-denied", "pw", "resetchannels");
        await using var connection = await AsUserAsync("relay-denied", "pw");
        var wire = new RedisAskWire(connection, Options("st2:"));
        await using var relay = NewRelay(wire, "denied");

        Assert.True(connection.GetSubscriber().IsConnected(RedisChannel.Literal("st2:anything")));

        relay.Start();
        await Task.Delay(TimeSpan.FromSeconds(1));

        Assert.False(relay.Subscribed.IsCompleted);
        Assert.Equal(AskSubscriptionState.NotSubscribed, relay.SubscriptionState);
        Assert.False(relay.Reachable);
        var section = await new AskRelayStatusSection(relay).CollectAsync(CancellationToken.None);
        Assert.Equal(("not-subscribed", false), ((string)section.Values["subscriptions"]!, (bool)section.Values["reachable"]!));
    }

    [Fact]
    public async Task A_relay_connected_as_a_user_with_channel_permission_holds()
    {
        await SetUserAsync("relay-granted", "pw", "&*");
        await using var connection = await AsUserAsync("relay-granted", "pw");
        var wire = new RedisAskWire(connection, Options("st3:"));
        await using var relay = NewRelay(wire, "granted");

        relay.Start();
        await relay.Subscribed.WaitAsync(Answer);

        Assert.Equal(AskSubscriptionState.Holding, relay.SubscriptionState);
        Assert.True(relay.Reachable);
    }

    private sealed class NeverInbound : IAskInbound
    {
        public ValueTask<AskTaking> TakeAsync(AskDelivery delivery, CancellationToken answerBy) => throw new NotSupportedException();

        public ValueTask<AskReservation> ReserveAsync(AskDelivery delivery, CancellationToken answerBy) => throw new NotSupportedException();
    }
}
