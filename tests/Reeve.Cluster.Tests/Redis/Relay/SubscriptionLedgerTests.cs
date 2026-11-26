using Reeve.Cluster.Redis.Relay;

namespace Unit.Redis.Relay;

public sealed class SubscriptionLedgerTests
{
    [Fact]
    public void A_channel_that_never_subscribed_is_not_held_whatever_the_connection()
    {
        var ledger = new SubscriptionLedger();

        Assert.False(ledger.IsSubscribed("ask:q:a"));
        ledger.ConnectionRestored("redis:6379");
        Assert.False(ledger.IsSubscribed("ask:q:a"));
    }

    [Fact]
    public void A_channel_is_held_from_its_successful_subscribe_until_it_is_removed()
    {
        var ledger = new SubscriptionLedger();

        ledger.Subscribed("ask:q:a");
        Assert.True(ledger.IsSubscribed("ask:q:a"));
        Assert.False(ledger.IsSubscribed("ask:r:a"));

        ledger.Unsubscribed("ask:q:a");
        Assert.False(ledger.IsSubscribed("ask:q:a"));
    }

    [Fact]
    public void A_failed_subscription_connection_loses_every_channel_until_it_is_restored()
    {
        var ledger = new SubscriptionLedger();
        ledger.Subscribed("ask:q:a");
        ledger.Subscribed("ask:r:a");

        ledger.ConnectionFailed("redis:6379");
        Assert.False(ledger.IsSubscribed("ask:q:a"));
        Assert.False(ledger.IsSubscribed("ask:r:a"));

        ledger.ConnectionRestored("redis:6379");
        Assert.True(ledger.IsSubscribed("ask:q:a"));
        Assert.True(ledger.IsSubscribed("ask:r:a"));
    }

    [Fact]
    public void Each_endpoint_that_failed_must_be_restored()
    {
        var ledger = new SubscriptionLedger();
        ledger.Subscribed("ask:q:a");

        ledger.ConnectionFailed("redis-1:6379");
        ledger.ConnectionFailed("redis-2:6379");
        ledger.ConnectionRestored("redis-1:6379");
        Assert.False(ledger.IsSubscribed("ask:q:a"));

        ledger.ConnectionRestored("redis-2:6379");
        Assert.True(ledger.IsSubscribed("ask:q:a"));
    }
}
