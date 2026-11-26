using Microsoft.Extensions.Options;
using StackExchange.Redis;

namespace Reeve.Cluster.Redis.Relay;

internal sealed class RedisAskWire : IAskWire
{
    private readonly IConnectionMultiplexer redis;
    private readonly string prefix;
    private readonly SubscriptionLedger ledger = new();
    private long connectionEvents;

    public long ConnectionEvents => Interlocked.Read(ref connectionEvents);

    public RedisAskWire(IConnectionMultiplexer redis, IOptions<ClusterRedisOptions> options)
    {
        this.redis = redis;
        prefix = options.Value.KeyPrefix;
        redis.ConnectionFailed += (_, e) =>
        {
            Interlocked.Increment(ref connectionEvents);
            if (e.ConnectionType == ConnectionType.Subscription)
            {
                ledger.ConnectionFailed(EndpointName(e));
            }
        };
        redis.ConnectionRestored += (_, e) =>
        {
            Interlocked.Increment(ref connectionEvents);
            if (e.ConnectionType == ConnectionType.Subscription)
            {
                ledger.ConnectionRestored(EndpointName(e));
            }
        };
    }

    private static string EndpointName(ConnectionFailedEventArgs e) => e.EndPoint?.ToString() ?? string.Empty;

    public async Task<WireRecord> ReadAsync(string record, CancellationToken cancellationToken)
    {
        var read = await redis.GetDatabase().StringGetWithExpiryAsync(prefix + record, CommandFlags.DemandMaster).WaitAsync(cancellationToken).ConfigureAwait(false);
        return new WireRecord(read.Value.IsNull ? null : (byte[]?)read.Value, read.Expiry);
    }

    public Task WriteAsync(string record, byte[] value, TimeSpan timeToLive, CancellationToken cancellationToken) =>
        redis.GetDatabase().StringSetAsync(prefix + record, value, timeToLive, When.Always, CommandFlags.DemandMaster).WaitAsync(cancellationToken);

    public async Task<bool> ReplaceAsync(string record, byte[] expected, byte[] value, TimeSpan timeToLive, CancellationToken cancellationToken)
    {
        var key = prefix + record;
        var transaction = redis.GetDatabase().CreateTransaction();
        transaction.AddCondition(Condition.StringEqual(key, expected));
        _ = transaction.StringSetAsync(key, value, timeToLive, When.Always, CommandFlags.DemandMaster);
        return await transaction.ExecuteAsync(CommandFlags.DemandMaster).WaitAsync(cancellationToken).ConfigureAwait(false);
    }

    public void Delete(string record) => redis.GetDatabase().KeyDelete(prefix + record, CommandFlags.FireAndForget);

    public Task PublishAsync(string channel, byte[] message, CancellationToken cancellationToken) =>
        redis.GetSubscriber().PublishAsync(RedisChannel.Literal(prefix + channel), message).WaitAsync(cancellationToken);

    public async Task SubscribeAsync(string channel, Action<byte[]> handler, CancellationToken cancellationToken)
    {
        await redis.GetSubscriber().SubscribeAsync(RedisChannel.Literal(prefix + channel), (_, value) =>
        {
            if (!value.IsNull)
            {
                handler((byte[])value!);
            }
        }).WaitAsync(cancellationToken).ConfigureAwait(false);
        ledger.Subscribed(channel);
    }

    public bool IsSubscribed(string channel) => ledger.IsSubscribed(channel);

    public void Unsubscribe(string channel)
    {
        ledger.Unsubscribed(channel);
        try
        {
            redis.GetSubscriber().Unsubscribe(RedisChannel.Literal(prefix + channel), handler: null, CommandFlags.FireAndForget);
        }
        catch (Exception exception) when (exception is not OutOfMemoryException)
        {
        }
    }
}
