using System.Globalization;
using System.Threading.Channels;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Reeve.Cluster.Wakeup;
using StackExchange.Redis;

namespace Reeve.Cluster.Redis;

using Wakeup = Reeve.Cluster.Wakeup.Wakeup;

internal sealed class RedisWakeupChannel : IWakeupChannel, IDisposable
{
    private readonly InProcessWakeupChannel local;
    private readonly ISubscriber subscriber;
    private readonly RedisChannel channel;
    private readonly ILogger logger;
    private readonly string origin = Guid.NewGuid().ToString("N");
    private readonly Channel<string> outbox = System.Threading.Channels.Channel.CreateBounded<string>(
        new BoundedChannelOptions(1024) { FullMode = BoundedChannelFullMode.DropOldest, SingleReader = true });
    private readonly Task sender;

    public RedisWakeupChannel(IConnectionMultiplexer redis, IOptions<ClusterRedisOptions> options, ILogger<RedisWakeupChannel> logger)
    {
        ArgumentNullException.ThrowIfNull(redis);
        ArgumentNullException.ThrowIfNull(options);
        ArgumentNullException.ThrowIfNull(logger);

        this.logger = logger;
        local = new InProcessWakeupChannel(Microsoft.Extensions.Logging.Abstractions.NullLogger<InProcessWakeupChannel>.Instance);
        subscriber = redis.GetSubscriber();
        channel = RedisChannel.Literal(ChannelName(options.Value.KeyPrefix));

        sender = Task.Run(SendLoopAsync);
    }

    public ValueTask PublishAsync(Wakeup wakeup, CancellationToken cancellationToken = default)
    {
        var task = local.PublishAsync(wakeup, cancellationToken);
        outbox.Writer.TryWrite(Encode(wakeup, origin));

        return task;
    }

    public IDisposable Subscribe(WakeupKind kind, Action<Wakeup> handler) => local.Subscribe(kind, handler);

    public void Dispose()
    {
        outbox.Writer.TryComplete();
        try
        {
            subscriber.Unsubscribe(channel, OnMessage, CommandFlags.FireAndForget);
        }
        catch (Exception exception) when (exception is not OutOfMemoryException)
        {
            logger.LogDebug(exception, "Unsubscribing from the Redis wake-up channel failed.");
        }
    }

    internal static string ChannelName(string prefix) => prefix + "wakeup";

    internal static string Encode(Wakeup wakeup, string origin) =>
        string.Create(CultureInfo.InvariantCulture, $"{wakeup.Kind}|{wakeup.Seq}|{origin}|{wakeup.Target}");

    internal static bool TryDecode(string message, out Wakeup wakeup, out string origin)
    {
        wakeup = default;
        origin = string.Empty;

        var parts = message.Split('|', 4);
        if (parts.Length != 4
            || !Enum.TryParse(parts[0], out WakeupKind kind)
            || !Enum.IsDefined(kind)
            || !long.TryParse(parts[1], NumberStyles.None, CultureInfo.InvariantCulture, out var seq))
        {
            return false;
        }

        origin = parts[2];
        wakeup = new Wakeup(kind, seq, parts[3].Length == 0 ? null : parts[3]);
        return true;
    }

    private async Task SendLoopAsync()
    {
        try
        {
            await subscriber.SubscribeAsync(channel, OnMessage).ConfigureAwait(false);
        }
        catch (Exception exception) when (exception is not OutOfMemoryException)
        {
            logger.LogWarning(exception, "Subscribing to the Redis wake-up channel failed; wake-ups from other replicas are lost, and the polls carry convergence.");
        }

        await foreach (var message in outbox.Reader.ReadAllAsync().ConfigureAwait(false))
        {
            try
            {
                await subscriber.PublishAsync(channel, message, CommandFlags.FireAndForget).ConfigureAwait(false);
            }
            catch (Exception exception) when (exception is not OutOfMemoryException)
            {
                logger.LogDebug(exception, "Publishing a wake-up to Redis failed; the polls carry it.");
            }
        }
    }

    private void OnMessage(RedisChannel _, RedisValue value)
    {
        if (!TryDecode(value.ToString(), out var wakeup, out var from))
        {
            logger.LogDebug("Ignored a malformed Redis wake-up message.");
            return;
        }

        if (from == origin)
        {
            return;
        }

        local.PublishAsync(wakeup).AsTask().GetAwaiter().GetResult();
    }
}
