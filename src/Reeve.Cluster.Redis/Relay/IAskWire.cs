namespace Reeve.Cluster.Redis.Relay;

internal readonly record struct WireRecord(byte[]? Value, TimeSpan? TimeToLive);

internal interface IAskWire
{
    long ConnectionEvents { get; }

    Task<WireRecord> ReadAsync(string record, CancellationToken cancellationToken);

    Task WriteAsync(string record, byte[] value, TimeSpan timeToLive, CancellationToken cancellationToken);

    Task<bool> ReplaceAsync(string record, byte[] expected, byte[] value, TimeSpan timeToLive, CancellationToken cancellationToken);

    void Delete(string record);

    Task PublishAsync(string channel, byte[] message, CancellationToken cancellationToken);

    Task SubscribeAsync(string channel, Action<byte[]> handler, CancellationToken cancellationToken);

    bool IsSubscribed(string channel);

    void Unsubscribe(string channel);
}
