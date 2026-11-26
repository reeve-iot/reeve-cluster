namespace Reeve.Cluster.Slots;

internal static class SlotStores
{
    public const string PostgreSql = "postgresql";

    public const string Redis = "redis";
}

internal sealed class SlotMetricsObserver(string store) : ISlotObserver
{
    public void Acquired(SlotAcquireResult result) => ClusterDiagnostics.SlotAcquires.Add(
        1,
        new KeyValuePair<string, object?>("store", store),
        new KeyValuePair<string, object?>(
            "result",
            result switch
            {
                SlotAcquireResult.Acquired => "acquired",
                SlotAcquireResult.Full => "full",
                SlotAcquireResult.NotServing => "not_serving",
                _ => "unknown",
            }));

    public void Renewed(SlotRenewResult result) => ClusterDiagnostics.SlotRenewals.Add(
        1,
        new KeyValuePair<string, object?>("store", store),
        new KeyValuePair<string, object?>(
            "result",
            result switch
            {
                SlotRenewResult.Renewed => "renewed",
                SlotRenewResult.Ended => "ended",
                SlotRenewResult.NotServing => "not_serving",
                _ => "unknown",
            }));
}
