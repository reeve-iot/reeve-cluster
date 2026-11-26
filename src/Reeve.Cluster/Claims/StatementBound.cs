using Microsoft.Extensions.Options;

namespace Reeve.Cluster.Claims;

internal static class StatementBound
{
    public static async Task<(T? Value, bool Cancelled)> RunAsync<T>(
        Func<CancellationToken, Task<T>> statement, IOptions<ClusterOptions> options, TimeProvider time, CancellationToken cancellationToken)
    {
        using var timer = new CancellationTokenSource(options.Value.FencedTransactionTimeout + TimeSpan.FromSeconds(2), time);
        using var bound = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken, timer.Token);
        try
        {
            var value = await statement(bound.Token).ConfigureAwait(false);
            cancellationToken.ThrowIfCancellationRequested();
            return (value, timer.IsCancellationRequested);
        }
        catch (OperationCanceledException) when (!cancellationToken.IsCancellationRequested && timer.IsCancellationRequested)
        {
            return (default, true);
        }
    }
}
