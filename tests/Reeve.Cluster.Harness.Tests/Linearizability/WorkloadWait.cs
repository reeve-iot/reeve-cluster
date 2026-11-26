namespace Harness.Linearizability;

internal static class WorkloadWait
{
    public static async Task<bool> UntilAsync(Func<bool> met, TimeSpan cap, CancellationToken cancellationToken = default)
    {
        var deadline = DateTime.UtcNow + cap;
        while (!met())
        {
            if (DateTime.UtcNow > deadline || cancellationToken.IsCancellationRequested)
            {
                return false;
            }

            await Task.Delay(50, CancellationToken.None).ConfigureAwait(false);
        }

        return true;
    }
}
