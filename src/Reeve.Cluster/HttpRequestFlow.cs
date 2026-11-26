namespace Reeve.Cluster;

internal static class HttpRequestFlow
{
    private static readonly AsyncLocal<Flag?> Current = new();

    public static IDisposable Enter()
    {
        var previous = Current.Value;
        var current = new Flag();
        Current.Value = current;
        return new Entry(current, previous);
    }

    public static bool IsInside => Current.Value?.Active == true;

    private sealed class Flag
    {
        public volatile bool Active = true;
    }

    private sealed class Entry(Flag current, Flag? previous) : IDisposable
    {
        private int disposed;

        public void Dispose()
        {
            if (Interlocked.Exchange(ref disposed, 1) != 0)
            {
                return;
            }
            current.Active = false;
            Current.Value = previous;
        }
    }
}
