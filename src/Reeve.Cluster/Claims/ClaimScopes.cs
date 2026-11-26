namespace Reeve.Cluster.Claims;

internal sealed class ClaimScopes
{
    private readonly Dictionary<string, int> open = new(StringComparer.Ordinal);

    public IDisposable Open(string key)
    {
        ArgumentNullException.ThrowIfNull(key);
        lock (open)
        {
            open[key] = open.GetValueOrDefault(key) + 1;
        }

        return new Scope(this, key);
    }

    public bool IsOpen(string key)
    {
        lock (open)
        {
            return open.ContainsKey(key);
        }
    }

    private void Close(string key)
    {
        lock (open)
        {
            if (open[key] == 1)
            {
                open.Remove(key);
            }
            else
            {
                open[key]--;
            }
        }
    }

    private sealed class Scope(ClaimScopes scopes, string key) : IDisposable
    {
        private int disposed;

        public void Dispose()
        {
            if (Interlocked.Exchange(ref disposed, 1) == 0)
            {
                scopes.Close(key);
            }
        }
    }
}
