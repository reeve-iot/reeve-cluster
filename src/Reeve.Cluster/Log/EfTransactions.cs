using System.Data.Common;

namespace Reeve.Cluster.Log;

internal sealed class EfTransactions
{
    private readonly object gate = new();
    private readonly Dictionary<DbTransaction, List<IClusterScopeWriter>> open = new(ReferenceEqualityComparer.Instance);

    public void Began(DbTransaction transaction)
    {
        ArgumentNullException.ThrowIfNull(transaction);
        lock (gate)
        {
            EndLocked(transaction);
            open[transaction] = [];
        }
    }

    public void Ended(DbTransaction transaction)
    {
        ArgumentNullException.ThrowIfNull(transaction);
        lock (gate)
        {
            EndLocked(transaction);
        }
    }

    public bool IsOpen(DbTransaction transaction)
    {
        ArgumentNullException.ThrowIfNull(transaction);
        lock (gate)
        {
            return open.ContainsKey(transaction);
        }
    }

    public void Holds(DbTransaction transaction, IClusterScopeWriter? scope)
    {
        ArgumentNullException.ThrowIfNull(transaction);
        if (scope is null)
        {
            return;
        }

        lock (gate)
        {
            if (open.TryGetValue(transaction, out var scopes) && !scopes.Contains(scope))
            {
                scopes.Add(scope);
                scope.TransactionOpened(transaction);
            }
        }
    }

    private void EndLocked(DbTransaction transaction)
    {
        if (open.Remove(transaction, out var scopes))
        {
            foreach (var scope in scopes)
            {
                scope.TransactionEnded(transaction);
            }
        }
    }
}
