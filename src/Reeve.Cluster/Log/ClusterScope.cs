using System.Data.Common;

namespace Reeve.Cluster.Log;

internal sealed class ClusterScope : IClusterScopeWriter
{
    private static long lastId;

    private readonly object gate = new();
    private readonly List<long> seqs = [];
    private readonly List<DbTransaction> openTransactions = [];
    private ClusterScopeKind kind;
    private bool commitRefused;
    private bool acknowledgedAtCommit;
    private IGeneration? pinnedGeneration;
    private volatile bool ended;

    public ClusterScope(ClusterScopeKind kind)
        : this(kind, null, 0)
    {
    }

    public ClusterScope(ClusterScopeKind kind, ClusterScope? parent, long fenceStepsAtBegin)
    {
        if (!Enum.IsDefined(kind))
        {
            throw new ArgumentOutOfRangeException(nameof(kind), kind, "Not a scope kind.");
        }

        this.kind = kind;
        Parent = parent;
        FenceStepsAtBegin = fenceStepsAtBegin;
        Id = "scope-" + Interlocked.Increment(ref lastId).ToString(System.Globalization.CultureInfo.InvariantCulture);
    }

    public string Id { get; }

    public ClusterScope? Parent { get; }

    public bool Ended => ended;

    public long FenceStepsAtBegin { get; }

    public ClusterScopeKind Kind
    {
        get
        {
            lock (gate)
            {
                return kind;
            }
        }
    }

    public IReadOnlyList<long> Seqs
    {
        get
        {
            lock (gate)
            {
                return [.. seqs];
            }
        }
    }

    public bool CommitRefused
    {
        get
        {
            lock (gate)
            {
                return commitRefused;
            }
        }
    }

    public bool AcknowledgedAtCommit
    {
        get
        {
            lock (gate)
            {
                return acknowledgedAtCommit;
            }
        }
    }

    public IGeneration? PinnedGeneration
    {
        get
        {
            lock (gate)
            {
                return pinnedGeneration;
            }
        }
    }

    public IReadOnlyCollection<DbTransaction> OpenTransactions
    {
        get
        {
            lock (gate)
            {
                return [.. openTransactions];
            }
        }
    }

    public void RecordSeq(long seq)
    {
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(seq);
        lock (gate)
        {
            if (kind != ClusterScopeKind.ExemptRequest)
            {
                seqs.Add(seq);
            }
        }
    }

    public void RecordRefusedCommit()
    {
        lock (gate)
        {
            commitRefused = true;
        }
    }

    public void OptOut()
    {
        lock (gate)
        {
            acknowledgedAtCommit = true;
        }
    }

    public void Pin(IGeneration generation)
    {
        ArgumentNullException.ThrowIfNull(generation);
        lock (gate)
        {
            if (pinnedGeneration is not null)
            {
                throw new InvalidOperationException($"Scope {Id} already pinned a generation (head {pinnedGeneration.Head}).");
            }

            if (ended)
            {
                return;
            }

            pinnedGeneration = generation;
            (generation as Generation)?.AddPin();
        }
    }

    public IGeneration PinOrGet(IGeneration generation)
    {
        ArgumentNullException.ThrowIfNull(generation);
        lock (gate)
        {
            if (ended)
            {
                return generation;
            }

            if (pinnedGeneration is null)
            {
                pinnedGeneration = generation;
                (generation as Generation)?.AddPin();
            }

            return pinnedGeneration;
        }
    }

    public void TransactionOpened(DbTransaction transaction)
    {
        ArgumentNullException.ThrowIfNull(transaction);
        lock (gate)
        {
            if (!openTransactions.Contains(transaction, ReferenceEqualityComparer.Instance))
            {
                openTransactions.Add(transaction);
            }
        }
    }

    public void TransactionEnded(DbTransaction transaction)
    {
        ArgumentNullException.ThrowIfNull(transaction);
        lock (gate)
        {
            openTransactions.RemoveAll(t => ReferenceEquals(t, transaction));
        }
    }

    public void ClassifyAsExempt()
    {
        lock (gate)
        {
            if (kind != ClusterScopeKind.ClientRequest)
            {
                throw new InvalidOperationException($"Only a client request's scope is classified as exempt; scope {Id} is {kind}.");
            }

            if (seqs.Count > 0 || commitRefused)
            {
                throw new InvalidOperationException(
                    $"Scope {Id} has recorded a seq or a refused commit as a client request; it cannot become exempt after that.");
            }

            kind = ClusterScopeKind.ExemptRequest;
        }
    }

    public void End()
    {
        lock (gate)
        {
            ended = true;
            (pinnedGeneration as Generation)?.RemovePin();
            pinnedGeneration = null;
        }
    }

    public static ClusterScope? Live(ClusterScope? scope)
    {
        while (scope is { Ended: true })
        {
            scope = scope.Parent;
        }

        return scope;
    }

    public override string ToString() => $"{Id} ({Kind})";
}

internal sealed class ClusterScopeSource : IClusterScopeSource
{
    private readonly AsyncLocal<ClusterScope?> ambient = new();
    private readonly FenceSteps fenceSteps;

    public ClusterScopeSource(FenceSteps fenceSteps)
    {
        ArgumentNullException.ThrowIfNull(fenceSteps);
        this.fenceSteps = fenceSteps;
    }

    public IClusterScope? Current => ClusterScope.Live(ambient.Value);

    public IClusterScopeWriter? CurrentWriter => ClusterScope.Live(ambient.Value);

    public IClusterScopeHandle Begin(ClusterScopeKind kind)
    {
        var scope = new ClusterScope(kind, ClusterScope.Live(ambient.Value), fenceSteps.Count);
        ambient.Value = scope;
        return new Handle(this, scope);
    }

    public IClusterScopeHandle BeginUnitOfWork() => BeginForApplication(ClusterScopeKind.UnitOfWork);

    public IClusterScopeHandle BeginBackground() => BeginForApplication(ClusterScopeKind.Background);

    private IClusterScopeHandle BeginForApplication(ClusterScopeKind kind)
    {
        if (ClusterScope.Live(ambient.Value) is { Kind: ClusterScopeKind.ClientRequest or ClusterScopeKind.ExemptRequest or ClusterScopeKind.Applier } outer)
        {
            return new Joined(outer);
        }

        return Begin(kind);
    }

    private sealed class Handle(ClusterScopeSource source, ClusterScope scope) : IClusterScopeHandle
    {
        private int disposed;

        public IClusterScope Scope => scope;

        public void Dispose()
        {
            if (Interlocked.Exchange(ref disposed, 1) != 0)
            {
                return;
            }

            scope.End();

            if (ReferenceEquals(source.ambient.Value, scope))
            {
                source.ambient.Value = ClusterScope.Live(scope.Parent);
            }
        }
    }

    private sealed class Joined(ClusterScope scope) : IClusterScopeHandle
    {
        public IClusterScope Scope => scope;

        public void Dispose()
        {
        }
    }
}
