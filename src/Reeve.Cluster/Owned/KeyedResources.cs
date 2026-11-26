using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Reeve.Cluster.Membership;

namespace Reeve.Cluster.Owned;

internal interface ICreatorRows
{
    Task<OwnedRead> ReadAsync(string kind, string key, CancellationToken cancellationToken);

    Task<long?> InsertAsync(string kind, string key, string owner, long epoch, CancellationToken cancellationToken);

    Task<long?> TakeAsync(string kind, string key, OwnedRow expected, string owner, long epoch, CancellationToken cancellationToken);

    Task<bool> ClearAsync(string kind, string key, OwnedRow expected, CancellationToken cancellationToken);
}

internal sealed class KeyedResources : IKeyedResources
{
    private readonly Dictionary<string, UnitKind> kinds;
    private readonly ICreatorRows rows;
    private readonly LocalPhaseGate gate;
    private readonly Action nudge;
    private readonly Func<string, string, bool> holds;
    private readonly Func<IReadOnlyList<string>> currentCapabilities;
    private readonly ClusterOptions options;
    private readonly TimeProvider time;
    private readonly ILogger logger;
    private readonly string replicaId;

    public KeyedResources(
        IEnumerable<UnitKind> registered,
        ICreatorRows rows,
        LocalPhaseGate gate,
        Action nudge,
        Func<string, string, bool> holds,
        Func<IReadOnlyList<string>> currentCapabilities,
        IOptions<ClusterOptions> options,
        TimeProvider time,
        ILogger<KeyedResources> logger)
    {
        ArgumentNullException.ThrowIfNull(registered);
        ArgumentNullException.ThrowIfNull(options);
        kinds = registered.Where(k => k.Keyed).ToDictionary(k => k.Kind, StringComparer.Ordinal);
        this.rows = rows;
        this.gate = gate;
        this.nudge = nudge;
        this.holds = holds;
        this.currentCapabilities = currentCapabilities;
        this.options = options.Value;
        this.time = time;
        this.logger = logger;
        replicaId = this.options.ReplicaId ?? throw new ArgumentException("ReplicaId is required.", nameof(options));
    }

    public async Task<CreateOutcome> CreateAsync(string kind, string key, CancellationToken cancellationToken = default)
    {
        var registered = Registered(kind, key);
        while (true)
        {
            var read = await ReadAsync(kind, key, cancellationToken).ConfigureAwait(false);
            var statement = OwnedStateMachine.Decide(OwnedWriter.Creator, read.Condition)?.Statement;
            if (statement == OwnedStatement.RefuseAlreadyOwned)
            {
                return CreateOutcome.AlreadyOwned;
            }

            if (!registered.Options.Eligibility.IsEligible(currentCapabilities()))
            {
                return CreateOutcome.NotEligible;
            }

            if (ServingEpoch() is not { } epoch)
            {
                return CreateOutcome.NotServing;
            }

            if (holds(kind, key))
            {
                return CreateOutcome.Stopping;
            }

            using var timer = Timer();
            using var bound = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken, timer.Token);
            var token = statement switch
            {
                OwnedStatement.Insert => await rows.InsertAsync(kind, key, replicaId, epoch, bound.Token).ConfigureAwait(false),
                OwnedStatement.Take => await rows.TakeAsync(kind, key, read.Row!.Value, replicaId, epoch, bound.Token).ConfigureAwait(false),
                _ => throw new InvalidOperationException($"The creator has no statement for a row that is {read.Condition}."),
            };
            if (token is not null)
            {
                logger.LogInformation("{Kind}/{Key} created under tenure {Token} ({Condition} before).", kind, key, token, read.Condition);
                nudge();
                return CreateOutcome.Created;
            }
        }
    }

    public async Task<bool> CleanUpAsync(string kind, string key, CancellationToken cancellationToken = default)
    {
        if (Registered(kind, key).Options.Policy != AdoptionPolicy.None)
        {
            throw new ArgumentException($"Kind {kind} does not have the None policy: its orphaned rows are adopted, not cleaned up.", nameof(kind));
        }

        while (true)
        {
            var read = await ReadAsync(kind, key, cancellationToken).ConfigureAwait(false);
            if (OwnedStateMachine.Decide(OwnedWriter.CreatorCleanUp, read.Condition) is null || ServingEpoch() is null)
            {
                return false;
            }

            using var timer = Timer();
            using var bound = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken, timer.Token);
            if (await rows.ClearAsync(kind, key, read.Row!.Value, bound.Token).ConfigureAwait(false))
            {
                logger.LogInformation("{Kind}/{Key} cleaned up: the orphaned row of {Owner} is free.", kind, key, read.Row.Value.OwnerReplica);
                return true;
            }
        }
    }

    private UnitKind Registered(string kind, string key)
    {
        ArgumentNullException.ThrowIfNull(key);
        return kinds.TryGetValue(kind, out var registered)
            ? registered
            : throw new ArgumentException($"Kind {kind} is not registered on this replica as a keyed resource: register it with AddClusterKeyedResource.", nameof(kind));
    }

    private async Task<OwnedRead> ReadAsync(string kind, string key, CancellationToken cancellationToken)
    {
        using var timer = Timer();
        using var bound = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken, timer.Token);
        return await rows.ReadAsync(kind, key, bound.Token).ConfigureAwait(false);
    }

    private long? ServingEpoch() => gate.CheckServing() is { IsServing: true, Epoch: { } epoch } ? epoch : null;

    private CancellationTokenSource Timer() => new(options.FencedTransactionTimeout + TimeSpan.FromSeconds(2), time);
}
