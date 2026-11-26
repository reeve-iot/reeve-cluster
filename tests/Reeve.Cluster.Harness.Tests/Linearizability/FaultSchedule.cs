using Npgsql;
using Reeve.Cluster.Harness.Processes;
using Reeve.Cluster.Harness.Seeds;
using Reeve.Cluster.Harness.Stack;

namespace Harness.Linearizability;

internal enum Fault
{
    Kill,

    Term,

    Freeze,

    Cut,

    Failover,
}

internal sealed class FaultSchedule
{
    private static readonly TimeSpan ReadyLimit = TimeSpan.FromSeconds(90);
    private readonly ProcessCluster cluster;
    private readonly ClusterStack stack;
    private readonly SeededRandom random;
    private readonly IReadOnlyList<string> replicas;
    private readonly Action<string> log;
    private readonly List<(Fault Fault, string? Replica)> events = [];
    private readonly List<string> degraded = [];

    public FaultSchedule(ProcessCluster cluster, ClusterStack stack, IReadOnlyList<string> replicas, RunSeed seed, Action<string> log)
    {
        this.cluster = cluster;
        this.stack = stack;
        this.replicas = replicas;
        this.log = log;
        random = seed.Fork("linearizability/faults");
    }

    public IReadOnlyList<(Fault Fault, string? Replica)> Events => events;

    public IReadOnlyList<string> Degraded => degraded;

    public event Action? FailedOver;

    public int UnexpectedExits { get; private set; }

    public IReadOnlyList<Fault> Plan(int count)
    {
        var kinds = new List<Fault>();
        while (kinds.Count < count)
        {
            var block = new List<Fault> { Fault.Kill, Fault.Term, Fault.Freeze, Fault.Cut };
            for (var i = block.Count - 1; i > 0; i--)
            {
                var j = random.NextInt("shuffle", 0, i + 1);
                (block[i], block[j]) = (block[j], block[i]);
            }

            kinds.AddRange(block.Take(count - kinds.Count));
        }

        kinds.Insert(random.NextInt("failover-at", count / 3, Math.Max(count / 3 + 1, 2 * count / 3 + 1)), Fault.Failover);
        return kinds;
    }

    public async Task RunAsync(int count, CancellationToken stop)
    {
        foreach (var fault in Plan(count))
        {
            await Task.Delay(random.NextDuration("gap", TimeSpan.FromSeconds(2), TimeSpan.FromSeconds(6)), stop).ConfigureAwait(false);
            var replica = fault == Fault.Failover ? null : random.Pick("replica", replicas);
            log($"fault: {fault} {replica}");
            events.Add((fault, replica));
            await ApplyAsync(fault, replica).ConfigureAwait(false);
            await RecoverAsync().ConfigureAwait(false);
            log($"fault: {fault} {replica}: recovered");
            if (fault == Fault.Failover)
            {
                FailedOver?.Invoke();
            }
        }
    }

    private async Task ApplyAsync(Fault fault, string? replica)
    {
        switch (fault)
        {
            case Fault.Kill:
                cluster[replica!].Signal(ProcessSignal.Kill);
                await cluster[replica!].WaitForExitAsync(ReadyLimit).ConfigureAwait(false);
                await cluster.StartProcessAsync(replica!).ConfigureAwait(false);
                break;
            case Fault.Term:
                cluster[replica!].Signal(ProcessSignal.Term);
                await cluster[replica!].WaitForExitAsync(ReadyLimit).ConfigureAwait(false);
                await cluster.StartProcessAsync(replica!).ConfigureAwait(false);
                break;
            case Fault.Freeze:
                cluster[replica!].Signal(ProcessSignal.Stop);
                await Task.Delay(FaultLength("freeze", TimeSpan.FromSeconds(8))).ConfigureAwait(false);
                cluster[replica!].Signal(ProcessSignal.Cont);
                break;
            case Fault.Cut:
                await cluster[replica!].Link.CutAsync().ConfigureAwait(false);
                await Task.Delay(FaultLength("cut", TimeSpan.FromSeconds(6))).ConfigureAwait(false);
                await cluster[replica!].Link.RestoreAsync().ConfigureAwait(false);
                break;
            default:
                var options = cluster[replicas[0]].Options;
                var repointed = await stack.FailOverAsync().ConfigureAwait(false);
                var tolerance = options.LeaseTtl - options.SafetyMargin - options.StopBudget;
                log($"fault: the repoint and restore took {repointed.TotalSeconds:F1} s (a replica rides out {tolerance.TotalSeconds:F1} s without the database)");
                if (repointed > tolerance)
                {
                    var note = $"failover degraded: the repoint took {repointed.TotalSeconds:F1} s, over the {tolerance.TotalSeconds:F1} s a replica rides out, so the replicas fenced by lease before the new primary was reachable and the timeline path was not taken";
                    degraded.Add(note);
                    log($"fault: {note}");
                }

                await AwaitRenewalsOnNewPrimaryAsync(options.LeaseTtl).ConfigureAwait(false);
                break;
        }
    }

    private async Task AwaitRenewalsOnNewPrimaryAsync(TimeSpan leaseTtl)
    {
        var deadline = DateTime.UtcNow + ReadyLimit;
        DateTimeOffset floor;
        while (true)
        {
            try
            {
                var (_, now) = await cluster.Run.Observer.MembersAsync().ConfigureAwait(false);
                if (now is { } databaseNow)
                {
                    floor = databaseNow + leaseTtl;
                    break;
                }
            }
            catch (NpgsqlException) when (DateTime.UtcNow < deadline)
            {
            }

            if (DateTime.UtcNow > deadline)
            {
                return;
            }

            await Task.Delay(100).ConfigureAwait(false);
        }

        foreach (var id in replicas)
        {
            while (!cluster[id].HasExited && DateTime.UtcNow < deadline)
            {
                try
                {
                    if (await cluster.Run.Observer.MemberAsync(id).ConfigureAwait(false) is { } member && member.LeaseExpiresAt > floor)
                    {
                        break;
                    }
                }
                catch (NpgsqlException)
                {
                }

                await Task.Delay(100).ConfigureAwait(false);
            }
        }
    }

    private TimeSpan FaultLength(string name, TimeSpan pastFenceStart) =>
        random.NextInt($"{name}-crosses", 0, 10) < 7
            ? random.NextDuration(name, FenceStart, FenceStart + pastFenceStart)
            : random.NextDuration($"{name}-rides-out", TimeSpan.FromSeconds(3), FenceStart);

    private TimeSpan FenceStart
    {
        get
        {
            var options = cluster[replicas[0]].Options;
            return options.LeaseTtl - options.SafetyMargin - options.StopBudget;
        }
    }

    private async Task RecoverAsync()
    {
        foreach (var id in replicas)
        {
            try
            {
                if (!cluster[id].HasExited)
                {
                    await cluster[id].WaitReadyAsync(ReadyLimit).ConfigureAwait(false);
                    continue;
                }
            }
            catch (InvalidOperationException) when (cluster[id].HasExited)
            {
            }

            UnexpectedExits++;
            log($"fault: {id} exited by itself with code {cluster[id].ExitCode}; starting it again");
            await cluster.StartProcessAsync(id).ConfigureAwait(false);
        }
    }
}
