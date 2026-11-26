using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.DependencyInjection;
using Npgsql;
using Reeve.Cluster.Harness.Replicas;
using Reeve.Cluster.Harness.Stack;
using Reeve.Cluster.Membership;

namespace Reeve.Cluster.Harness.Stimuli;

internal static partial class HarnessOperations
{
    public static IReadOnlySet<HarnessOperation> Implemented { get; } = new HashSet<HarnessOperation>
    {
        HarnessOperation.PauseReplica,
        HarnessOperation.ResumeReplica,
        HarnessOperation.CrashReplica,
        HarnessOperation.RestartReplica,
        HarnessOperation.StopReplica,
        HarnessOperation.StopReplicaEarly,
        HarnessOperation.OperatorDelete,
        HarnessOperation.RaiseProtocolFloor,
        HarnessOperation.ClockShift,
        HarnessOperation.ReconcilerThrow,
        HarnessOperation.ReconcilerHang,
        HarnessOperation.ReconcilerRelease,
        HarnessOperation.FixReconcilerFault,
        HarnessOperation.ReportFatalFault,
        HarnessOperation.ReportFatalFaultWhileFenced,
        HarnessOperation.StartWrite,
        HarnessOperation.SaveInForeignTransaction,
        HarnessOperation.RollbackWrite,
        HarnessOperation.RestoreDatabase,
    };

    public static Task PauseReplica(Replica replica) => replica.PauseAsync();

    public static Task ResumeReplica(Replica replica) => replica.ResumeAsync();

    public static Task CrashReplica(Replica replica) => replica.CrashAsync();

    public static Task StopReplica(Replica replica) => replica.StopAsync();

    public static Task StopReplicaEarly(Replica replica)
    {
        if (replica.IsRunning)
        {
            replica.RecordEnvironment(EnvironmentActions.StopEarly);
        }

        return replica.StopAsync();
    }

    public static Task<Replica> RestartReplica(HarnessRun run, string replicaId, ReplicaSpec? spec = null) =>
        run.StartReplicaAsync(replicaId, spec);

    public static async Task<int> OperatorDelete(HarnessRun run, string replicaId)
    {
        var deleted = await ExecuteAsync(run, "DELETE FROM reeve_cluster.member WHERE replica_id = @id", replicaId).ConfigureAwait(false);
        if (deleted > 0)
        {
            run.RecordEnvironment(replicaId, EnvironmentActions.OperatorDelete);
        }

        return deleted;
    }

    public static Task<int> MutateMemberRow(HarnessRun run, string replicaId) =>
        ExecuteAsync(run, "UPDATE reeve_cluster.member SET epoch = epoch + 1 WHERE replica_id = @id", replicaId);

    public static async Task RaiseProtocolFloor(HarnessRun run, int floor)
    {
        await using var connection = await run.OpenObserverConnectionAsync().ConfigureAwait(false);
        await using var command = new NpgsqlCommand("UPDATE reeve_cluster.cluster SET protocol_floor = @floor WHERE protocol_floor <= @floor", connection);
        command.Parameters.AddWithValue("floor", floor);
        if (await command.ExecuteNonQueryAsync().ConfigureAwait(false) == 0)
        {
            throw new InvalidOperationException($"The protocol floor is already above {floor}: a floor is raised, never lowered.");
        }

        run.RecordEnvironment(null, EnvironmentActions.FloorRaise, ("floor", floor));
    }

    public static void ClockShift(Replica replica, TimeSpan delta) => replica.Clock.Shift(delta);

    public static Task CutNetwork(Replica replica) =>
        (replica.Link ?? throw new InvalidOperationException($"Replica '{replica.Id}' has no proxies: the run has no stack.")).CutAsync();

    public static Task DatabaseOutage(HarnessRun run, TimeSpan downtime) =>
        (run.Stack ?? throw new InvalidOperationException("The run has no stack.")).Database.RestartAsync(downtime);

    public static void ReconcilerThrow(Replica replica, string aggregateType, Func<Exception> fault, int times = 1) =>
        replica.Reconcilers.ThrowNext(aggregateType, fault, times);

    public static Faults.ReconcilerHang ReconcilerHang(Replica replica, string aggregateType, Faults.HangMode mode = Faults.HangMode.Cooperative) =>
        replica.Reconcilers.HangNext(aggregateType, mode);

    public static void ReconcilerRelease(Faults.ReconcilerHang hang) => hang.Release();

    public static void FixFault(HarnessRun run)
    {
        run.RecordEnvironment(null, EnvironmentActions.FixFault);
        foreach (var replica in run.Replicas.Where(r => !r.IsCrashed))
        {
            replica.Reconcilers.ClearFaults();
        }
    }

    public static void ReportFatalFault(Replica replica, string reason) =>
        replica.Services.GetRequiredService<IPhaseGate>().ReportFatalFault(reason);

    public static void ReportFatalFaultWhileFenced(Replica replica, string reason) =>
        replica.Services.GetRequiredService<IPhaseGate>().ReportFatalFault(reason);

    public static Task<HarnessWrite> StartWrite(Replica replica, CancellationToken cancellationToken = default) =>
        HarnessWrite.BeginAsync(replica, cancellationToken);

    public static async Task<InvalidOperationException> SaveInForeignTransaction(HarnessWrite write, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(write);
        await using var db = HarnessCapture.Context(write.Replica, write.Connection);
        await db.Database.UseTransactionAsync(write.Transaction, cancellationToken).ConfigureAwait(false);
        db.Add(new HarnessCaptured { Id = 1, Label = "refused" });
        try
        {
            await db.SaveChangesAsync(cancellationToken).ConfigureAwait(false);
        }
        catch (InvalidOperationException refused)
        {
            return refused;
        }

        throw new InvalidOperationException("The capturing save in a transaction handed to EF with UseTransaction was not refused.");
    }

    public static Task RollbackWrite(HarnessWrite write, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(write);
        return write.RollbackAsync(cancellationToken);
    }

    private static async Task<int> ExecuteAsync(HarnessRun run, string sql, string replicaId)
    {
        await using var connection = await run.OpenObserverConnectionAsync().ConfigureAwait(false);
        await using var command = new NpgsqlCommand(sql, connection);
        command.Parameters.AddWithValue("id", replicaId);
        return await command.ExecuteNonQueryAsync().ConfigureAwait(false);
    }
}
