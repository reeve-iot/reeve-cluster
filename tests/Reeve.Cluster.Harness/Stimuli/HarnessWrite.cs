using Microsoft.Extensions.DependencyInjection;
using Npgsql;
using Reeve.Cluster.Harness.Replicas;
using Reeve.Cluster.Log;

namespace Reeve.Cluster.Harness.Stimuli;

internal sealed class HarnessWrite : IAsyncDisposable
{
    private readonly Replica replica;
    private readonly IClusterScopeHandle request;
    private readonly NpgsqlConnection connection;
    private readonly NpgsqlTransaction transaction;
    private long highest;

    private HarnessWrite(Replica replica, IClusterScopeHandle request, NpgsqlConnection connection, NpgsqlTransaction transaction, string xid)
    {
        this.replica = replica;
        this.request = request;
        this.connection = connection;
        this.transaction = transaction;
        Xid = xid;
    }

    public string Xid { get; }

    public IClusterScope Scope => request.Scope;

    public NpgsqlTransaction Transaction => transaction;

    public NpgsqlConnection Connection => connection;

    public Replica Replica => replica;

    internal static async Task<HarnessWrite> BeginAsync(Replica replica, CancellationToken cancellationToken)
    {
        var request = replica.Services.GetRequiredService<IClusterScopeSource>().Begin(ClusterScopeKind.ClientRequest);
        var connection = new NpgsqlConnection(replica.Options.ConnectionString);
        try
        {
            await connection.OpenAsync(cancellationToken).ConfigureAwait(false);
            var transaction = await connection.BeginTransactionAsync(cancellationToken).ConfigureAwait(false);
            await using var command = new NpgsqlCommand("SELECT pg_current_xact_id()::text", connection, transaction);
            var xid = (string)(await command.ExecuteScalarAsync(cancellationToken).ConfigureAwait(false))!;
            var write = new HarnessWrite(replica, request, connection, transaction, xid);
            write.Emit(LogActions.WBegin);
            return write;
        }
        catch
        {
            await connection.DisposeAsync().ConfigureAwait(false);
            request.Dispose();
            throw;
        }
    }

    public async Task<long> AppendAsync(IReadOnlyCollection<ChangeEntry> entries, CancellationToken cancellationToken = default)
    {
        var log = replica.Services.GetRequiredService<ChangeLog>();
        var seq = (await log.AppendAsync(transaction, entries, (IClusterScopeWriter)request.Scope, cancellationToken).ConfigureAwait(false)).HighestSeq;
        highest = Math.Max(highest, seq);
        return seq;
    }

    public async Task CommitAsync(CancellationToken cancellationToken = default)
    {
        await transaction.CommitAsync(cancellationToken).ConfigureAwait(false);
        if (highest > 0)
        {
            await replica.Services.GetRequiredService<PostCommitWakeup>().AfterCommitAsync(highest, Xid, cancellationToken).ConfigureAwait(false);
        }
    }

    public async Task RollbackAsync(CancellationToken cancellationToken = default)
    {
        await transaction.RollbackAsync(cancellationToken).ConfigureAwait(false);
        Emit(LogActions.WRollback);
    }

    public async ValueTask DisposeAsync()
    {
        await transaction.DisposeAsync().ConfigureAwait(false);
        await connection.DisposeAsync().ConfigureAwait(false);
        request.Dispose();
    }

    private void Emit(string action) =>
        replica.Services.GetRequiredService<IModelEventSink>().Emit(new ModelEvent(action, new Dictionary<string, object?>
        {
            [ObservationKeys.Xid] = Xid,
            [ObservationKeys.Request] = (request.Scope as ClusterScope)?.Id,
        }));
}
