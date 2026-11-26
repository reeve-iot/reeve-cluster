using Harness.Slice;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.DependencyInjection;
using Npgsql;
using Reeve.Cluster.Harness.Replicas;
using Reeve.Cluster.Harness.Stack;
using Reeve.Cluster.Harness.Stimuli;
using Reeve.Cluster.Harness.Timing;
using Reeve.Cluster.Log;
using Reeve.Cluster.Membership;

namespace Harness.Failover;

internal sealed class FailoverRig : IAsyncDisposable
{
    public static readonly TimeSpan Limit = TimeSpan.FromSeconds(90);
    private const string RenewalApplication = "reeve-cluster renewal";
    private const string LostApplication = "lost-write";
    private readonly Dictionary<Replica, HttpClient> clients = [];
    private readonly ClusterStack? ownedStack;

    private FailoverRig(HarnessRun run, ClusterStack? ownedStack)
    {
        Run = run;
        this.ownedStack = ownedStack;
    }

    public HarnessRun Run { get; }

    public ClusterStack Stack => Run.Stack!;

    public static async Task<FailoverRig> CreateAsync(Action<string> log, StandbyMode mode = StandbyMode.SyncFirst)
    {
        var stack = new ClusterStack(log, new ClusterStackOptions { Standby = true });
        try
        {
            await stack.InitializeAsync();
            var rig = await OnAsync(stack, log, ownsStack: true);
            await stack.Pair.SetModeAsync(mode);
            return rig;
        }
        catch
        {
            await stack.DisposeAsync();
            throw;
        }
    }

    public static async Task<FailoverRig> OnAsync(ClusterStack stack, Action<string> log, bool ownsStack = false)
    {
        var run = await HarnessRun.BeginAsync(log, stack);
        try
        {
            await run.MigrateAsync();
            await using var connection = await run.OpenObserverConnectionAsync();
            await using var command = new NpgsqlCommand(FailoverApp.CreateTables, connection);
            await command.ExecuteNonQueryAsync();
            return new FailoverRig(run, ownsStack ? stack : null);
        }
        catch
        {
            await run.DisposeAsync();
            throw;
        }
    }

    public static void Profile(ClusterOptions options)
    {
        TestProfile.ApplyLibraryDefaults(options);
        options.ApplyStallLimit = TestProfile.TolerantApplyStallLimit;
    }

    public static ReplicaSpec Spec(bool redis = true) => FailoverApp.Spec(
        profile: Profile,
        configure: o =>
        {
            o.ReplicationCheckInterval = TimeSpan.FromSeconds(1);
            o.RejoinBackoff = TestProfile.RejoinBackoff;
        },
        redis: redis);

    public async Task<Replica> StartAsync(string id, ReplicaSpec? spec = null)
    {
        var replica = await Run.PrepareAsync(id, spec ?? Spec());
        await replica.StartAsync();
        await replica.Started!.WaitAsync(Limit);
        await SliceRig.WaitForPhaseAsync(replica, ReplicaPhase.Serving, Limit);
        return replica;
    }

    public async Task<(Replica R1, Replica R2)> StartPairAsync(ReplicaSpec? spec = null)
    {
        var r1 = await StartAsync("r1", spec);
        var r2 = await StartAsync("r2", spec);
        return (r1, r2);
    }

    public HttpClient Client(Replica replica)
    {
        lock (clients)
        {
            if (!clients.TryGetValue(replica, out var client))
            {
                clients[replica] = client = new HttpClient { BaseAddress = replica.Url, Timeout = TimeSpan.FromSeconds(90) };
            }

            return client;
        }
    }

    public async Task<long> PutAsync(Replica replica, string id, string value)
    {
        using var response = await Client(replica).PutAsync($"items/{id}?value={value}", null);
        response.EnsureSuccessStatusCode();
        return (await SliceRig.ItemAsync(response)).Seq!.Value;
    }

    public async Task<long> AppendOtherTypeAsync(Replica replica, int entries)
    {
        using var scope = replica.Services.GetRequiredService<IClusterScopeAccessor>().BeginBackground();
        await using var connection = await Run.OpenObserverConnectionAsync();
        await using var transaction = await connection.BeginTransactionAsync();
        var seq = await replica.Services.GetRequiredService<IChangeLog>()
            .AppendAsync(transaction, [.. Enumerable.Range(0, entries).Select(_ => new ChangeEntry("ledger", $"l{Guid.NewGuid():N}"))]);
        await transaction.CommitAsync();
        return seq;
    }

    public async Task<long> TimelineAsync()
    {
        await using var connection = await Run.OpenObserverConnectionAsync();
        return await ScalarAsync(connection, $"SELECT {MemberStore.TimelineSql}");
    }

    public async Task<long> HeadAsync()
    {
        await using var connection = await Run.OpenObserverConnectionAsync();
        return await ScalarAsync(connection, "SELECT coalesce(max(seq), 0) FROM reeve_cluster.change_log");
    }

    public async Task<long> ScalarAsync(string sql)
    {
        await using var connection = await Run.OpenObserverConnectionAsync();
        return await ScalarAsync(connection, sql);
    }

    public async Task WaitStandbyGoneAsync()
    {
        await using var connection = await Stack.OpenObserverConnectionAsync();
        await SliceRig.UntilAsync(async () => await ScalarAsync(connection, "SELECT count(*) FROM pg_stat_replication") == 0, "the primary to notice its standby is gone", Limit);
    }

    public async Task<long> LostAppendAsync(Replica replica, string prefix, int entries)
    {
        var write = Task.Run(async () =>
        {
            using var scope = replica.Services.GetRequiredService<IClusterScopeAccessor>().BeginBackground();
            await using var connection = LostConnection(replica);
            await connection.OpenAsync();
            await using var transaction = await connection.BeginTransactionAsync();
            for (var i = 0; i < entries; i++)
            {
                await using var insert = new NpgsqlCommand("INSERT INTO slice_item (type, id, value) VALUES ('widget', $1, 'lost')", connection, transaction)
                {
                    Parameters = { new NpgsqlParameter<string> { TypedValue = $"{prefix}{i}" } },
                };
                await insert.ExecuteNonQueryAsync();
            }

            var seq = await replica.Services.GetRequiredService<IChangeLog>()
                .AppendAsync(transaction, [.. Enumerable.Range(0, entries).Select(i => new ChangeEntry(FailoverApp.Widget, $"{prefix}{i}"))]);
            await transaction.CommitAsync();
            return seq;
        });
        return await write.WaitAsync(Limit);
    }

    public async Task<long> LostCapturedWriteAsync(Replica replica, int id, string label)
    {
        var write = Task.Run(async () =>
        {
            using var request = replica.Services.GetRequiredService<IClusterScopeSource>().Begin(ClusterScopeKind.ClientRequest);
            await using var connection = LostConnection(replica);
            await connection.OpenAsync();
            await using var db = HarnessCapture.Context(replica, connection);
            db.Add(new HarnessCaptured { Id = id, Label = label });
            await db.SaveChangesAsync();
        });
        await write.WaitAsync(Limit);
        return await HeadAsync();
    }

    public async Task<long> LostSkipOpenAsync(Replica replica, long fromSeq)
    {
        var open = Task.Run(() => replica.Services.GetRequiredService<IChangeSkip>().OpenAsync(new SkipRequest(FailoverApp.Widget, fromSeq, "lost", "ops")));
        return Assert.IsType<SkipOpenResult.Opened>(await open.WaitAsync(Limit)).SkipId;
    }

    public IAsyncDisposable CancelSyncWaits() => new SyncWaitCanceller(this);

    private sealed class SyncWaitCanceller : IAsyncDisposable
    {
        private readonly CancellationTokenSource stop = new();
        private readonly Task loop;

        public SyncWaitCanceller(FailoverRig rig) => loop = Task.Run(() => Loop(rig));

        public async ValueTask DisposeAsync()
        {
            await stop.CancelAsync();
            await loop;
            stop.Dispose();
        }

        private async Task Loop(FailoverRig rig)
        {
            await using var connection = await rig.Run.OpenObserverConnectionAsync();
            while (!stop.IsCancellationRequested)
            {
                await ScalarAsync(
                    connection,
                    $"SELECT count(*) FROM (SELECT pg_cancel_backend(pid) FROM pg_stat_activity WHERE datname = current_database() AND wait_event = 'SyncRep' AND application_name <> '{RenewalApplication}') c");
                await Task.Delay(20, CancellationToken.None);
            }
        }
    }

    private static NpgsqlConnection LostConnection(Replica replica) =>
        new(new NpgsqlConnectionStringBuilder(replica.Options.ConnectionString) { ApplicationName = LostApplication, Pooling = false }.ConnectionString);

    private static async Task<long> ScalarAsync(NpgsqlConnection connection, string sql)
    {
        await using var command = new NpgsqlCommand(sql, connection);
        var value = await command.ExecuteScalarAsync();
        return value is bool b ? (b ? 1 : 0) : Convert.ToInt64(value, System.Globalization.CultureInfo.InvariantCulture);
    }

    public async ValueTask DisposeAsync()
    {
        foreach (var client in clients.Values)
        {
            client.Dispose();
        }

        List<Exception>? failures = null;
        try
        {
            await Run.DisposeAsync();
        }
        catch (Exception exception)
        {
            (failures ??= []).Add(exception);
        }

        if (ownedStack is not null)
        {
            try
            {
                await ownedStack.DisposeAsync();
            }
            catch (Exception exception)
            {
                (failures ??= []).Add(exception);
            }
        }

        if (failures is not null)
        {
            throw new AggregateException("Disposing the failover rig failed.", failures);
        }
    }
}
