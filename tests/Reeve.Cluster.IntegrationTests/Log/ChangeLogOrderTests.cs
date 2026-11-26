using System.Collections.Concurrent;
using Npgsql;
using Reeve.Cluster;
using Reeve.Cluster.Harness.Containers;
using Testcontainers.PostgreSql;

namespace Int.Log;

public sealed class CommitTimestampPostgres : IAsyncLifetime
{
    private readonly PostgreSqlContainer container = new PostgreSqlBuilder(PostgresFixture.Image)
        .WithCommand("-c", "track_commit_timestamp=on", "-c", "max_connections=200")
        .Build();

    public string ConnectionString => container.GetConnectionString();

    public Task InitializeAsync() => container.StartAsync();

    public Task DisposeAsync() => container.DisposeAsync().AsTask();
}

public sealed class ChangeLogOrderTests : IClassFixture<CommitTimestampPostgres>
{
    private static readonly TimeSpan Bound = TimeSpan.FromSeconds(30);

    private readonly CommitTimestampPostgres postgres;

    public ChangeLogOrderTests(CommitTimestampPostgres postgres)
    {
        this.postgres = postgres;
    }

    private Task<LogRig> RigAsync(IFaultPoints? points = null) => LogRig.CreateAsync(postgres.ConnectionString, points);

    [Fact]
    public async Task I3_Seqs_are_in_commit_order_under_concurrent_writers_every_snapshot_sees_a_prefix_and_a_transactions_seqs_are_contiguous()
    {
        const int Writers = 8;
        const int TransactionsEach = 40;
        var rig = await RigAsync();
        var writes = new ConcurrentBag<(List<long> Seqs, bool Committed)>();
        var snapshots = new ConcurrentBag<List<long>>();
        using var done = new CancellationTokenSource();

        var observer = Task.Run(async () =>
        {
            await using var connection = await rig.OpenAsync();
            while (!done.IsCancellationRequested)
            {
                snapshots.Add(await LogRig.SeqsAsync(connection));
            }
        });

        var writers = Enumerable.Range(0, Writers).Select(w => Task.Run(async () =>
        {
            var random = new Random(1000 + w);
            await using var connection = await rig.OpenAsync();
            for (var t = 0; t < TransactionsEach; t++)
            {
                using var request = rig.Scopes.Begin(ClusterScopeKind.ClientRequest);
                await using var transaction = await connection.BeginTransactionAsync();
                await LogRig.TouchAsync(transaction, random.Next(1, 65));
                await rig.Log.AppendAsync(transaction, LogRig.Entries([.. Enumerable.Range(0, random.Next(1, 4)).Select(i => $"type{i}")]));
                if (random.Next(4) == 0)
                {
                    await Task.Yield();
                    await rig.Log.AppendAsync(transaction, LogRig.Entries("late"));
                }

                var commit = random.Next(10) != 0;
                if (commit)
                {
                    await transaction.CommitAsync();
                }
                else
                {
                    await transaction.RollbackAsync();
                }

                writes.Add(([.. request.Scope.Seqs], commit));
            }
        })).ToList();

        await Task.WhenAll(writers).WaitAsync(Bound * 4);
        await done.CancelAsync();
        await observer.WaitAsync(Bound);

        var committed = await rig.SeqsAsync();
        Assert.Equal(Writers * TransactionsEach, writes.Count);

        foreach (var (seqs, _) in writes)
        {
            Assert.Equal(Enumerable.Range((int)seqs[0], seqs.Count).Select(s => (long)s), seqs);
        }

        Assert.Equal(writes.Where(w => w.Committed).SelectMany(w => w.Seqs).Order(), committed);
        Assert.Contains(writes, w => !w.Committed);

        await using (var connection = await rig.OpenAsync())
        await using (var command = new NpgsqlCommand("SELECT pg_xact_commit_timestamp(xmin) FROM reeve_cluster.change_log ORDER BY seq", connection))
        await using (var reader = await command.ExecuteReaderAsync())
        {
            DateTime? previous = null;
            while (await reader.ReadAsync())
            {
                var at = reader.GetDateTime(0);
                Assert.True(previous is null || previous <= at, $"A later seq committed at {at:O}, before an earlier one at {previous:O}.");
                previous = at;
            }
        }

        Assert.True(snapshots.Count > 10, $"The observer read only {snapshots.Count} snapshots.");
        foreach (var snapshot in snapshots.Where(s => s.Count > 0))
        {
            Assert.Equal(committed.Where(c => c <= snapshot[^1]), snapshot);
        }
    }

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public async Task I3_The_transaction_of_entry_n_has_ended_before_n_plus_1_is_assigned(bool firstCommits)
    {
        var rig = await RigAsync();
        await using var first = await rig.OpenAsync();
        await using var firstTransaction = await first.BeginTransactionAsync();
        var firstSeq = await rig.Log.AppendAsync(firstTransaction, LogRig.Entries("a"));

        var second = Task.Run(async () =>
        {
            await using var connection = await rig.OpenAsync();
            await using var transaction = await connection.BeginTransactionAsync();
            var seq = await rig.Log.AppendAsync(transaction, LogRig.Entries("b"));
            await transaction.CommitAsync();
            return seq;
        });

        await rig.UntilSomeoneWaitsForTheLockAsync(Bound);
        Assert.False(second.IsCompleted, "The second append was assigned a seq while the first transaction held the lock.");

        if (firstCommits)
        {
            await firstTransaction.CommitAsync();
        }
        else
        {
            await firstTransaction.RollbackAsync();
        }

        var secondSeq = await second.WaitAsync(Bound);
        Assert.True(secondSeq > firstSeq);
        Assert.Equal(firstCommits ? new List<long> { firstSeq, secondSeq } : [secondSeq], await rig.SeqsAsync());
    }

    [Fact]
    public async Task I3_A_transactions_seqs_stay_contiguous_across_two_appends_while_another_writer_waits_between_them()
    {
        var rig = await RigAsync();
        using var request = rig.Scopes.Begin(ClusterScopeKind.ClientRequest);
        await using var connection = await rig.OpenAsync();
        await using var transaction = await connection.BeginTransactionAsync();
        await rig.Log.AppendAsync(transaction, LogRig.Entries("a", "b"));

        var other = Task.Run(async () =>
        {
            using var otherRequest = rig.Scopes.Begin(ClusterScopeKind.ClientRequest);
            await using var otherConnection = await rig.OpenAsync();
            await using var otherTransaction = await otherConnection.BeginTransactionAsync();
            var seq = await rig.Log.AppendAsync(otherTransaction, LogRig.Entries("x"));
            await otherTransaction.CommitAsync();
            return seq;
        });
        await rig.UntilSomeoneWaitsForTheLockAsync(Bound);

        var highest = await rig.Log.AppendAsync(transaction, LogRig.Entries("c", "d", "e"));
        await transaction.CommitAsync();
        var otherSeq = await other.WaitAsync(Bound);

        var seqs = request.Scope.Seqs;
        Assert.Equal(Enumerable.Range((int)seqs[0], 5).Select(s => (long)s), seqs);
        Assert.Equal(seqs[^1], highest);
        Assert.Equal(highest + 1, otherSeq);
    }
}
