using System.Data.Common;
using Int.Log;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Storage;
using Npgsql;
using Reeve.Cluster;
using Reeve.Cluster.Harness.Containers;
using Reeve.Cluster.Log;
using Reeve.Cluster.Wakeup;

namespace Int.Capture;

public enum ConnectionLoss
{
    TerminatedBackend,

    SocketReset,
}

public sealed class ConnectionLossRetryTests : IClassFixture<PostgresFixture>
{
    private readonly PostgresFixture postgres;

    public ConnectionLossRetryTests(PostgresFixture postgres)
    {
        this.postgres = postgres;
    }

    private Task<CaptureRig> RigAsync() => CaptureRig.CreateAsync(postgres);

    private static Setting NewSetting(int id) => new() { Id = id, Name = "n" + id, Value = "v" };

    [Theory]
    [InlineData(ConnectionLoss.TerminatedBackend)]
    [InlineData(ConnectionLoss.SocketReset)]
    public async Task A_connection_lost_under_the_append_surfaces_as_a_transient_NpgsqlException(ConnectionLoss loss)
    {
        await using var rig = await RigAsync();
        using var request = rig.Log.Scopes.Begin(ClusterScopeKind.ClientRequest);
        await using var link = await Link.OpenAsync(rig, postgres, loss);
        await using var db = rig.NewContext(link.Connection);
        await using var transaction = await db.Database.BeginTransactionAsync();
        db.Settings.Add(NewSetting(1));
        await db.SaveChangesAsync();
        await link.BreakAsync();

        var failure = await Assert.ThrowsAnyAsync<Exception>(() => transaction.CommitAsync());

        var lost = Assert.IsAssignableFrom<NpgsqlException>(failure);
        Assert.True(lost.IsTransient, Describe(lost));
        Assert.Equal(0, rig.Interceptor.PairCount);
        Assert.Empty(request.Scope.OpenTransactions);
        Assert.Equal(0, await rig.CountAsync("public.setting"));
        Assert.Empty(await rig.EntriesAsync());
    }

    [Theory]
    [InlineData(ConnectionLoss.TerminatedBackend)]
    [InlineData(ConnectionLoss.SocketReset)]
    public async Task A_connection_lost_under_an_explicit_transactions_commit_is_retried_and_appended_once(ConnectionLoss loss)
    {
        await using var rig = await RigAsync();
        using var request = rig.Log.Scopes.Begin(ClusterScopeKind.ClientRequest);
        await using var link = await Link.OpenAsync(rig, postgres, loss);
        await using var db = HandOverTests.Retrying(rig, link.Connection);
        var attempts = 0;

        await db.Database.CreateExecutionStrategy().ExecuteAsync(async () =>
        {
            attempts++;
            db.ChangeTracker.Clear();
            await using var transaction = await db.Database.BeginTransactionAsync();
            db.Settings.Add(NewSetting(1));
            await db.SaveChangesAsync();
            if (attempts == 1)
            {
                await link.BreakAsync();
            }

            await transaction.CommitAsync();
        });

        Assert.Equal(2, attempts);
        await AssertAppendedOnceAsync(rig, request.Scope.Seqs, request.Scope.OpenTransactions);
    }

    [Theory]
    [InlineData(ConnectionLoss.TerminatedBackend)]
    [InlineData(ConnectionLoss.SocketReset)]
    public async Task A_connection_lost_under_a_capturing_implicit_save_held_before_the_append_is_retried_and_appended_once(ConnectionLoss loss)
    {
        await using var rig = await RigAsync();
        using var request = rig.Log.Scopes.Begin(ClusterScopeKind.ClientRequest);
        await using var link = await Link.OpenAsync(rig, postgres, loss);
        await using var db = HandOverTests.Retrying(rig, link.Connection);
        db.Settings.Add(NewSetting(1));

        using (var hold = rig.Ack.Skip.Faults.HoldNext(CaptureFaultPoints.CommittingBeforeAppend))
        {
            var save = db.SaveChangesAsync();
            await hold.WaitReachedAsync();
            await link.BreakAsync();
            hold.Release();

            await save;
        }

        Assert.Equal(2, rig.Ack.Skip.Faults.Reached(CaptureFaultPoints.CommittingBeforeAppend));
        await AssertAppendedOnceAsync(rig, request.Scope.Seqs, request.Scope.OpenTransactions);
    }

    [Theory]
    [InlineData(ConnectionLoss.TerminatedBackend)]
    [InlineData(ConnectionLoss.SocketReset)]
    public async Task A_connection_lost_under_the_beginning_contexts_commit_of_a_hand_over_is_retried_and_appended_once(ConnectionLoss loss)
    {
        await using var rig = await RigAsync();
        using var request = rig.Log.Scopes.Begin(ClusterScopeKind.ClientRequest);
        await using var link = await Link.OpenAsync(rig, postgres, loss);
        await using var first = HandOverTests.Retrying(rig, link.Connection);
        var attempts = 0;

        await first.Database.CreateExecutionStrategy().ExecuteAsync(async () =>
        {
            attempts++;
            await using var transaction = await first.Database.BeginTransactionAsync();
            await using var second = HandOverTests.Retrying(rig, link.Connection);
            await second.Database.UseTransactionAsync(transaction.GetDbTransaction());
            second.Settings.Add(NewSetting(1));
            await second.SaveChangesAsync();
            if (attempts == 1)
            {
                await link.BreakAsync();
            }

            await transaction.CommitAsync();
        });

        Assert.Equal(2, attempts);
        await AssertAppendedOnceAsync(rig, request.Scope.Seqs, request.Scope.OpenTransactions);
    }

    [Fact]
    public async Task A_commit_after_a_statement_found_the_break_throws_what_a_commit_without_the_capture_throws()
    {
        await using var rig = await RigAsync();
        using var request = rig.Log.Scopes.Begin(ClusterScopeKind.ClientRequest);

        var bare = await CommitAfterFoundBreakAsync(rig, capture: false);
        var captured = await CommitAfterFoundBreakAsync(rig, capture: true);

        Assert.Equal(bare.GetType(), captured.GetType());
        Assert.Equal(0, await rig.CountAsync("public.setting"));
        Assert.Empty(await rig.EntriesAsync());
    }

    private static async Task<Exception> CommitAfterFoundBreakAsync(CaptureRig rig, bool capture)
    {
        await using var connection = await rig.OpenAsync();
        await using var db = capture
            ? rig.NewContext(connection)
            : new CaptureDb(new DbContextOptionsBuilder<CaptureDb>().UseNpgsql(connection).Options);
        await using var transaction = await db.Database.BeginTransactionAsync();
        db.Settings.Add(NewSetting(capture ? 1 : 2));
        await db.SaveChangesAsync();
        await HandOverTests.TerminateAsync(rig, connection);
        await Assert.ThrowsAnyAsync<NpgsqlException>(() => db.Database.ExecuteSqlRawAsync("SELECT 1"));

        return await Assert.ThrowsAnyAsync<Exception>(() => transaction.CommitAsync());
    }

    private static async Task AssertAppendedOnceAsync(CaptureRig rig, IReadOnlyList<long> seqs, IReadOnlyCollection<DbTransaction> openTransactions)
    {
        Assert.Equal(1, await rig.CountAsync("public.setting"));
        Assert.Equal([("setting", "n1")], await rig.EntriesAsync());
        var seq = Assert.Single(seqs);
        Assert.Equal([new Wakeup(WakeupKind.Appended, seq)], rig.Wakeups);
        Assert.Empty(openTransactions);
        Assert.Equal(0, rig.Interceptor.PairCount);
        Assert.Empty(rig.Logger.Errors);
    }

    private static string Describe(NpgsqlException exception) =>
        $"{exception.GetType().Name}, SqlState {(exception as PostgresException)?.SqlState}, inner {exception.InnerException?.GetType().Name}";

    private sealed class Link : IAsyncDisposable
    {
        private readonly CaptureRig rig;
        private readonly HoldingProxy? proxy;

        private Link(CaptureRig rig, NpgsqlConnection connection, HoldingProxy? proxy)
        {
            this.rig = rig;
            this.proxy = proxy;
            Connection = connection;
        }

        public NpgsqlConnection Connection { get; }

        public static async Task<Link> OpenAsync(CaptureRig rig, PostgresFixture postgres, ConnectionLoss loss)
        {
            if (loss == ConnectionLoss.TerminatedBackend)
            {
                return new Link(rig, await rig.OpenAsync(), null);
            }

            var server = new NpgsqlConnectionStringBuilder(postgres.ConnectionString);
            var proxy = new HoldingProxy(server.Host!, server.Port);
            var connectionString = new NpgsqlConnectionStringBuilder(AckWaitCancelTests.Proxied(rig.Log.ConnectionString, proxy)) { Pooling = false }.ConnectionString;
            var connection = new NpgsqlConnection(connectionString);
            await connection.OpenAsync();
            return new Link(rig, connection, proxy);
        }

        public async Task BreakAsync()
        {
            if (proxy is null)
            {
                await HandOverTests.TerminateAsync(rig, Connection);
            }
            else
            {
                proxy.Reset();
            }
        }

        public async ValueTask DisposeAsync()
        {
            await Connection.DisposeAsync();
            if (proxy is not null)
            {
                await proxy.DisposeAsync();
            }
        }
    }
}
