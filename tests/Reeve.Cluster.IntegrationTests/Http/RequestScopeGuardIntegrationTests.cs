using Microsoft.AspNetCore.Builder;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.DependencyInjection;
using Npgsql;
using Microsoft.Extensions.Options;
using Reeve.Cluster;
using Reeve.Cluster.Core;
using Reeve.Cluster.Harness.Containers;
using Reeve.Cluster.EntityFrameworkCore;
using Reeve.Cluster.Log;
using Reeve.Cluster.Membership;
using Int.Log;

namespace Int.Http;

public sealed class RequestScopeGuardIntegrationTests : IClassFixture<PostgresFixture>, IAsyncLifetime
{
    private readonly PostgresFixture postgres;
    private readonly List<IAsyncDisposable> owned = [];

    public RequestScopeGuardIntegrationTests(PostgresFixture postgres) => this.postgres = postgres;

    public Task InitializeAsync() => Task.CompletedTask;

    public async Task DisposeAsync()
    {
        foreach (var item in Enumerable.Reverse(owned))
        {
            await item.DisposeAsync();
        }
    }

    [Fact]
    public async Task Middleware_transaction_before_cluster_refuses_endpoint_mapped_save_and_rolls_back()
    {
        var host = await HostAsync((app, rig) => app.Use(TransactionMiddleware(rig)));
        var error = await Record.ExceptionAsync(() => host.Client.PostAsync("/guard/mapped", null));
        AssertReason(error);
        Assert.Equal(0, await host.Rig.Log.ScalarAsync<long>("SELECT count(*) FROM public.config WHERE id = 900002"));
        Assert.Equal(0, await host.Rig.Log.ScalarAsync<long>("SELECT count(*) FROM reeve_cluster.change_log WHERE aggregate_type = 'a' AND aggregate_key = '900002'"));
    }

    [Fact]
    public async Task Middleware_transaction_before_cluster_allows_endpoint_unmapped_save()
    {
        var host = await HostAsync((app, rig) => app.Use(TransactionMiddleware(rig)));
        using var response = await host.Client.PostAsync("/guard/audit", null);
        Assert.True(response.IsSuccessStatusCode);
        Assert.Equal(1, await host.GuardAuditCountAsync());
    }

    [Fact]
    public async Task Middleware_save_after_next_is_refused()
    {
        var host = await HostAsync((app, rig) => app.Use(TransactionMiddleware(rig, saveAfterNext: true)));
        var error = await Record.ExceptionAsync(() => host.Client.PostAsync("/guard/after", null));
        AssertReason(error);
        Assert.Equal(0, await host.Rig.Log.ScalarAsync<long>("SELECT count(*) FROM public.config WHERE id = 900001"));
    }

    [Fact]
    public async Task Endpoint_transaction_with_mapped_change_is_refused_when_middleware_saves_after_next()
    {
        var host = await HostAsync((app, rig) => app.Use(TransactionMiddleware(rig, saveAfterNext: true, beginInEndpoint: true)));
        var error = await Record.ExceptionAsync(() => host.Client.PostAsync("/guard/endpoint-transaction", null));
        AssertReason(error);
        Assert.Equal(0, await host.Rig.Log.ScalarAsync<long>("SELECT count(*) FROM public.config WHERE id = 900003"));
        Assert.Equal(0, await host.Rig.Log.ScalarAsync<long>("SELECT count(*) FROM reeve_cluster.change_log WHERE aggregate_type = 'a' AND aggregate_key = '900003'"));
    }

    [Fact]
    public async Task Joiner_after_next_refuses_using_pre_scope_beginning_pair()
    {
        var host = await HostAsync((app, rig) => app.Use(TransactionMiddleware(rig, saveJoinedAfterNext: true)));
        var error = await Record.ExceptionAsync(() => host.Client.PostAsync("/guard/joined-context", null));
        AssertReason(error);
        Assert.Equal(0, await host.Rig.Log.ScalarAsync<long>("SELECT count(*) FROM public.config WHERE id = 900004"));
        Assert.Equal(0, await host.Rig.Log.ScalarAsync<long>("SELECT count(*) FROM reeve_cluster.change_log WHERE aggregate_type = 'a' AND aggregate_key = '900004'"));
    }

    private async Task<HttpHostRig> HostAsync(Action<IApplicationBuilder, Log.ApplierRig> middleware)
    {
        var rig = await ApplierRig.CreateAsync(postgres, ["a", "b"], null, o => o.ReadIndexTimeout = TimeSpan.FromSeconds(10));
        owned.Add(rig);
        await rig.LoadAsync();
        await using (var connection = await rig.Log.OpenAsync())
        {
            await using var command = new NpgsqlCommand("CREATE TABLE IF NOT EXISTS public.guard_audit (id int PRIMARY KEY, text text NOT NULL); TRUNCATE public.guard_audit", connection);
            await command.ExecuteNonQueryAsync();
        }
        rig.Gate.Apply(MemberTrigger.StateLoaded);
        rig.Gate.Apply(MemberTrigger.ActivationCommitted);
        rig.Gate.Apply(MemberTrigger.AppliedToActivationHead);
        var index = new ReadIndex(rig.Gate, rig.Store, new ReadIndexStore(rig.Data), rig.Channel, Options.Create(rig.Options), TimeProvider.System,
            NoFaultPoints.Instance, rig.Events, NullLogger<ReadIndex>.Instance);
        owned.Add(new Disposing(index));
        var host = new HttpHostRig(rig, index, beforeCluster: app => middleware(app, rig));
        owned.Add(host);
        return host;
    }

    private static Func<Microsoft.AspNetCore.Http.RequestDelegate, Microsoft.AspNetCore.Http.RequestDelegate> TransactionMiddleware(
        Log.ApplierRig rig, bool saveAfterNext = false, bool beginInEndpoint = false, bool saveJoinedAfterNext = false) => next => async context =>
    {
        if (!context.Request.Path.StartsWithSegments("/guard"))
        {
            await next(context);
            return;
        }

        await using var connection = await rig.Log.OpenAsync();
        var db = new ConfigDb(new DbContextOptionsBuilder<ConfigDb>().UseNpgsql(connection)
            .AddInterceptors(context.RequestServices.GetRequiredService<ClusterCaptureInterceptor>()).Options);
        await using (db)
        {
            var transaction = beginInEndpoint ? null : await db.Database.BeginTransactionAsync();
            context.Items["guard-db"] = db;
            await next(context);
            if (saveAfterNext)
            {
                db.Rows.Add(new ConfigRow { Id = 900001, Value = 1 });
                await db.SaveChangesAsync();
            }
            if (saveJoinedAfterNext)
            {
                var joiner = (ConfigDb)context.Items["guard-joiner"]!;
                await using (joiner)
                {
                    await joiner.SaveChangesAsync();
                }
            }
            if (transaction is not null)
            {
                await using (transaction)
                {
                    await transaction.CommitAsync();
                }
            }
        }
    };

    private sealed class Disposing(IDisposable inner) : IAsyncDisposable
    {
        public ValueTask DisposeAsync()
        {
            inner.Dispose();
            return ValueTask.CompletedTask;
        }
    }

    private static void AssertReason(Exception? error)
    {
        Assert.NotNull(error);
        while (error is not null && error is not ClusterConfigurationException)
        {
            error = error.InnerException;
        }
        var refusal = Assert.IsType<ClusterConfigurationException>(error);
        Assert.Equal(ClusterConfigurationReason.WriteOutsideClusterScope, refusal.Reason);
    }
}
