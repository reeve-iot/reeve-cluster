using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.TestHost;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Storage;
using Npgsql;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using Reeve.Cluster;
using Reeve.Cluster.AspNetCore;
using Reeve.Cluster.Core;
using Reeve.Cluster.EntityFrameworkCore;
using Reeve.Cluster.Log;
using Reeve.Cluster.Membership;

namespace Int.Http;

public sealed class AuditRow
{
    public int Id { get; set; }
    public string Text { get; set; } = "";
}

public sealed class ConfigRow
{
    public int Id { get; set; }

    public long Value { get; set; }
}

public sealed class ConfigDb(DbContextOptions<ConfigDb> options) : DbContext(options)
{
    public DbSet<ConfigRow> Rows => Set<ConfigRow>();

    public DbSet<AuditRow> Audits => Set<AuditRow>();

    protected override void OnModelCreating(ModelBuilder modelBuilder)
    {
        modelBuilder.Entity<ConfigRow>(e =>
        {
            e.ToTable("config");
            e.Property(x => x.Id).HasColumnName("id").ValueGeneratedNever();
            e.Property(x => x.Value).HasColumnName("value");
        });
        modelBuilder.Entity<AuditRow>(e =>
        {
            e.ToTable("guard_audit");
            e.Property(x => x.Id).HasColumnName("id").ValueGeneratedNever();
            e.Property(x => x.Text).HasColumnName("text");
        });
    }
}

public sealed record Seen(string? Kind, long? PinnedHead, long? PinnedHeadAfter = null);

internal sealed class HttpHostRig : IAsyncDisposable
{
    private readonly WebApplication app;
    private readonly AckDataSource skipData;

    public HttpHostRig(Log.ApplierRig rig, ReadIndex index, bool ackDatabaseDown = false, Func<HttpContext, ClusterHttpError, Task>? writeError = null, Action<IApplicationBuilder>? beforeCluster = null)
    {
        Rig = rig;
        Refusal = new CommitRefusal(rig.Gate, rig.Log.FenceSteps);
        Changes = new ChangeLog(rig.Log.Scopes, Refusal, rig.Log.EfTransactions, NoFaultPoints.Instance, rig.Events);
        var ackOptions = ackDatabaseDown
            ? new ClusterOptions { ConnectionString = "Host=127.0.0.1;Port=1;Username=x;Password=x;Database=x;Timeout=1;Command Timeout=1", ReplicaId = "r1", ReconcileTimeout = rig.Options.ReconcileTimeout }
            : rig.Options;
        var ackWrapped = Microsoft.Extensions.Options.Options.Create(ackOptions);
        var ackData = ackDatabaseDown ? new ClusterDataSource(ackOptions) : rig.Data;
        Capture = new ClusterCaptureInterceptor(
            ClusterMapping.Build(b => b.Entity<ConfigRow>("a").Key(r => r.Id.ToString(System.Globalization.CultureInfo.InvariantCulture)).AllColumns()),
            rig.Log.Scopes, Changes, Refusal, rig.Log.EfTransactions, new PostCommitWakeup(rig.Channel, NoFaultPoints.Instance, rig.Events),
            NoFaultPoints.Instance, rig.Events, NullLogger<ClusterCaptureInterceptor>.Instance);
        Ack = new ChangeAck(
            rig.Log.Scopes, Refusal, rig.Gate, rig.Store, ackData, new AckRecorder(new AckDataSource(ackWrapped), TimeProvider.System, NoFaultPoints.Instance, rig.Events),
            rig.Channel, ackWrapped, TimeProvider.System, Points, rig.Events, NullLogger<ChangeAck>.Instance);
        var builder = WebApplication.CreateSlimBuilder();
        builder.WebHost.UseTestServer();
        builder.Logging.ClearProviders();
        builder.Services.AddSingleton<IPhaseGate>(rig.Gate);
        builder.Services.AddSingleton<IGenerationSource>(rig.Store);
        builder.Services.AddSingleton(index);
        builder.Services.AddSingleton<IClusterScopeSource>(rig.Log.Scopes);
        builder.Services.AddSingleton<IClusterScopeAccessor>(rig.Log.Scopes);
        builder.Services.AddSingleton<IFaultPoints>(Points);
        builder.Services.AddSingleton<IChangeAck>(Ack);
        builder.Services.AddSingleton(TimeProvider.System);
        builder.Services.AddSingleton(Microsoft.Extensions.Options.Options.Create(rig.Options));
        skipData = new AckDataSource(Microsoft.Extensions.Options.Options.Create(rig.Options));
        Skips = new ChangeSkip(
            skipData, Microsoft.Extensions.Options.Options.Create(rig.Options), Changes,
            new PostCommitWakeup(rig.Channel, NoFaultPoints.Instance, rig.Events), NoFaultPoints.Instance, rig.Events);
        builder.Services.AddSingleton<IChangeSkip>(Skips);
        builder.Services.AddReeveClusterAspNetCore(o => o.WriteError = writeError);
        builder.Services.AddSingleton(Capture);

        app = builder.Build();
        beforeCluster?.Invoke(app);
        app.UseReeveCluster();
        app.MapGet("/plain", Now);
        app.MapGet("/exempt", Now).AllowWhenFenced();
        app.MapGet("/off/plain", Now).SkipReadIndex();
        app.MapGet("/off/exempt", Now).AllowWhenFenced().SkipReadIndex();
        app.MapPost("/write", (Delegate)Write).AcknowledgeWrites().SkipReadIndex();
        app.MapReeveClusterSkips("/skips").AllowAnonymous();
        app.MapPost("/ef", (Delegate)EfWrite).AcknowledgeWrites().SkipReadIndex();
        app.MapPost("/guard/mapped", async (HttpContext context) =>
        {
            var db = (ConfigDb)context.Items["guard-db"]!;
            db.Rows.Add(new ConfigRow { Id = 900002, Value = 99 });
            await db.SaveChangesAsync();
            return Results.Ok();
        });
        app.MapPost("/guard/endpoint-transaction", async (HttpContext context) =>
        {
            var db = (ConfigDb)context.Items["guard-db"]!;
            await db.Database.BeginTransactionAsync();
            db.Rows.Add(new ConfigRow { Id = 900003, Value = 99 });
            return Results.Ok();
        });
        app.MapPost("/guard/joined-context", (HttpContext context) =>
        {
            var owner = (ConfigDb)context.Items["guard-db"]!;
            var joiner = new ConfigDb(new DbContextOptionsBuilder<ConfigDb>().UseNpgsql(owner.Database.GetDbConnection())
                .AddInterceptors(Capture).Options);
            joiner.Database.UseTransaction(owner.Database.CurrentTransaction!.GetDbTransaction());
            joiner.Rows.Add(new ConfigRow { Id = 900004, Value = 99 });
            context.Items["guard-joiner"] = joiner;
            return Results.Ok();
        });
        app.MapPost("/guard/audit", async (HttpContext context) =>
        {
            var db = (ConfigDb)context.Items["guard-db"]!;
            db.Audits.Add(new AuditRow { Id = 1, Text = "audit" });
            await db.SaveChangesAsync();
            return Results.Ok();
        });
        app.MapPost("/guard/after", () => Results.Ok());
        app.MapGet("/slow", async (HttpContext context) =>
        {
            Entered.TrySetResult();
            await Finish.Task;
            return Now(context, after: true);
        });
        app.StartAsync().GetAwaiter().GetResult();
        Client = app.GetTestClient();
    }

    public Task<long> GuardAuditCountAsync() => Rig.Log.ScalarAsync<long>("SELECT count(*) FROM public.guard_audit");

    public Log.ApplierRig Rig { get; }

    public CommitRefusal Refusal { get; }

    public ChangeLog Changes { get; }

    public ClusterCaptureInterceptor Capture { get; }

    public ChangeAck Ack { get; }

    public ChangeSkip Skips { get; }

    public RecordingPoints Points { get; } = new();

    public async Task EventAsync(string action, int count = 1)
    {
        using var bound = new CancellationTokenSource(TimeSpan.FromSeconds(30));
        while (Rig.Events.All.Count(e => e.Action == action) < count)
        {
            await Task.Delay(10, bound.Token);
        }
    }

    public async Task PointAsync(string point)
    {
        using var bound = new CancellationTokenSource(TimeSpan.FromSeconds(30));
        while (!Points.Reached.Contains(point))
        {
            await Task.Delay(10, bound.Token);
        }
    }

    public NpgsqlTransaction? Left { get; private set; }

    public (NpgsqlConnection Connection, ConfigDb First, ConfigDb Second, IDbContextTransaction Transaction)? LeftEf { get; private set; }

    public async Task FenceAsync()
    {
        Rig.Gate.RequestFence(new FenceRequest(FenceTrigger.ApplierStall));
        Assert.Equal(ReplicaPhase.Fencing, Rig.Gate.Phase);
        await new AppendRefusalParticipant(Refusal).RunAsync(new FenceContext(new FenceRequest(FenceTrigger.ApplierStall)), CancellationToken.None);
    }

    public HttpClient Client { get; }

    public IServiceProvider Services => app.Services;

    public TaskCompletionSource ResponseStarted { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);

    public TaskCompletionSource Entered { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);

    public TaskCompletionSource Finish { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);

    private async Task<IResult> Write(HttpContext context)
    {
        context.Response.OnStarting(() =>
        {
            ResponseStarted.TrySetResult();
            return Task.CompletedTask;
        });
        var query = context.Request.Query;
        var commits = query.TryGetValue("commits", out var c) ? int.Parse(c.ToString(), System.Globalization.CultureInfo.InvariantCulture) : 1;
        var pause = query.TryGetValue("pause", out var p) ? int.Parse(p.ToString(), System.Globalization.CultureInfo.InvariantCulture) : -1;
        for (var i = 0; i < commits; i++)
        {
            if (i == pause)
            {
                await PauseAsync();
            }

            try
            {
                var connection = await Rig.Log.OpenAsync();
                var transaction = await connection.BeginTransactionAsync();
                if (query.ContainsKey("leaveOpen"))
                {
                    Rig.Log.EfTransactions.Began(transaction);
                    await Changes.AppendAsync(transaction, [new ChangeEntry("a", "k")]);
                    Left = transaction;
                    continue;
                }

                await using (connection)
                await using (transaction)
                {
                    await Changes.AppendAsync(transaction, [new ChangeEntry("a", "k")]);
                    if (i == 0 && query.ContainsKey("pauseTx"))
                    {
                        await PauseAsync();
                    }

                    await transaction.CommitAsync();
                }
            }
            catch (Exception) when (query.ContainsKey("swallow"))
            {
            }

            if (i == 0 && query.ContainsKey("optout"))
            {
                Ack.AcknowledgeAtCommit();
            }
        }

        if (pause >= commits)
        {
            await PauseAsync();
        }

        return Results.Ok("written");
    }

    private async Task<IResult> EfWrite(HttpContext context)
    {
        var query = context.Request.Query;
        var connection = await Rig.Log.OpenAsync();
        var first = NewConfigDb(connection);
        var second = NewConfigDb(connection);
        var transaction = await first.Database.BeginTransactionAsync();
        if (query.ContainsKey("capture"))
        {
            (await first.Rows.SingleAsync(r => r.Id == 1)).Value += 1;
            await first.SaveChangesAsync();
        }
        else
        {
            await first.Database.ExecuteSqlRawAsync("UPDATE public.config SET value = value + 1 WHERE id = 2");
        }

        await second.Database.UseTransactionAsync(transaction.GetDbTransaction());
        if (query.ContainsKey("leaveOpen"))
        {
            LeftEf = (connection, first, second, transaction);
            return Results.Ok("left open");
        }

        await using (connection)
        await using (first)
        await using (second)
        await using (transaction)
        {
            if (query.ContainsKey("pause"))
            {
                await PauseAsync();
            }

            await transaction.CommitAsync();
        }

        return Results.Ok("written");
    }

    private ConfigDb NewConfigDb(NpgsqlConnection connection) =>
        new(new DbContextOptionsBuilder<ConfigDb>().UseNpgsql(connection).AddInterceptors(Capture).Options);

    private async Task PauseAsync()
    {
        Entered.TrySetResult();
        await Finish.Task;
    }

    private static Seen Now(HttpContext context, bool after = false)
    {
        var scope = context.RequestServices.GetRequiredService<IClusterScopeAccessor>().Current;
        var head = scope?.PinnedGeneration?.Head;
        return new Seen(scope?.Kind.ToString(), after ? null : head, after ? head : null);
    }

    public async ValueTask DisposeAsync()
    {
        Finish.TrySetResult();
        Client.Dispose();
        await app.DisposeAsync();
        if (Left is { } left)
        {
            await left.DisposeAsync();
        }

        if (LeftEf is { } leftEf)
        {
            await leftEf.Transaction.DisposeAsync();
            await leftEf.Second.DisposeAsync();
            await leftEf.First.DisposeAsync();
            await leftEf.Connection.DisposeAsync();
        }

        Capture.Dispose();

        Ack.Dispose();
        Refusal.Dispose();
        await skipData.DisposeAsync();
    }
}

internal sealed class RecordingPoints : IFaultPoints
{
    private readonly System.Collections.Concurrent.ConcurrentQueue<string> reached = new();

    public IReadOnlyList<string> Reached => [.. reached];

    public ValueTask ReachAsync(string point, CancellationToken cancellationToken = default)
    {
        reached.Enqueue(point);
        return ValueTask.CompletedTask;
    }
}
