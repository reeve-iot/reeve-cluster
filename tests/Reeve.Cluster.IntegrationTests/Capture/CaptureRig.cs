using System.Data.Common;
using Int.Log;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Diagnostics;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.DependencyInjection;
using Npgsql;
using Reeve.Cluster;
using Reeve.Cluster.EntityFrameworkCore;
using Reeve.Cluster.Harness.Containers;
using Reeve.Cluster.Log;
using Reeve.Cluster.Wakeup;

namespace Int.Capture;

public sealed class Setting
{
    public int Id { get; set; }

    public string Name { get; set; } = "";

    public string Value { get; set; } = "";

    public string? Note { get; set; }
}

public sealed class Widget
{
    public int Id { get; set; }

    public string Label { get; set; } = "";
}

public sealed class Audit
{
    public int Id { get; set; }

    public string Text { get; set; } = "";
}

public sealed class CaptureDb(DbContextOptions<CaptureDb> options) : DbContext(options)
{
    public DbSet<Setting> Settings => Set<Setting>();

    public DbSet<Widget> Widgets => Set<Widget>();

    public DbSet<Audit> Audits => Set<Audit>();

    protected override void OnModelCreating(ModelBuilder modelBuilder)
    {
        modelBuilder.Entity<Setting>(e =>
        {
            e.ToTable("setting");
            e.Property(x => x.Id).HasColumnName("id").ValueGeneratedNever();
            e.Property(x => x.Name).HasColumnName("name");
            e.Property(x => x.Value).HasColumnName("value");
            e.Property(x => x.Note).HasColumnName("note");
        });
        modelBuilder.Entity<Widget>(e =>
        {
            e.ToTable("widget");
            e.Property(x => x.Id).HasColumnName("id").ValueGeneratedNever();
            e.Property(x => x.Label).HasColumnName("label");
        });
        modelBuilder.Entity<Audit>(e =>
        {
            e.ToTable("audit");
            e.Property(x => x.Id).HasColumnName("id").ValueGeneratedNever();
            e.Property(x => x.Text).HasColumnName("text");
        });
    }
}

internal sealed class CaptureRig : IAsyncDisposable
{
    private readonly List<Wakeup> wakeups = [];
    private readonly IDisposable subscription;

    private CaptureRig(AckRig ack, IClusterMapping mapping)
    {
        Ack = ack;
        Mapping = mapping;
        Logger = new RecordingLogger();
        subscription = ack.Channel.Subscribe(WakeupKind.Appended, wakeup => { lock (wakeups) { wakeups.Add(wakeup); } });
        Interceptor = new ClusterCaptureInterceptor(
            mapping, Log.Scopes, Log.Log, Log.Refusal, Log.EfTransactions, new PostCommitWakeup(ack.Channel, ack.Skip.Faults, Log.Events),
            ack.Skip.Faults, Log.Events, Logger);
    }

    public AckRig Ack { get; }

    public IClusterMapping Mapping { get; }

    public LogRig Log => Ack.Log;

    public ClusterCaptureInterceptor Interceptor { get; }

    public RecordingLogger Logger { get; }

    public IReadOnlyList<Wakeup> Wakeups
    {
        get
        {
            lock (wakeups)
            {
                return [.. wakeups];
            }
        }
    }

    public static IClusterMapping DefaultMapping() => ClusterMapping.Build(b =>
    {
        b.Entity<Setting>("setting").Key(s => s.Name).Columns(s => s.Name, s => s.Value);
        b.Entity<Widget>("widget").NoKey().AllColumns();
    });

    public static async Task<CaptureRig> CreateAsync(PostgresFixture postgres, IClusterMapping? mapping = null)
    {
        var ack = await AckRig.CreateAsync(postgres);
        await using (var connection = await ack.Log.OpenAsync())
        {
            await using var command = new NpgsqlCommand(
                """
                CREATE TABLE public.setting (id int PRIMARY KEY, name text NOT NULL, value text NOT NULL, note text NULL);
                CREATE TABLE public.widget (id int PRIMARY KEY, label text NOT NULL UNIQUE);
                CREATE TABLE public.audit (id int PRIMARY KEY, text text NOT NULL);
                """, connection);
            await command.ExecuteNonQueryAsync();
        }

        return new CaptureRig(ack, mapping ?? DefaultMapping());
    }

    public CaptureDb NewContext(DbConnection? connection = null)
    {
        var builder = new DbContextOptionsBuilder<CaptureDb>();
        _ = connection is null ? builder.UseNpgsql(Log.ConnectionString) : builder.UseNpgsql(connection);
        builder.AddClusterCapture(Interceptor, Mapping);
        return new CaptureDb(builder.Options);
    }

    public async Task<NpgsqlConnection> OpenAsync() => await Log.OpenAsync();

    public async Task<List<(string Type, string? Key)>> EntriesAsync()
    {
        await using var connection = await Log.OpenAsync();
        await using var command = new NpgsqlCommand("SELECT aggregate_type, aggregate_key FROM reeve_cluster.change_log ORDER BY seq", connection);
        await using var reader = await command.ExecuteReaderAsync();
        var rows = new List<(string, string?)>();
        while (await reader.ReadAsync())
        {
            rows.Add((reader.GetString(0), reader.IsDBNull(1) ? null : reader.GetString(1)));
        }

        return rows;
    }

    public Task<long> CountAsync(string table) => Log.ScalarAsync<long>($"SELECT count(*) FROM {table}");

    public async ValueTask DisposeAsync()
    {
        subscription.Dispose();
        Interceptor.Dispose();
        await Ack.DisposeAsync();
    }
}

internal static class NoContextEvent
{
    public static TransactionEventData Of(DbTransaction transaction) =>
        new(Definition.Instance, (_, _) => string.Empty, transaction, null, Guid.NewGuid(), Guid.NewGuid(), true, DateTimeOffset.UtcNow);

    public static TransactionEndEventData Ended(DbTransaction transaction) =>
        new(Definition.Instance, (_, _) => string.Empty, transaction, null, Guid.NewGuid(), Guid.NewGuid(), true, DateTimeOffset.UtcNow, TimeSpan.Zero);

#pragma warning disable EF1001 // LoggingOptions is EF Core's internal API; the event definition only needs some options.
    private sealed class Definition() : EventDefinitionBase(new Microsoft.EntityFrameworkCore.Diagnostics.Internal.LoggingOptions(), RelationalEventId.TransactionCommitting, LogLevel.Debug, "test")
#pragma warning restore EF1001
    {
        public static readonly Definition Instance = new();
    }
}

internal sealed class RecordingLogger : ILogger<ClusterCaptureInterceptor>
{
    private readonly List<(LogLevel Level, string Message)> lines = [];

    public IReadOnlyList<(LogLevel Level, string Message)> Lines
    {
        get
        {
            lock (lines)
            {
                return [.. lines];
            }
        }
    }

    public IReadOnlyList<string> Errors => [.. Lines.Where(l => l.Level >= LogLevel.Error).Select(l => l.Message)];

    public IDisposable? BeginScope<TState>(TState state)
        where TState : notnull => null;

    public bool IsEnabled(LogLevel logLevel) => true;

    public void Log<TState>(LogLevel logLevel, EventId eventId, TState state, Exception? exception, Func<TState, Exception?, string> formatter)
    {
        lock (lines)
        {
            lines.Add((logLevel, formatter(state, exception)));
        }
    }
}
