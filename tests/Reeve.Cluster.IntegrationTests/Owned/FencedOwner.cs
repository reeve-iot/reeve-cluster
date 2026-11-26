using Microsoft.EntityFrameworkCore;
using Npgsql;
using Reeve.Cluster;
using Reeve.Cluster.Owned;

namespace Int.Owned;

internal sealed class FencedOwner(string kind, string key, long token, long timeline, TimeSpan? timeout = null) : OwnedContext, IFencedOwner, IFaultPoints
{
    private readonly List<string> reached = [];

    public override string Kind => kind;

    public override string Key => key;

    public override long Token => token;

    public override IGeneration Configuration => throw new NotSupportedException();

    public override Task EndAsync(CancellationToken cancellationToken = default) => throw new NotSupportedException();

    public Exception? Refusal { get; set; }

    public int Starts { get; private set; }

    public int OtherTimelines { get; private set; }

    public IReadOnlyList<string> Reached
    {
        get
        {
            lock (reached)
            {
                return [.. reached];
            }
        }
    }

    public IFaultPoints FaultPoints => this;

    public override void EnsureOwner()
    {
        if (Refusal is { } refusal)
        {
            throw refusal;
        }
    }

    public FencedStart Start()
    {
        EnsureOwner();
        Starts++;
        return new FencedStart(kind, key, token, timeline, (int)(timeout ?? TimeSpan.FromSeconds(30)).TotalMilliseconds);
    }

    public void OnOtherTimeline() => OtherTimelines++;

    public ValueTask ReachAsync(string point, CancellationToken cancellationToken = default)
    {
        lock (reached)
        {
            reached.Add(point);
        }

        return ValueTask.CompletedTask;
    }
}

public sealed class Effect
{
    public long Id { get; set; }

    public long Tenure { get; set; }
}

public sealed class EffectDb(DbContextOptions<EffectDb> options) : DbContext(options)
{
    public DbSet<Effect> Effects => Set<Effect>();

    public static EffectDb Over(string connectionString) =>
        new(new DbContextOptionsBuilder<EffectDb>().UseNpgsql(connectionString).Options);

    public static async Task CreateTableAsync(NpgsqlConnection connection)
    {
        await using var command = new NpgsqlCommand("CREATE TABLE public.effect (id bigserial PRIMARY KEY, tenure bigint NOT NULL)", connection);
        await command.ExecuteNonQueryAsync();
    }

    protected override void OnModelCreating(ModelBuilder modelBuilder) =>
        modelBuilder.Entity<Effect>(e =>
        {
            e.ToTable("effect");
            e.Property(x => x.Id).HasColumnName("id");
            e.Property(x => x.Tenure).HasColumnName("tenure");
        });
}
