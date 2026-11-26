using Microsoft.EntityFrameworkCore;
using Reeve.Cluster;
using Reeve.Cluster.EntityFrameworkCore;
using Reeve.Cluster.Harness.Containers;

namespace Int.Capture;

public sealed class GapJsonOwner
{
    public int Id { get; set; }
    public GapJsonDetails Details { get; set; } = new();
}

public sealed class GapJsonDetails
{
    public string Value { get; set; } = "";
}

public sealed class GapLeft
{
    public int Id { get; set; }
    public List<GapRight> Rights { get; set; } = [];
}

public sealed class GapRight
{
    public int Id { get; set; }
    public List<GapLeft> Lefts { get; set; } = [];
}

public sealed class GapParent
{
    public int Id { get; set; }
    public List<GapChild> Children { get; set; } = [];
}

public sealed class GapChild
{
    public int Id { get; set; }
    public int ParentId { get; set; }
}

public sealed class CaptureGapsDb(DbContextOptions<CaptureGapsDb> options) : DbContext(options)
{
    public DbSet<GapJsonOwner> JsonOwners => Set<GapJsonOwner>();
    public DbSet<GapLeft> Lefts => Set<GapLeft>();
    public DbSet<GapRight> Rights => Set<GapRight>();
    public DbSet<GapParent> Parents => Set<GapParent>();

    protected override void OnModelCreating(ModelBuilder model)
    {
        model.Entity<GapJsonOwner>(e =>
        {
            e.ToTable("gap_json_owner");
            e.Property(x => x.Id).HasColumnName("id").ValueGeneratedNever();
            e.OwnsOne(x => x.Details, d => d.ToJson("details"));
        });
        model.Entity<GapLeft>(e =>
        {
            e.ToTable("gap_left");
            e.Property(x => x.Id).HasColumnName("id").ValueGeneratedNever();
            e.HasMany(x => x.Rights).WithMany(x => x.Lefts).UsingEntity<Dictionary<string, object>>(
                "gap_left_right",
                right => right.HasOne<GapRight>().WithMany().HasForeignKey("RightsId"),
                left => left.HasOne<GapLeft>().WithMany().HasForeignKey("LeftsId"));
        });
        model.Entity<GapRight>(e =>
        {
            e.ToTable("gap_right");
            e.Property(x => x.Id).HasColumnName("id").ValueGeneratedNever();
        });
        model.Entity<GapParent>(e =>
        {
            e.ToTable("gap_parent");
            e.Property(x => x.Id).HasColumnName("id").ValueGeneratedNever();
            e.HasMany(x => x.Children).WithOne().HasForeignKey(x => x.ParentId).OnDelete(DeleteBehavior.Cascade);
        });
        model.Entity<GapChild>(e =>
        {
            e.ToTable("gap_child");
            e.Property(x => x.Id).HasColumnName("id").ValueGeneratedNever();
            e.Property(x => x.ParentId).HasColumnName("parent_id");
        });
    }
}

public sealed class CaptureGapsTests : IClassFixture<PostgresFixture>
{
    private readonly PostgresFixture postgres;

    public CaptureGapsTests(PostgresFixture postgres) => this.postgres = postgres;

    [Fact]
    public async Task A_changed_JSON_owned_property_is_logged_with_its_owner_key()
    {
        var mapping = ClusterMapping.Build(b => b.Entity<GapJsonOwner>("gap-json-owner").Key(x => x.Id.ToString()).AllColumns());
        await using var rig = await CreateRig(mapping);
        await using (var seed = NewDb(rig))
        {
            seed.JsonOwners.Add(new GapJsonOwner { Id = 1, Details = new GapJsonDetails { Value = "before" } });
            await seed.SaveChangesAsync();
        }
        var before = (await rig.EntriesAsync()).Count;

        await using (var db = NewDb(rig))
        {
            (await db.JsonOwners.SingleAsync()).Details.Value = "after";
            await db.SaveChangesAsync();
        }

        Assert.Contains(("gap-json-owner", (string?)"1"), (await rig.EntriesAsync()).Skip(before));
    }

    [Fact]
    public async Task A_many_to_many_link_between_existing_rows_logs_both_aggregate_keys()
    {
        var mapping = ClusterMapping.Build(b =>
        {
            b.Entity<GapLeft>("gap-left").Key(x => x.Id.ToString()).AllColumns();
            b.Entity<GapRight>("gap-right").Key(x => x.Id.ToString()).AllColumns();
        });
        await using var rig = await CreateRig(mapping);
        await using (var seed = NewDb(rig))
        {
            seed.Lefts.Add(new GapLeft { Id = 1 });
            seed.Rights.Add(new GapRight { Id = 2 });
            await seed.SaveChangesAsync();
        }
        var before = (await rig.EntriesAsync()).Count;

        await using (var db = NewDb(rig))
        {
            var left = await db.Lefts.SingleAsync();
            var right = await db.Rights.SingleAsync();
            left.Rights.Add(right);
            await db.SaveChangesAsync();
        }

        var added = (await rig.EntriesAsync()).Skip(before).ToList();
        Assert.Contains(("gap-left", (string?)"1"), added);
        Assert.Contains(("gap-right", (string?)"2"), added);
    }

    [Fact]
    public async Task Deleting_a_parent_without_loading_cascade_children_logs_keyless_child_entry()
    {
        var mapping = ClusterMapping.Build(b =>
        {
            b.Entity<GapParent>("gap-parent").Key(x => x.Id.ToString()).AllColumns();
            b.Entity<GapChild>("gap-child").NoKey().AllColumns();
        });
        await using var rig = await CreateRig(mapping);
        await using (var seed = NewDb(rig))
        {
            seed.Parents.Add(new GapParent { Id = 1, Children = [new GapChild { Id = 2 }] });
            await seed.SaveChangesAsync();
        }
        var before = (await rig.EntriesAsync()).Count;

        await using (var db = NewDb(rig))
        {
            db.Parents.Remove(new GapParent { Id = 1 });
            await db.SaveChangesAsync();
        }

        Assert.Equal(0, await rig.CountAsync("public.gap_child"));
        Assert.Contains(("gap-child", (string?)null), (await rig.EntriesAsync()).Skip(before));
    }

    private async Task<CaptureRig> CreateRig(IClusterMapping mapping)
    {
        var rig = await CaptureRig.CreateAsync(postgres, mapping);
        await using var connection = await rig.OpenAsync();
        await using var command = new Npgsql.NpgsqlCommand(
            """
            CREATE TABLE public.gap_json_owner (id int PRIMARY KEY, details jsonb NOT NULL);
            CREATE TABLE public.gap_left (id int PRIMARY KEY);
            CREATE TABLE public.gap_right (id int PRIMARY KEY);
            CREATE TABLE public.gap_left_right ("LeftsId" int NOT NULL REFERENCES public.gap_left(id), "RightsId" int NOT NULL REFERENCES public.gap_right(id), PRIMARY KEY ("LeftsId", "RightsId"));
            CREATE TABLE public.gap_parent (id int PRIMARY KEY);
            CREATE TABLE public.gap_child (id int PRIMARY KEY, parent_id int NOT NULL REFERENCES public.gap_parent(id) ON DELETE CASCADE);
            """, connection);
        await command.ExecuteNonQueryAsync();
        return rig;
    }

    private static CaptureGapsDb NewDb(CaptureRig rig) => new(
        new DbContextOptionsBuilder<CaptureGapsDb>()
            .UseNpgsql(rig.Log.ConnectionString)
            .AddInterceptors(rig.Interceptor)
            .Options);
}
