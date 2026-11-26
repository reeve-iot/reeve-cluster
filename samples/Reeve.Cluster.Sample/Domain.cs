using Microsoft.EntityFrameworkCore;

namespace Reeve.Cluster.Sample;

public sealed class Flag
{
    public string Name { get; set; } = "";

    public bool Enabled { get; set; }
}

public sealed class Quota
{
    public string Tenant { get; set; } = "";

    public int DailyLimit { get; set; }
}

public sealed class Setting
{
    public string Name { get; set; } = "";

    public int Value { get; set; }
}

public sealed class SampleDb(DbContextOptions<SampleDb> options) : DbContext(options)
{
    public DbSet<Flag> Flags => Set<Flag>();

    public DbSet<Quota> Quotas => Set<Quota>();

    public DbSet<Setting> Settings => Set<Setting>();

    protected override void OnModelCreating(ModelBuilder modelBuilder)
    {
        modelBuilder.Entity<Flag>(e =>
        {
            e.ToTable("sample_flags");
            e.HasKey(x => x.Name);
            e.Property(x => x.Name).HasColumnName("name");
            e.Property(x => x.Enabled).HasColumnName("enabled");
        });
        modelBuilder.Entity<Quota>(e =>
        {
            e.ToTable("sample_quotas");
            e.HasKey(x => x.Tenant);
            e.Property(x => x.Tenant).HasColumnName("tenant");
            e.Property(x => x.DailyLimit).HasColumnName("daily_limit");
        });
        modelBuilder.Entity<Setting>(e =>
        {
            e.ToTable("sample_settings");
            e.HasKey(x => x.Name);
            e.Property(x => x.Name).HasColumnName("name");
            e.Property(x => x.Value).HasColumnName("value");
        });
    }
}
