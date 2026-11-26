using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.DependencyInjection;
using Npgsql;
using Reeve.Cluster.EntityFrameworkCore;
using Reeve.Cluster.Harness.Replicas;

namespace Reeve.Cluster.Harness.Stimuli;

internal sealed class HarnessCaptured
{
    public int Id { get; set; }

    public string Label { get; set; } = "";
}

internal sealed class HarnessCaptureDb(DbContextOptions<HarnessCaptureDb> options) : DbContext(options)
{
    protected override void OnModelCreating(ModelBuilder modelBuilder) =>
        modelBuilder.Entity<HarnessCaptured>(e =>
        {
            e.ToTable("harness_captured");
            e.Property(x => x.Id).HasColumnName("id").ValueGeneratedNever();
            e.Property(x => x.Label).HasColumnName("label");
        });
}

internal static class HarnessCapture
{
    public static void Register(IServiceCollection services) =>
        services.AddReeveClusterEntityFrameworkCore(b => b.Entity<HarnessCaptured>("widget").NoKey().AllColumns());

    public static HarnessCaptureDb Context(Replica replica, NpgsqlConnection connection)
    {
        var interceptor = replica.Services.GetService<ClusterCaptureInterceptor>()
            ?? throw new InvalidOperationException($"Replica '{replica.Id}' has no EF Core capture: register it with HarnessCapture.Register.");
        return new HarnessCaptureDb(new DbContextOptionsBuilder<HarnessCaptureDb>().UseNpgsql(connection).AddInterceptors(interceptor).Options);
    }
}
