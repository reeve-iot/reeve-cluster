using Microsoft.EntityFrameworkCore;
using Reeve.Cluster;
using Reeve.Cluster.Harness.Containers;

namespace Int.Capture;

public sealed class RequestScopeGuardTests(PostgresFixture postgres) : IClassFixture<PostgresFixture>
{
    [Fact]
    public async Task Request_marker_without_scope_refuses_mapped_save_without_appending()
    {
        await using var rig = await CaptureRig.CreateAsync(postgres);
        using (HttpRequestFlow.Enter())
        await using (var db = rig.NewContext())
        {
            db.Settings.Add(new Setting { Id = 1, Name = "n", Value = "v" });
            var refusal = await Assert.ThrowsAsync<ClusterConfigurationException>(() => db.SaveChangesAsync());
            Assert.Equal(ClusterConfigurationReason.WriteOutsideClusterScope, refusal.Reason);
        }
        Assert.Empty(await rig.EntriesAsync());
    }

    [Fact]
    public async Task Request_marker_without_scope_allows_unmapped_save()
    {
        await using var rig = await CaptureRig.CreateAsync(postgres);
        using (HttpRequestFlow.Enter())
        await using (var db = rig.NewContext())
        {
            db.Audits.Add(new Audit { Id = 1, Text = "audit" });
            await db.SaveChangesAsync();
        }
        Assert.Equal(1, await rig.CountAsync("public.audit"));
    }

    [Fact]
    public async Task Request_marker_inside_cluster_scope_allows_mapped_save()
    {
        await using var rig = await CaptureRig.CreateAsync(postgres);
        using (HttpRequestFlow.Enter())
        using (rig.Log.Scopes.Begin(ClusterScopeKind.ClientRequest))
        await using (var db = rig.NewContext())
        {
            db.Settings.Add(new Setting { Id = 2, Name = "scoped", Value = "v" });
            await db.SaveChangesAsync();
        }
        Assert.Single(await rig.EntriesAsync());
    }

    [Fact]
    public async Task Disposed_request_marker_leaves_scope_less_mapped_save_unchanged()
    {
        await using var rig = await CaptureRig.CreateAsync(postgres);
        using (HttpRequestFlow.Enter()) { }
        await using var db = rig.NewContext();
        db.Settings.Add(new Setting { Id = 3, Name = "outside", Value = "v" });
        await db.SaveChangesAsync();
        Assert.Single(await rig.EntriesAsync());
    }
}
