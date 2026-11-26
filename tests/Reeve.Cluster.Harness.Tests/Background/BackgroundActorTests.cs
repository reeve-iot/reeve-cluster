using System.Diagnostics;
using System.Net;
using Harness.SelfTests;
using Harness.Slice;
using Microsoft.Extensions.DependencyInjection;
using Reeve.Cluster.Harness.Faults;
using Reeve.Cluster.Harness.Replicas;
using Reeve.Cluster.Harness.Stack;
using Reeve.Cluster.Membership;
using Xunit.Abstractions;

namespace Harness.Background;

[Collection(StackCollection.Name)]
public sealed class BackgroundActorTests(StackFixture stack, ITestOutputHelper output)
{
    private static readonly TimeSpan Limit = TimeSpan.FromSeconds(60);

    [Fact]
    public async Task I21_after_an_acknowledgement_no_component_on_either_replica_acts_on_an_earlier_write()
    {
        await using var rig = await SliceRig.CreateAsync(stack, output.WriteLine);
        var (r1, r2) = await rig.StartPairAsync(BackgroundApp.Spec());
        const int writes = 12;

        var acked = new long[writes + 1];
        _ = await PutAsync(rig, r1, 0);
        await WaitForActAsync(rig, [r1, r2], 0);

        for (var write = 1; write <= writes; write++)
        {
            acked[write] = await PutAsync(rig, write % 2 == 0 ? r1 : r2, write);
            if (write % 3 == 0)
            {
                await WaitForActAsync(rig, [r1, r2], write);
            }
        }

        await WaitForActAsync(rig, [r1, r2], writes);

        foreach (var replica in new[] { r1, r2 })
        {
            var acts = replica.Services.GetRequiredService<ActLog>().All;
            Assert.Contains(acts, a => a.Write == 0);
            for (var write = 1; write <= writes; write++)
            {
                var late = acts.Where(a => a.Timestamp > acked[write] && a.Write < write).ToList();
                Assert.True(late.Count == 0, $"{replica.Id}: {late.Count} actions on write {late.FirstOrDefault()?.Write} after write {write} was acknowledged.");
            }
        }
    }

    [Fact]
    public async Task I21_the_peers_component_acts_on_the_earlier_write_until_the_acknowledgement_and_not_after()
    {
        await using var rig = await SliceRig.CreateAsync(stack, output.WriteLine);
        var (r1, r2) = await rig.StartPairAsync(BackgroundApp.Spec());
        _ = await PutAsync(rig, r1, 0);
        await WaitForActAsync(rig, [r1, r2], 0);

        using var pass = r2.Faults.HoldNext(LogFaultPoints.PassBegin);
        var put = PutAsync(rig, r1, 1);
        await pass.WaitReachedAsync(timeout: Limit);
        var before = r2.Services.GetRequiredService<ActLog>().All.Count;
        await SliceRig.UntilAsync(() => Task.FromResult(r2.Services.GetRequiredService<ActLog>().All.Count > before), "an action of the peer's component while the write is unacknowledged");
        Assert.False(put.IsCompleted);

        pass.Release();
        var acknowledged = await put.WaitAsync(Limit);
        await WaitForActAsync(rig, [r1, r2], 1);
        foreach (var replica in new[] { r1, r2 })
        {
            Assert.DoesNotContain(replica.Services.GetRequiredService<ActLog>().All, a => a.Timestamp > acknowledged && a.Write < 1);
        }
    }

    private static async Task<long> PutAsync(SliceRig rig, Replica replica, int write)
    {
        using var response = await rig.Client(replica).PutAsync($"items/widget/a?value=v{write}", null).WaitAsync(Limit);
        Assert.Equal(HttpStatusCode.OK, response.StatusCode);
        return long.Parse(response.Headers.GetValues(BackgroundApp.AnsweredAtHeader).Single(), System.Globalization.CultureInfo.InvariantCulture);
    }

    private static Task WaitForActAsync(SliceRig rig, Replica[] replicas, int write) =>
        SliceRig.UntilAsync(
            () => Task.FromResult(replicas.All(r => r.Services.GetRequiredService<ActLog>().All.Any(a => a.Write == write))),
            $"an action on write {write} on every replica");
}
