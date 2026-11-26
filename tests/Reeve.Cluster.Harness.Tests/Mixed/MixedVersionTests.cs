using System.Net;
using System.Text.Json;
using Harness.SelfTests;
using Reeve.Cluster.Harness.Stack;
using Reeve.Cluster.Membership;
using Xunit.Abstractions;

namespace Harness.Mixed;

[Collection(StackCollection.Name)]
[Trait("Category", "MixedVersion")]
public sealed class MixedVersionTests(StackFixture stack, ITestOutputHelper output)
{
    [MixedVersionFact]
    public async Task A_write_acknowledged_by_one_version_is_applied_by_the_other()
    {
        await using var rig = await MixedRig.CreateAsync(stack, output.WriteLine, "o1", "n1");
        try
        {
            var o1 = await rig.StartAsync("o1");
            var n1 = await rig.StartAsync("n1");

            foreach (var (writer, reader) in new[] { (o1, n1), (n1, o1) })
            {
                await using var lag = await rig.LagApplyAsync(reader);
                var name = $"from-{writer.Id}";
                Assert.Equal(HttpStatusCode.OK, await writer.PutFlagAsync(name, true));
                Assert.True(await reader.AppliedFlagAsync(name), $"{reader.Id} had not applied the flag {writer.Id} acknowledged");
                await AssertCursorAtHeadAsync(rig, writer, reader);

                var first = writer.Id == "o1" ? 1 : 100;
                for (var value = first; value < first + 5; value++)
                {
                    using var response = await writer.Process.Client.PostAsync("config", MixedNode.Json($$$"""{"quotas":{"acme":{{{value}}}},"settings":{"mode":{{{value}}}}}"""));
                    Assert.Equal(HttpStatusCode.OK, response.StatusCode);
                    Assert.Equal(((int?)value, (int?)value), await reader.AppliedConfigAsync("acme", "mode"));
                    await AssertCursorAtHeadAsync(rig, writer, reader);
                }
            }
        }
        catch
        {
            output.WriteLine(await rig.Cluster.ReportAsync());
            throw;
        }
    }

    [MixedVersionFact]
    public async Task Owned_units_have_one_owner_while_placement_crosses_versions()
    {
        await using var rig = await MixedRig.CreateAsync(stack, output.WriteLine, "o1", "n1");
        try
        {
            var o1 = await rig.StartAsync("o1");
            var alone = await SettleAsync(rig, [o1], "o1 running both units alone", f => AllOn(f.Owners, "o1"));
            await PlacerIsAsync(rig, "o1");
            Assert.Equal("Created", await o1.CreateKeyAsync("k-old"));

            var n1 = await rig.StartAsync("n1");
            Assert.Equal("Created", await n1.CreateKeyAsync("k-new"));

            var onNew = await SettleAsync(rig, [o1, n1], "the old placer balancing the placed load with a sample unit on n1", f => Balanced(f, "n1"));
            await PlacerIsAsync(rig, "o1");
            AssertMovedTo(alone, onNew, "n1");

            var stopped = MixedRig.StopAsync(o1);
            var afterDrain = await SettleAsync(rig, [o1, n1], "every unit on n1 once o1 has drained", f => AllOn(f.Owners, "n1"));
            await stopped;
            await PlacerIsAsync(rig, "n1");
            AssertMovedTo(onNew, afterDrain, "n1");
            await MixedRig.UntilAsync(async () => (await n1.TenuresAsync()).ContainsKey("mixed-keyed/k-old"), "n1 adopting the key the old build created");

            o1 = await rig.StartAsync("o1");
            var onOld = await SettleAsync(rig, [o1, n1], "the new placer balancing the placed load with a sample unit on o1", f => Balanced(f, "o1"));
            await PlacerIsAsync(rig, "n1");
            AssertMovedTo(afterDrain, onOld, "o1");

            stopped = MixedRig.StopAsync(n1);
            var finished = await SettleAsync(rig, [o1, n1], "every unit on o1 once n1 has drained", f => AllOn(f.Owners, "o1"));
            await stopped;
            await PlacerIsAsync(rig, "o1");
            AssertMovedTo(onOld, finished, "o1");
            await MixedRig.UntilAsync(async () => (await o1.TenuresAsync()).Keys.Intersect(["mixed-keyed/k-old", "mixed-keyed/k-new"]).Count() == 2, "o1 adopting the keys both builds created");
        }
        catch
        {
            output.WriteLine(await rig.Cluster.ReportAsync());
            throw;
        }
    }

    [MixedVersionFact]
    public async Task Ask_is_answered_across_versions_in_both_directions()
    {
        await using var rig = await MixedRig.CreateAsync(stack, output.WriteLine, "o1", "n1");
        try
        {
            var o1 = await rig.StartAsync("o1");
            var n1 = await rig.StartAsync("n1");
            var (oldVersion, newVersion) = await MixedRig.VersionsAsync(o1, n1);

            Assert.Equal($"n1|{newVersion}|up", Answer(await o1.AskMemberAsync("n1", "up")));
            Assert.Equal($"o1|{oldVersion}|down", Answer(await n1.AskMemberAsync("o1", "down")));

            Assert.Equal("Created", await o1.CreateKeyAsync("owned-by-old"));
            Assert.Equal("Created", await n1.CreateKeyAsync("owned-by-new"));
            await MixedRig.UntilAsync(async () => (await o1.TenuresAsync()).ContainsKey("mixed-keyed/owned-by-old") && (await n1.TenuresAsync()).ContainsKey("mixed-keyed/owned-by-new"), "each build running the key it created");
            Assert.Equal($"n1|{newVersion}|to-key", Answer(await o1.AskKeyAsync("owned-by-new", "to-key")));
            Assert.Equal($"o1|{oldVersion}|to-key", Answer(await n1.AskKeyAsync("owned-by-old", "to-key")));
        }
        catch
        {
            output.WriteLine(await rig.Cluster.ReportAsync());
            throw;
        }
    }

    [MixedVersionFact]
    public async Task Claims_and_slots_exclude_across_versions()
    {
        await using var rig = await MixedRig.CreateAsync(stack, output.WriteLine, "o1", "n1");
        try
        {
            var o1 = await rig.StartAsync("o1");
            var n1 = await rig.StartAsync("n1");

            foreach (var store in new[] { "pg", "redis" })
            {
                var won = await o1.ClaimAsync(store, "job");
                Assert.Equal("won", won.GetProperty("result").GetString());
                var lost = await n1.ClaimAsync(store, "job");
                Assert.Equal(("lost", "o1"), (lost.GetProperty("result").GetString(), lost.GetProperty("holder").GetString()));
                Assert.True(await o1.ReleaseClaimAsync(store, "job", won.GetProperty("token").GetGuid()));

                won = await n1.ClaimAsync(store, "job");
                Assert.Equal("won", won.GetProperty("result").GetString());
                lost = await o1.ClaimAsync(store, "job");
                Assert.Equal(("lost", "n1"), (lost.GetProperty("result").GetString(), lost.GetProperty("holder").GetString()));
                Assert.True(await n1.ReleaseClaimAsync(store, "job", won.GetProperty("token").GetGuid()));

                var first = await o1.AcquireSlotAsync(store, "pool", "a", 2);
                var second = await n1.AcquireSlotAsync(store, "pool", "b", 2);
                Assert.Equal(("acquired", "acquired"), (first.GetProperty("result").GetString(), second.GetProperty("result").GetString()));
                Assert.Equal("full", (await o1.AcquireSlotAsync(store, "pool", "c", 2)).GetProperty("result").GetString());
                Assert.Equal("full", (await n1.AcquireSlotAsync(store, "pool", "c", 2)).GetProperty("result").GetString());
                Assert.True(await n1.ReleaseSlotAsync(store, "pool", "b", second.GetProperty("token").GetGuid()));
                Assert.Equal("acquired", (await o1.AcquireSlotAsync(store, "pool", "c", 2)).GetProperty("result").GetString());
            }
        }
        catch
        {
            output.WriteLine(await rig.Cluster.ReportAsync());
            throw;
        }
    }

    [MixedVersionFact]
    public async Task The_old_build_runs_on_the_schema_the_new_build_migrated()
    {
        await using var rig = await MixedRig.CreateAsync(stack, output.WriteLine, "o1", "n1");
        try
        {
            await rig.SeedSchemaWithOldBuildAsync();
            var before = await rig.SchemaDigestAsync();
            var n1 = await rig.StartAsync("n1");
            var after = await rig.SchemaDigestAsync();
            if (MixedRig.SchemaDeltaDeclaredNone)
            {
                Assert.True(before == after, $"{MixedRig.SchemaDeltaVariable}=none, yet the new build changed the schema the old build created.");
            }
            else
            {
                Assert.True(before != after, $"The new build migrated nothing on top of the old build's schema, so the old build below only restarts. Set {MixedRig.SchemaDeltaVariable}=none for a pair whose migrations are the same.");
            }

            var o1 = await rig.StartAsync("o1");

            Assert.Equal(MemberRowState.Active, await rig.StateOfAsync("o1"));
            Assert.Equal(HttpStatusCode.OK, await o1.PutFlagAsync("from-old", true));
            Assert.True(await n1.AppliedFlagAsync("from-old"));
            Assert.Equal(HttpStatusCode.OK, await n1.PutFlagAsync("from-new", true));
            Assert.True(await o1.AppliedFlagAsync("from-new"));
        }
        catch
        {
            output.WriteLine(await rig.Cluster.ReportAsync());
            throw;
        }
    }

    [MixedVersionFact]
    public async Task The_version_floor_is_refused_beside_the_old_build_and_raised_once_it_leaves()
    {
        await using var rig = await MixedRig.CreateAsync(stack, output.WriteLine, "o1", "n1");
        try
        {
            var o1 = await rig.StartAsync("o1");
            var n1 = await rig.StartAsync("n1");
            var (_, newVersion) = await MixedRig.VersionsAsync(o1, n1);
            var before = await rig.VersionFloorAsync();

            Assert.False(await n1.RequireVersionAsync(newVersion));
            Assert.False(await n1.AllMembersAtLeastAsync(newVersion));
            Assert.Equal(before, await rig.VersionFloorAsync());

            await MixedRig.StopAsync(o1);
            Assert.True(await n1.RequireVersionAsync(newVersion));
            Assert.Equal(newVersion, await rig.VersionFloorAsync());

            var again = await rig.StartWithoutWaitingAsync("o1");
            await rig.AwaitRefusedAsync(again, "activation.version-floor");
            Assert.Equal(MemberRowState.Joining, await rig.StateOfAsync("o1"));
            Assert.NotEqual(HttpStatusCode.OK, await ReadyStatusAsync(again));
        }
        catch
        {
            output.WriteLine(await rig.Cluster.ReportAsync());
            throw;
        }
    }

    [MixedVersionFact]
    public async Task A_full_stop_rollback_lowers_the_floor_and_the_old_build_serves_again()
    {
        await using var rig = await MixedRig.CreateAsync(stack, output.WriteLine, "o1", "n1");
        try
        {
            await rig.SeedSchemaWithOldBuildAsync();
            var n1 = await rig.StartAsync("n1");
            var newVersion = await n1.VersionAsync();
            Assert.True(await n1.RequireVersionAsync(newVersion));
            Assert.Equal(newVersion, await rig.VersionFloorAsync());

            var refused = await rig.StartWithoutWaitingAsync("o1");
            await rig.AwaitRefusedAsync(refused, "activation.version-floor");
            var oldVersion = await refused.VersionAsync();
            Assert.NotEqual(HttpStatusCode.OK, await ReadyStatusAsync(refused));

            await MixedRig.StopAsync(refused);
            await MixedRig.StopAsync(n1);
            Assert.Equal("Lowered", await rig.LowerVersionFloorAsync(oldVersion));

            var o1 = await rig.StartAsync("o1");
            Assert.Equal(MemberRowState.Active, await rig.StateOfAsync("o1"));
            Assert.Equal(HttpStatusCode.OK, await o1.PutFlagAsync("after-rollback", true));
            Assert.Equal(oldVersion, await rig.VersionFloorAsync());
        }
        catch
        {
            output.WriteLine(await rig.Cluster.ReportAsync());
            throw;
        }
    }

    private static readonly string[] Placed = ["mixed-single", "mixed-single-2", "mixed-single-3", "mixed-single-4", "mixed-single-5", "mixed-single-6"];

    private sealed record Unit(string Owner, long Token);

    private sealed record Fleet(IReadOnlyDictionary<string, Unit> Owners, IReadOnlyDictionary<string, IReadOnlySet<string>> Running, IReadOnlyDictionary<string, int> Loads);

    private static bool AllOn(IReadOnlyDictionary<string, Unit> owners, string replica) =>
        owners.Count == Placed.Length && owners.Values.All(u => u.Owner == replica);

    private static bool Balanced(Fleet fleet, string joiner)
    {
        var loads = fleet.Running.Keys.Select(id => fleet.Loads.GetValueOrDefault(id)).ToList();
        return fleet.Owners.Count == Placed.Length && loads.Max() - loads.Min() <= 1 && fleet.Owners.Values.Any(u => u.Owner == joiner);
    }

    private static void AssertMovedTo(Fleet before, Fleet after, string joiner)
    {
        Assert.True(Placed.Any(kind => before.Owners[kind].Owner != after.Owners[kind].Owner), $"No sample unit changed owner (before {Describe(before)}; after {Describe(after)}): the placer moved only library units, so register more sample units than the library places.");
        AssertTokensGrew(before, after);
        foreach (var kind in Placed.Where(kind => before.Owners[kind].Owner != after.Owners[kind].Owner))
        {
            Assert.True(after.Owners[kind].Owner == joiner, $"{kind} moved to {after.Owners[kind].Owner}, not to the member that joined ({joiner})");
        }
    }

    private static void AssertTokensGrew(Fleet before, Fleet after)
    {
        foreach (var kind in Placed.Where(kind => before.Owners[kind].Owner != after.Owners[kind].Owner))
        {
            Assert.True(after.Owners[kind].Token > before.Owners[kind].Token, $"{kind} moved from {before.Owners[kind].Owner} to {after.Owners[kind].Owner} without a larger token");
        }
    }

    private static string Describe(Fleet fleet) => string.Join(", ", fleet.Owners.Select(o => $"{o.Key}={o.Value.Owner}@{o.Value.Token}"));

    private static async Task PlacerIsAsync(MixedRig rig, string replica) =>
        await MixedRig.UntilAsync(async () => (await rig.OwnerAsync("placer", "placer"))?.Owner == replica, $"{replica} holding the placer");

    private static async Task<Fleet> ReadFleetAsync(MixedRig rig, MixedNode[] nodes)
    {
        var running = new Dictionary<string, IReadOnlySet<string>>();
        foreach (var node in nodes)
        {
            running[node.Id] = (await node.TenuresOrNoneAsync()).Keys.Where(k => Placed.Contains(k.Split('/')[0])).Select(k => k.Split('/')[0]).ToHashSet();
        }

        var owners = new Dictionary<string, Unit>();
        foreach (var kind in Placed)
        {
            if (await rig.OwnerAsync(kind, kind) is { } row)
            {
                owners[kind] = new Unit(row.Owner, row.Token);
            }
        }

        return new Fleet(owners, running, await rig.PlacedLoadsAsync());
    }

    private static bool Overlaps(Fleet fleet) => Placed.Any(kind => fleet.Running.Values.Count(r => r.Contains(kind)) > 1);

    private static async Task<Fleet> SettleAsync(MixedRig rig, MixedNode[] nodes, string what, Func<Fleet, bool> expected)
    {
        Fleet? settled = null;
        Fleet? last = null;
        try
        {
            await MixedRig.UntilAsync(
            async () =>
            {
                var fleet = last = await ReadFleetAsync(rig, nodes);
                if (Overlaps(fleet))
                {
                    fleet = await ReadFleetAsync(rig, nodes);
                    Assert.False(Overlaps(fleet), $"two processes ran a placed unit at once: {string.Join("; ", fleet.Running.Select(p => $"{p.Key} runs [{string.Join(", ", p.Value)}]"))}");
                }

                if (!expected(fleet) || !nodes.All(n => fleet.Running[n.Id].SetEquals(fleet.Owners.Where(o => o.Value.Owner == n.Id).Select(o => o.Key))))
                {
                    return false;
                }

                var again = await ReadFleetAsync(rig, nodes);
                settled = again;
                return expected(again) && again.Owners.Count == fleet.Owners.Count && again.Owners.All(o => fleet.Owners.TryGetValue(o.Key, out var u) && u == o.Value);
            },
            what);
        }
        catch (TimeoutException exception)
        {
            throw new TimeoutException($"{exception.Message} Last seen: owners [{(last is null ? "" : Describe(last))}], loads [{string.Join(", ", last?.Loads.Select(l => $"{l.Key}={l.Value}") ?? [])}], running [{string.Join("; ", last?.Running.Select(r => $"{r.Key}: {string.Join(",", r.Value)}") ?? [])}].", exception);
        }

        return settled!;
    }

    private static async Task AssertCursorAtHeadAsync(MixedRig rig, MixedNode writer, MixedNode reader) =>
        Assert.True(await rig.AppliedSeqAsync(reader.Id) >= await rig.ChangeLogHeadAsync(), $"{reader.Id}'s cursor was behind the log head when {writer.Id}'s write was acknowledged");

    private static string Answer(JsonElement answer)
    {
        Assert.Equal("Answered", answer.GetProperty("outcome").GetString());
        Assert.True(answer.GetProperty("error").ValueKind == JsonValueKind.Null, $"the handler threw: {answer.GetProperty("error")}");
        return answer.GetProperty("response").GetString()!;
    }

    private static async Task<HttpStatusCode> ReadyStatusAsync(MixedNode node)
    {
        using var response = await node.Process.Client.GetAsync("health/ready");
        return response.StatusCode;
    }
}
