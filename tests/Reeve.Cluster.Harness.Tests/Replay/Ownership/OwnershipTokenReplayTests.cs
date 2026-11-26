using Harness.Owned;
using Harness.SelfTests;
using Microsoft.Extensions.DependencyInjection;
using Npgsql;
using Reeve.Cluster.Harness.Stack;
using Reeve.Cluster.Owned;
using Xunit.Abstractions;
using OwnedWriter = Harness.Owned.OwnedWriter;

namespace Harness.Replay.Ownership;

[Collection(StackCollection.Name)]
public sealed class OwnershipTokenReplayTests(StackFixture stack, ITestOutputHelper output)
{
    private const string Keyed = "conn";
    private const string Key = "k";

    private static void Quick(ClusterOptions o)
    {
        o.RetentionInterval = TimeSpan.FromMilliseconds(500);
        o.OwnedRowRetention = TimeSpan.FromSeconds(1);
    }

    [Fact]
    public async Task Replay_M2_A3_a_re_created_key_draws_a_token_above_the_ended_tenure_and_the_stale_fenced_write_is_refused()
    {
        await using var rig = await OwnedRig.CreateAsync(stack, output.WriteLine);
        var spec = rig.Spec(
            services =>
            {
                services.AddSingleton(new OwnedPlan(1, TimeSpan.FromMilliseconds(100), IgnoreStop: true));
                services.AddClusterKeyedResource<OwnedWriter>(Keyed, k => k.Policy = AdoptionPolicy.None);
            },
            configure: Quick);

        var r1 = await rig.StartAsync("r1", spec);
        var r2 = await rig.StartAsync("r2", spec);
        await OwnedRig.UntilAsync(
            () => Task.FromResult(new[] { r1, r2 }.Any(r => r.Services.GetRequiredService<UnitRuntime>().Holds(FreeRowSweep.Kind, FreeRowSweep.Kind))),
            "the placer to place the free-row sweep");

        using var held = r2.Faults.HoldNext(OwnedFaultPoints.EnsureOwnerBeforeEffect);
        Assert.Equal(CreateOutcome.Created, await r2.Services.GetRequiredService<IKeyedResources>().CreateAsync(Keyed, Key));
        var first = await rig.Log.StartAsync(s => s is { Kind: Keyed, Replica: "r2" }, "tenure t1 on r2");
        await held.WaitReachedAsync(timeout: OwnedRig.Limit);

        await first.Context.EndAsync();
        await OwnedRig.UntilAsync(async () => await rig.RowAsync(Keyed, Key) is null, "the sweep to delete the free row");

        Assert.Equal(CreateOutcome.Created, await r1.Services.GetRequiredService<IKeyedResources>().CreateAsync(Keyed, Key));
        var second = await rig.Log.StartAsync(s => s is { Kind: Keyed, Replica: "r1" }, "tenure t2 on r1");
        Assert.True(second.Token > first.Token, $"the re-created key drew token {second.Token}, not above the ended tenure's {first.Token}");

        var timeline = await rig.ScalarAsync("SELECT reeve_cluster.server_timeline()");
        Assert.Equal("RC001", await rig.AssertOwnerStateAsync(Keyed, Key, first.Token, timeline));
        Assert.Null(await rig.AssertOwnerStateAsync(Keyed, Key, second.Token, timeline));

        Assert.True(held.Release());
        await OwnedRig.UntilAsync(() => Task.FromResult(rig.Log.Refusals.Any(r => r.Token == first.Token)), "the held write of t1 to be refused");
        var refusal = rig.Log.Refusals.First(r => r.Token == first.Token).Error;
        Assert.True(refusal.InnerException is PostgresException { SqlState: "RC001" }, $"the held write was refused, but not by the database's RC001: {refusal.GetType().Name}: {refusal.Message}");
        await rig.AwaitTenureAsync(Keyed, Key, first.Token, 4);
        var effects = await rig.EffectsAsync();
        Assert.DoesNotContain(effects, e => e.Replica == "r2");
        Assert.All(effects, e => Assert.Equal(second.Token, e.Tenure));
    }

    [Fact]
    public async Task Replay_M2_A3_cmd_a_command_retained_with_the_ended_tenures_token_is_in_doubt_for_the_re_created_key_and_not_run_as_its_own()
    {
        await using var rig = await OwnedRig.CreateAsync(stack, output.WriteLine);
        var probe = new CommandProbe();
        var spec = rig.Spec(
            services =>
            {
                services.AddSingleton(new OwnedPlan(1, TimeSpan.FromMilliseconds(100)));
                services.AddSingleton(probe);
                services.AddClusterKeyedResource<OwnedWriter>(Keyed, k => k.Policy = AdoptionPolicy.None);
                services.AddClusterCommandHandler<HeldCommandHandler>(Keyed, InDoubtPolicy.Complete);
            },
            configure: Quick);

        var r1 = await rig.StartAsync("r1", spec);
        var r2 = await rig.StartAsync("r2", spec);
        await OwnedRig.UntilAsync(
            () => Task.FromResult(new[] { r1, r2 }.Any(r => r.Services.GetRequiredService<UnitRuntime>().Holds(FreeRowSweep.Kind, FreeRowSweep.Kind))),
            "the placer to place the free-row sweep");

        Assert.Equal(CreateOutcome.Created, await r2.Services.GetRequiredService<IKeyedResources>().CreateAsync(Keyed, Key));
        var first = await rig.Log.StartAsync(s => s is { Kind: Keyed, Replica: "r2" }, "tenure t1 on r2");
        var sent = await r2.Services.GetRequiredService<IClusterCommands>().SendDurableAsync(Keyed, Key, new byte[] { 1 }, "c1");
        Assert.Equal(DurableSendOutcome.Sent, sent.Outcome);
        await OwnedRig.UntilAsync(() => Task.FromResult(probe.Starts >= 1), "the first tenure's handler to start");
        Assert.Equal(("taken", first.Token), await CommandRowAsync(rig, sent.CommandId));

        await first.Context.EndAsync();
        await OwnedRig.UntilAsync(async () => await rig.RowAsync(Keyed, Key) is null, "the sweep to delete the free row");
        Assert.Equal(("taken", first.Token), await CommandRowAsync(rig, sent.CommandId));

        Assert.Equal(CreateOutcome.Created, await r1.Services.GetRequiredService<IKeyedResources>().CreateAsync(Keyed, Key));
        var second = await rig.Log.StartAsync(s => s is { Kind: Keyed, Replica: "r1" }, "tenure t2 on r1");

        await OwnedRig.UntilAsync(
            async () => probe.Starts >= 2 || (await CommandRowAsync(rig, sent.CommandId)).State != "taken",
            "the new tenure to classify the retained command");
        Assert.True(probe.Starts == 1, $"the command retained with taken_token {first.Token} was read as the re-created tenure's own (token {second.Token}): its handler ran again");
        Assert.Equal(("in_doubt", first.Token), await CommandRowAsync(rig, sent.CommandId));
        var waited = await r1.Services.GetRequiredService<IClusterCommands>().WaitAsync(Keyed, Key, "c1", TimeSpan.FromSeconds(30));
        Assert.Equal(DurableCommandOutcome.InDoubt, waited.Outcome);
        Assert.True(second.Token > first.Token, $"the re-created key drew token {second.Token}, not above the ended tenure's {first.Token}");
    }

    private static async Task<(string State, long? Token)> CommandRowAsync(OwnedRig rig, long id)
    {
        await using var connection = await rig.Run.OpenObserverConnectionAsync();
        await using var command = new NpgsqlCommand("SELECT state, taken_token FROM reeve_cluster.command WHERE id = @id", connection);
        command.Parameters.AddWithValue("id", id);
        await using var reader = await command.ExecuteReaderAsync();
        Assert.True(await reader.ReadAsync(), "the command row is gone");
        return (reader.GetString(0), reader.IsDBNull(1) ? null : reader.GetInt64(1));
    }

    private sealed class CommandProbe
    {
        private int starts;

        public int Starts => Volatile.Read(ref starts);

        public void Started() => Interlocked.Increment(ref starts);
    }

    private sealed class HeldCommandHandler(CommandProbe probe) : IDurableCommandHandler
    {
        public async Task<ReadOnlyMemory<byte>> HandleAsync(DurableCommand command, CancellationToken cancellationToken)
        {
            probe.Started();
            await Task.Delay(Timeout.Infinite, cancellationToken);
            return default;
        }
    }

    [Fact]
    public async Task Replay_M2_F1_probe_an_ownership_statement_that_waits_on_a_row_is_ended_by_the_servers_transaction_timeout()
    {
        await using var rig = await OwnedRig.CreateAsync(stack, output.WriteLine);
        var spec = rig.Spec(
            services =>
            {
                services.AddSingleton(new OwnedPlan(1, TimeSpan.FromMilliseconds(100)));
                services.AddClusterKeyedResource<OwnedWriter>(Keyed, k => k.Policy = AdoptionPolicy.None);
            });
        var r1 = await rig.StartAsync("r1", spec);

        await using var observer = await rig.Run.OpenObserverConnectionAsync();
        await using var open = await observer.BeginTransactionAsync();
        await using (var insert = new NpgsqlCommand(
            "INSERT INTO reeve_cluster.owned (kind, key, owner_replica, owner_epoch, token) VALUES (@kind, @key, NULL, NULL, reeve_cluster.draw_token())",
            observer,
            open))
        {
            insert.Parameters.AddWithValue("kind", Keyed);
            insert.Parameters.AddWithValue("key", Key);
            await insert.ExecuteNonQueryAsync();
        }

        var failure = await Record.ExceptionAsync(() => r1.Services.GetRequiredService<IKeyedResources>().CreateAsync(Keyed, Key));
        await open.RollbackAsync();

        Assert.NotNull(failure);
        var postgres = failure as PostgresException ?? failure.InnerException as PostgresException;
        Assert.True(postgres is { SqlState: "25P04" }, $"the creator's statement ended with {failure.GetType().Name}, not the server's transaction_timeout (25P04): {failure.Message}");
        Assert.Null(await rig.RowAsync(Keyed, Key));
    }

    [Fact]
    public async Task Replay_M2_S2r1_the_sweep_leaves_the_high_water_key_and_a_re_created_key_raises_it()
    {
        await using var rig = await OwnedRig.CreateAsync(stack, output.WriteLine);
        var spec = rig.Spec(
            services =>
            {
                services.AddSingleton(new OwnedPlan(1, TimeSpan.FromMilliseconds(100), Redis: true));
                services.AddClusterKeyedResource<OwnedWriter>(Keyed, k =>
                {
                    k.Policy = AdoptionPolicy.None;
                    k.RedisFence = true;
                });
            },
            configure: Quick,
            redis: true);
        var replicas = new[] { await rig.StartAsync("r1", spec), await rig.StartAsync("r2", spec) };
        await OwnedRig.UntilAsync(
            () => Task.FromResult(replicas.Any(r => r.Services.GetRequiredService<UnitRuntime>().Holds(FreeRowSweep.Kind, FreeRowSweep.Kind))),
            "the placer to place the free-row sweep");

        var creator = replicas[0].Services.GetRequiredService<IKeyedResources>();
        Assert.Equal(CreateOutcome.Created, await creator.CreateAsync(Keyed, Key));
        var first = await rig.Log.StartAsync(s => s.Kind == Keyed, "the key's first tenure");
        await OwnedRig.UntilAsync(async () => (await rig.RedisLogAsync(first)).Contains(first.Token), "the first tenure's write in Redis");
        var database = await rig.RedisAsync();
        var highWater = $"{first.RedisTag}:hw";
        Assert.Equal(first.Token, (long)await database.StringGetAsync(highWater));

        await first.Context.EndAsync();
        await OwnedRig.UntilAsync(async () => await rig.RowAsync(Keyed, Key) is null, "the sweep to delete the free row");
        await rig.ExecuteAsync(
            $"""
            WITH t AS (SELECT reeve_cluster.draw_token() AS token),
            o AS (INSERT INTO reeve_cluster.owned (kind, key, owner_replica, owner_epoch, token, updated_at)
                  SELECT '{Keyed}', 'later', NULL, NULL, token, now() - interval '2 days' FROM t)
            INSERT INTO reeve_cluster.release_request (kind, key, token, requested_at) SELECT '{Keyed}', 'later', token, now() FROM t
            """);
        await OwnedRig.UntilAsync(async () => await rig.RowAsync(Keyed, "later") is null, "the sweep to delete the later free row");

        var kept = await database.StringGetAsync(highWater);
        Assert.True(kept.HasValue, "the sweep deleted the key's Redis high-water key");
        Assert.Equal(first.Token, (long)kept);

        Assert.Equal(CreateOutcome.Created, await creator.CreateAsync(Keyed, Key));
        var second = await rig.Log.StartAsync(s => s.Kind == Keyed && s.Token > first.Token, "the re-created key's tenure");
        await OwnedRig.UntilAsync(async () => (await rig.RedisLogAsync(second)).Contains(second.Token), "the second tenure's write in Redis");
        Assert.Equal(second.Token, (long)await database.StringGetAsync(highWater));
        OwnedRig.AssertNoInterleave(await rig.RedisLogAsync(second), "the Redis list in the order Redis took it");
    }
}
