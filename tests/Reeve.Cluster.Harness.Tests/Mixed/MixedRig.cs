using Harness.Slice;
using Npgsql;
using Reeve.Cluster.Harness.Processes;
using Reeve.Cluster.Harness.Replicas;
using Reeve.Cluster.Harness.Stack;
using Reeve.Cluster.Membership;

namespace Harness.Mixed;

internal sealed class MixedRig : IAsyncDisposable
{
    public const string OldRefVariable = "REEVE_MIXED_OLD_REF";
    public const string NewRefVariable = "REEVE_MIXED_NEW_REF";

    public const string SchemaDeltaVariable = "REEVE_MIXED_SCHEMA_DELTA";

    public static readonly TimeSpan Limit = TimeSpan.FromSeconds(90);
    private static readonly TimeSpan ExitLimit = TimeSpan.FromSeconds(60);

    private readonly SampleBuild oldBuild;
    private readonly SampleBuild newBuild;
    private string? oldLibrary;
    private string? newLibrary;

    private MixedRig(ProcessCluster cluster, SampleBuild oldBuild, SampleBuild newBuild)
    {
        Cluster = cluster;
        this.oldBuild = oldBuild;
        this.newBuild = newBuild;
    }

    public ProcessCluster Cluster { get; }

    public HarnessRun Run => Cluster.Run;

    public static string? OldRef => Environment.GetEnvironmentVariable(OldRefVariable);

    public static string? NewRef => Environment.GetEnvironmentVariable(NewRefVariable);

    public static bool SchemaDeltaDeclaredNone => string.Equals(Environment.GetEnvironmentVariable(SchemaDeltaVariable), "none", StringComparison.OrdinalIgnoreCase);

    public static async Task<MixedRig> CreateAsync(StackFixture stack, Action<string> log, params string[] ids)
    {
        var oldRef = OldRef ?? throw new InvalidOperationException($"{OldRefVariable} is not set.");
        var oldBuild = await SampleBuilds.FromRefAsync(oldRef, log);
        var newBuild = string.IsNullOrWhiteSpace(NewRef) ? SampleBuilds.Current() : await SampleBuilds.FromRefAsync(NewRef, log);
        var cluster = await ProcessCluster.CreateAsync(stack.Stack, log, ids, migrate: false);
        return new MixedRig(cluster, oldBuild, newBuild);
    }

    public async Task<MixedNode> StartAsync(string id)
    {
        var process = await Cluster.StartProcessAsync(id, build: BuildOf(id));
        var node = new MixedNode(process, id.StartsWith('o') ? "old" : "new");
        await RecordLibraryAsync(node);
        return node;
    }

    public async Task<MixedNode> StartWithoutWaitingAsync(string id)
    {
        var process = await Cluster.StartWithoutWaitingAsync(id, build: BuildOf(id));
        return new MixedNode(process, id.StartsWith('o') ? "old" : "new");
    }

    public async Task SeedSchemaWithOldBuildAsync()
    {
        var seeder = await StartAsync("o1");
        await StopAsync(seeder);
    }

    public static async Task StopAsync(MixedNode node)
    {
        node.MarkStopping();
        node.Process.Signal(ProcessSignal.Term);
        var exitCode = await node.Process.WaitForExitAsync(ExitLimit);
        Assert.True(exitCode == 0, $"{node.Id} ({node.Build}) drained to exit code {exitCode}.{Environment.NewLine}{string.Join(Environment.NewLine, node.Process.Output.TakeLast(30))}");
    }

    public async Task RecordLibraryAsync(MixedNode node)
    {
        var library = await node.LibraryVersionAsync();
        if (node.Build == "old")
        {
            oldLibrary = library;
        }
        else
        {
            newLibrary = library;
        }

        if (oldLibrary is not null && newLibrary is not null)
        {
            Assert.True(Core(oldLibrary) < Core(newLibrary), $"The old build runs library {oldLibrary} and the new one {newLibrary}: the new build must be a later release ({OldRefVariable}, {NewRefVariable}).");
        }
    }

    public static async Task<(string Old, string New)> VersionsAsync(MixedNode oldNode, MixedNode newNode)
    {
        var (oldVersion, newVersion) = (await oldNode.VersionAsync(), await newNode.VersionAsync());
        Assert.True(Core(oldVersion) < Core(newVersion), $"The old process reports {oldVersion} and the new one {newVersion}: the new build must be a later release ({OldRefVariable}, {NewRefVariable}).");
        return (oldVersion, newVersion);
    }

    private static Version Core(string version) => Version.Parse(version.Split('-', '+')[0]);

    public async Task<(string Owner, long Token)?> OwnerAsync(string kind, string key)
    {
        await using var connection = await Run.OpenObserverConnectionAsync();
        await using var command = new NpgsqlCommand("SELECT owner_replica, token FROM reeve_cluster.owned WHERE kind = @kind AND key = @key", connection);
        command.Parameters.AddWithValue("kind", kind);
        command.Parameters.AddWithValue("key", key);
        await using var reader = await command.ExecuteReaderAsync();
        return await reader.ReadAsync() && !reader.IsDBNull(0) ? (reader.GetString(0), reader.GetInt64(1)) : null;
    }

    public async Task<IReadOnlyDictionary<string, int>> PlacedLoadsAsync()
    {
        await using var connection = await Run.OpenObserverConnectionAsync();
        await using var command = new NpgsqlCommand("SELECT owner_replica, count(*) FROM reeve_cluster.owned WHERE kind = key AND kind <> 'placer' AND owner_replica IS NOT NULL GROUP BY owner_replica", connection);
        await using var reader = await command.ExecuteReaderAsync();
        var loads = new Dictionary<string, int>();
        while (await reader.ReadAsync())
        {
            loads[reader.GetString(0)] = (int)reader.GetInt64(1);
        }

        return loads;
    }

    public async Task<string?> VersionFloorAsync()
    {
        await using var connection = await Run.OpenObserverConnectionAsync();
        await using var command = new NpgsqlCommand("SELECT version_floor FROM reeve_cluster.cluster", connection);
        return await command.ExecuteScalarAsync() as string;
    }

    public async Task<MemberRowState?> StateOfAsync(string id) => (await Run.Observer.MemberAsync(id))?.State;

    public async Task<string> LowerVersionFloorAsync(string version)
    {
        var command = await Cluster.StartWithoutWaitingAsync("n1", build: newBuild, arguments: ["--lower-floor", version]);
        var exitCode = await command.WaitForExitAsync(ExitLimit);
        var line = command.Output.FirstOrDefault(l => l.StartsWith("harness: floor-lowering ", StringComparison.Ordinal));
        Assert.True(line is not null, $"The floor command of the new build reported nothing (exit code {exitCode}).{Environment.NewLine}{string.Join(Environment.NewLine, command.Output.TakeLast(30))}");
        return line["harness: floor-lowering ".Length..];
    }

    public async Task<string> SchemaDigestAsync()
    {
        await using var connection = await Run.OpenObserverConnectionAsync();
        await using var command = new NpgsqlCommand(
            """
            SELECT md5(
              (SELECT coalesce(string_agg(table_schema || '.' || table_name || '.' || column_name || ':' || data_type || ':' || is_nullable || ':' || coalesce(column_default, ''), E'\n' ORDER BY table_schema, table_name, column_name), '')
                 FROM information_schema.columns WHERE table_schema NOT IN ('pg_catalog', 'information_schema'))
              || (SELECT coalesce(string_agg(schemaname || '.' || indexdef, E'\n' ORDER BY schemaname, indexname), '')
                 FROM pg_indexes WHERE schemaname NOT IN ('pg_catalog', 'information_schema'))
              || (SELECT coalesce(string_agg(query_to_xml(format('SELECT * FROM %I.%I ORDER BY 1', schemaname, tablename), false, true, '')::text, E'\n' ORDER BY schemaname, tablename), '')
                 FROM pg_tables WHERE tablename ILIKE '%migration%history%'))
            """,
            connection);
        return (string)(await command.ExecuteScalarAsync())!;
    }

    public static readonly TimeSpan ApplyLag = TimeSpan.FromMilliseconds(200);

    public async Task<IAsyncDisposable> LagApplyAsync(MixedNode reader) =>
        new Lag(await reader.Process.Link.Postgres.LatencyAsync(ApplyLag, TimeSpan.Zero, Direction.Downstream));

    private sealed class Lag(ProxyToxic toxic) : IAsyncDisposable
    {
        public async ValueTask DisposeAsync() => await toxic.RemoveAsync();
    }

    public async Task<long> ChangeLogHeadAsync()
    {
        await using var connection = await Run.OpenObserverConnectionAsync();
        await using var command = new NpgsqlCommand("SELECT coalesce(max(seq), 0) FROM reeve_cluster.change_log", connection);
        return (long)(await command.ExecuteScalarAsync())!;
    }

    public async Task<long> AppliedSeqAsync(string id) => (await Run.Observer.MemberAsync(id))?.AppliedSeq ?? throw new InvalidOperationException($"{id} has no member row.");

    public async Task AwaitRefusedAsync(MixedNode node, string reason)
    {
        await OutputContainsAsync(node, reason);
        await ListeningAsync(node);
        await RecordLibraryAsync(node);
    }

    private static Task OutputContainsAsync(MixedNode node, string text) =>
        SliceRig.UntilAsync(() => Task.FromResult(node.Process.Output.Any(line => line.Contains(text, StringComparison.Ordinal))), $"{node.Id} writing '{text}'", Limit);

    private static Task ListeningAsync(MixedNode node) =>
        SliceRig.UntilAsync(
            async () =>
            {
                try
                {
                    using var response = await node.Process.Client.GetAsync("health/live");
                    return response.IsSuccessStatusCode;
                }
                catch (HttpRequestException)
                {
                    return false;
                }
            },
            $"{node.Id} listening",
            Limit);

    public static Task UntilAsync(Func<Task<bool>> condition, string what) => SliceRig.UntilAsync(condition, what, Limit);

    public ValueTask DisposeAsync() => Cluster.DisposeAsync();

    private SampleBuild BuildOf(string id) => id.StartsWith('o') ? oldBuild : id.StartsWith('n') ? newBuild : throw new ArgumentException($"A replica of a mixed fleet is called o… (old build) or n… (new build): '{id}'.", nameof(id));
}
