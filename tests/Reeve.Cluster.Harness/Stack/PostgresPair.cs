using DotNet.Testcontainers.Builders;
using DotNet.Testcontainers.Configurations;
using DotNet.Testcontainers.Containers;
using DotNet.Testcontainers.Networks;

namespace Reeve.Cluster.Harness.Stack;

internal enum StandbyMode
{
    Async,

    SyncFirst,

    SyncAny,
}

internal sealed class PostgresNode(IContainer container, string alias)
{
    public IContainer Container { get; } = container;

    public string Alias { get; } = alias;

    public string Upstream => $"{Alias}:5432";

    public string DataDir { get; set; } = "";
}

internal sealed class PostgresPair : IAsyncDisposable
{
    public const string StandbyName = "standby1";
    private const string PrimaryAlias = "reeve-pg";
    private const string StandbyAlias = "reeve-pg-standby";
    private const string Password = "postgres";
    private const string ReplicationRole = "replicator";
    private const string ReplicationPassword = "replicator";
    private const int Postgres = 999;
    private const string StandbyDataDir = "/var/lib/postgresql/standby-data";

    private readonly INetwork network;
    private readonly string image;
    private readonly Action<string> log;
    private string pgData = "";

    private PostgresPair(INetwork network, string image, PostgresNode primary, Action<string> log)
    {
        this.network = network;
        this.image = image;
        Primary = primary;
        this.log = log;
    }

    public PostgresNode Primary { get; private set; }

    public PostgresNode? Standby { get; private set; }

    public StandbyMode Mode { get; private set; } = StandbyMode.Async;

    private readonly List<PostgresNode> retired = [];

    public static async Task<PostgresPair> StartPrimaryAsync(INetwork network, string image, Action<string> log, CancellationToken cancellationToken = default)
    {
        var container = new ContainerBuilder(image)
            .WithNetwork(network)
            .WithNetworkAliases(PrimaryAlias)
            .WithEnvironment("POSTGRES_PASSWORD", Password)
            .WithResourceMapping(Bytes(PrimaryInitScript), FilePath.Of("/docker-entrypoint-initdb.d/10-replication.sh"), fileMode: (UnixFileModes)0b111_101_101)
            .WithCommand("postgres")
            .Build();
        await container.StartAsync(cancellationToken).ConfigureAwait(false);
        var pair = new PostgresPair(network, image, new PostgresNode(container, PrimaryAlias), log);
        await pair.WaitForSqlAsync(pair.Primary, cancellationToken).ConfigureAwait(false);
        pair.pgData = (await container.ExecAsync(["printenv", "PGDATA"], cancellationToken).ConfigureAwait(false)).Stdout.Trim();
        pair.Primary.DataDir = pair.pgData;
        return pair;
    }

    public async Task AddStandbyAsync(string replicationAddress, CancellationToken cancellationToken = default)
    {
        var (host, port) = Split(replicationAddress);
        var container = new ContainerBuilder(image)
            .WithNetwork(network)
            .WithNetworkAliases(StandbyAlias)
            .WithEnvironment("PGPASSWORD", ReplicationPassword)
            .WithCreateParameterModifier(p => p.User = "postgres")
            .WithEntrypoint("bash", "-c", StandbyScript(host, port))
            .Build();
        await container.StartAsync(cancellationToken).ConfigureAwait(false);
        Standby = new PostgresNode(container, StandbyAlias) { DataDir = StandbyDataDir };
        await WaitForSqlAsync(Standby, cancellationToken).ConfigureAwait(false);
        await WaitForPrimaryAsync(
            $"SELECT count(*) FROM pg_stat_replication WHERE application_name = '{StandbyName}' AND state = 'streaming'",
            "the standby to stream",
            cancellationToken).ConfigureAwait(false);
    }

    public async Task SetModeAsync(StandbyMode mode, CancellationToken cancellationToken = default)
    {
        await ApplyModeAsync(Primary, mode, cancellationToken).ConfigureAwait(false);
        Mode = mode;
        if (Standby is not null)
        {
            var state = mode switch { StandbyMode.Async => "async", StandbyMode.SyncFirst => "sync", _ => "quorum" };
            await WaitForPrimaryAsync(
                $"SELECT count(*) FROM pg_stat_replication WHERE application_name = '{StandbyName}' AND state = 'streaming' AND sync_state = '{state}'",
                $"the standby to be {state}",
                cancellationToken).ConfigureAwait(false);
        }
    }

    public async Task<IReadOnlyList<string>> ReplicationStateAsync(CancellationToken cancellationToken = default) =>
        (await QueryAsync(Primary, "SELECT application_name || ' ' || state || ' ' || sync_state FROM pg_stat_replication ORDER BY 1", cancellationToken).ConfigureAwait(false))
            .Split('\n', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries);

    public async Task<string> QueryAsync(PostgresNode node, string sql, CancellationToken cancellationToken = default, string database = "postgres")
    {
        var result = await node.Container.ExecAsync(["psql", "-U", "postgres", "-d", database, "-X", "-v", "ON_ERROR_STOP=1", "-tAc", sql], cancellationToken).ConfigureAwait(false);
        return result.ExitCode == 0
            ? result.Stdout.Trim()
            : throw new InvalidOperationException($"psql on {node.Alias} failed ({result.ExitCode}) for '{sql}': {result.Stderr}");
    }

    public async Task CrashPrimaryAsync(CancellationToken cancellationToken = default)
    {
        log($"database: crash {Primary.Alias}");
        var stop = $"pg_ctl stop -m immediate -D '{Primary.DataDir}'";
        await Primary.Container.ExecAsync(["bash", "-c", $"if [ \"$(id -u)\" = 0 ]; then exec gosu postgres {stop}; else exec {stop}; fi"], cancellationToken).ConfigureAwait(false);
        await WaitUntilAsync(() => IsDownAsync(Primary, cancellationToken), $"{Primary.Alias} to stop", cancellationToken).ConfigureAwait(false);
    }

    public async Task PromoteAsync(CancellationToken cancellationToken = default)
    {
        var standby = Standby ?? throw new InvalidOperationException("The pair has no standby to promote.");
        log($"database: promote {standby.Alias}");
        await QueryAsync(standby, "SELECT pg_promote(true, 60)", cancellationToken).ConfigureAwait(false);
        await WaitUntilAsync(async () => await QueryAsync(standby, "SELECT NOT pg_is_in_recovery()", cancellationToken).ConfigureAwait(false) == "t", "the standby to leave recovery", cancellationToken).ConfigureAwait(false);
        retired.Add(Primary);
        Primary = standby;
        Standby = null;
        Mode = StandbyMode.Async;
    }

    public async Task SwitchoverAsync(Func<CancellationToken, Task>? promoted = null, CancellationToken cancellationToken = default)
    {
        var mode = Mode;
        var old = Primary;
        log($"database: switchover from {old.Alias}");
        await old.Container.StopAsync(cancellationToken).ConfigureAwait(false);
        var standby = Standby ?? throw new InvalidOperationException("The pair has no standby to promote.");
        await PromoteAsync(cancellationToken).ConfigureAwait(false);
        retired.Remove(old);
        if (promoted is not null)
        {
            await promoted(cancellationToken).ConfigureAwait(false);
        }

        await old.Container.CopyAsync(Array.Empty<byte>(), $"{old.DataDir}/standby.signal", Postgres, Postgres, UnixFileModes.UserRead | UnixFileModes.UserWrite, cancellationToken).ConfigureAwait(false);
        var conf = $"primary_conninfo = 'host={standby.Alias} port=5432 user={ReplicationRole} password={ReplicationPassword} application_name={StandbyName}'\n";
        await old.Container.CopyAsync(System.Text.Encoding.UTF8.GetBytes(conf), $"{old.DataDir}/postgresql.auto.conf", Postgres, Postgres, UnixFileModes.UserRead | UnixFileModes.UserWrite, cancellationToken).ConfigureAwait(false);
        await old.Container.StartAsync(cancellationToken).ConfigureAwait(false);
        await WaitForSqlAsync(old, cancellationToken).ConfigureAwait(false);
        Standby = old;
        await WaitForPrimaryAsync(
            $"SELECT count(*) FROM pg_stat_replication WHERE application_name = '{StandbyName}' AND state = 'streaming'",
            "the old primary to stream from the new one",
            cancellationToken).ConfigureAwait(false);
        if (mode != StandbyMode.Async)
        {
            await SetModeAsync(mode, cancellationToken).ConfigureAwait(false);
        }
    }

    public async ValueTask DisposeAsync()
    {
        foreach (var node in new[] { Standby, Primary }.Concat(retired))
        {
            if (node is not null)
            {
                await node.Container.DisposeAsync().ConfigureAwait(false);
            }
        }
    }

    private static async Task<bool> IsDownAsync(PostgresNode node, CancellationToken cancellationToken)
    {
        try
        {
            return (await node.Container.ExecAsync(["pg_isready", "-q"], cancellationToken).ConfigureAwait(false)).ExitCode != 0;
        }
        catch (Exception exception) when (exception is not OperationCanceledException)
        {
            return true;
        }
    }

    private async Task ApplyModeAsync(PostgresNode node, StandbyMode mode, CancellationToken cancellationToken)
    {
        var sql = mode switch
        {
            StandbyMode.Async => "ALTER SYSTEM RESET synchronous_standby_names",
            StandbyMode.SyncFirst => $"ALTER SYSTEM SET synchronous_standby_names = 'FIRST 1 ({StandbyName})'",
            _ => $"ALTER SYSTEM SET synchronous_standby_names = 'ANY 1 ({StandbyName})'",
        };
        await QueryAsync(node, sql, cancellationToken).ConfigureAwait(false);
        await QueryAsync(node, "SELECT pg_reload_conf()", cancellationToken).ConfigureAwait(false);
    }

    private Task WaitForPrimaryAsync(string countSql, string what, CancellationToken cancellationToken) =>
        WaitUntilAsync(async () => await QueryAsync(Primary, countSql, cancellationToken).ConfigureAwait(false) == "1", what, cancellationToken);

    private async Task WaitForSqlAsync(PostgresNode node, CancellationToken cancellationToken) =>
        await WaitUntilAsync(
            async () =>
            {
                var result = await node.Container.ExecAsync(["psql", "-U", "postgres", "-X", "-tAc", "SELECT 1"], cancellationToken).ConfigureAwait(false);
                return result.ExitCode == 0;
            },
            $"{node.Alias} to answer SQL",
            cancellationToken).ConfigureAwait(false);

    private static async Task WaitUntilAsync(Func<Task<bool>> condition, string what, CancellationToken cancellationToken)
    {
        var deadline = DateTime.UtcNow.AddSeconds(120);
        while (true)
        {
            try
            {
                if (await condition().ConfigureAwait(false))
                {
                    return;
                }
            }
            catch (InvalidOperationException) when (DateTime.UtcNow < deadline)
            {
            }

            if (DateTime.UtcNow > deadline)
            {
                throw new TimeoutException($"Gave up waiting for {what}.");
            }

            await Task.Delay(250, cancellationToken).ConfigureAwait(false);
        }
    }

    private static (string Host, int Port) Split(string address)
    {
        var colon = address.LastIndexOf(':');
        return (address[..colon], int.Parse(address[(colon + 1)..], System.Globalization.CultureInfo.InvariantCulture));
    }

    private static byte[] Bytes(string script) => System.Text.Encoding.UTF8.GetBytes(script.ReplaceLineEndings("\n"));

    private const string PrimaryInitScript = $$"""
        #!/bin/bash
        set -euo pipefail
        psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" <<SQL
        CREATE ROLE {{ReplicationRole}} WITH REPLICATION LOGIN PASSWORD '{{ReplicationPassword}}';
        SQL
        echo "host replication {{ReplicationRole}} all scram-sha-256" >> "$PGDATA/pg_hba.conf"
        """;

    private static string StandbyScript(string streamHost, int streamPort) => $$"""
        set -euo pipefail
        export PGDATA={{StandbyDataDir}}
        if [ ! -f "$PGDATA/PG_VERSION" ]; then
          mkdir -p "$PGDATA"
          chmod 700 "$PGDATA"
          until pg_isready -q -h {{PrimaryAlias}} -p 5432 -U postgres; do sleep 1; done
          pg_basebackup -h {{PrimaryAlias}} -p 5432 -U {{ReplicationRole}} -D "$PGDATA" -X stream -R -C -S {{StandbyName}}_slot
          # -R wrote primary_conninfo without an application_name, and straight to the primary; the stream goes through the stack's proxy
          echo "primary_conninfo = 'host={{streamHost}} port={{streamPort}} user={{ReplicationRole}} password={{ReplicationPassword}} application_name={{StandbyName}}'" >> "$PGDATA/postgresql.auto.conf"
        fi
        exec postgres -D "$PGDATA"
        """;
}
