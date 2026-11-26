using Microsoft.Extensions.Options;
using Npgsql;
using Reeve.Cluster;
using Reeve.Cluster.Commands;
using Reeve.Cluster.Harness.Containers;
using Reeve.Cluster.Owned;
using Int.Owned;
using static Int.Owned.OwnedDatabase;

namespace Int.Commands;

internal sealed class CommandsRig : IAsyncDisposable
{
    public const string Kind = "scheduler";
    public const string Key = "k1";

    private readonly OwnedDataSource data;

    private CommandsRig(OwnedDatabase db, long token, OwnedDataSource data, CommandStore store)
    {
        Db = db;
        Token = token;
        this.data = data;
        Store = store;
        Owner = new FencedOwner(Kind, Key, token, db.Timeline);
    }

    public OwnedDatabase Db { get; }

    public long Token { get; }

    public CommandStore Store { get; }

    public FencedOwner Owner { get; }

    public static async Task<CommandsRig> CreateAsync(PostgresFixture postgres)
    {
        var db = await OwnedDatabase.CreateAsync(postgres);
        await db.AddMemberAsync("a");
        var token = await db.CreateOwnedAsync(Kind, Key, "a");
        var options = Options.Create(new ClusterOptions { ConnectionString = db.ConnectionString });
        var data = new OwnedDataSource(options);
        return new CommandsRig(db, token, data, new CommandStore(data, options));
    }

    public FencedOwner OwnerWith(long token) => new(Kind, Key, token, Db.Timeline);

    public async Task<SentCommand> InsertAsync(string idempotency, byte[]? payload = null, string kind = Kind, string key = Key) =>
        await Store.InsertAsync(kind, key, idempotency, payload ?? [1, 2, 3], CancellationToken.None)
            ?? throw new InvalidOperationException("The insert met a row.");

    public async Task<(string State, long? Token)> RowAsync(long id)
    {
        await using var connection = await Db.OpenAsync();
        await using var command = Command(connection, "SELECT state, taken_token FROM reeve_cluster.command WHERE id = @id", ("id", id));
        await using var reader = await command.ExecuteReaderAsync();
        Assert.True(await reader.ReadAsync());
        return (reader.GetString(0), reader.IsDBNull(1) ? null : reader.GetInt64(1));
    }

    public async Task SetStateAsync(long id, string state, long? token = null)
    {
        await using var connection = await Db.OpenAsync();
        await ExecAsync(
            connection,
            "UPDATE reeve_cluster.command SET state = @state, taken_token = @token, taken_at = CASE WHEN @token::bigint IS NULL THEN NULL ELSE now() END WHERE id = @id",
            ("state", state), ("token", (object?)token ?? DBNull.Value), ("id", id));
    }

    public ValueTask DisposeAsync() => data.DisposeAsync();
}
