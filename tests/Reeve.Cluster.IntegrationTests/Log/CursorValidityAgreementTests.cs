using Npgsql;
using NpgsqlTypes;
using Reeve.Cluster.Core;
using Reeve.Cluster.Harness.Containers;

namespace Int.Log;

public sealed class CursorValidityAgreementTests : IClassFixture<PostgresFixture>
{
    private readonly PostgresFixture postgres;

    public CursorValidityAgreementTests(PostgresFixture postgres)
    {
        this.postgres = postgres;
    }

    public static TheoryData<string, bool> Cases => new()
    {
        { "names a row", true },
        { "names a row with another token", false },
        { "names a row without its token", false },
        { "names no row", false },
        { "names a row retention deleted", false },
        { "is above the head", false },
        { "is 0 with horizon 0", true },
        { "is 0 with a horizon above 0", false },
        { "is 0 with horizon 0 on a log with entries", true },
    };

    [Theory]
    [MemberData(nameof(Cases))]
    public async Task I5_the_SQL_predicate_and_its_CSharp_form_agree(string @case, bool valid)
    {
        var log = await LogRig.CreateAsync(postgres);
        await using var connection = await log.OpenAsync();
        var entries = new List<Cursor>();
        for (var i = 0; i < 3; i++)
        {
            await using var insert = new NpgsqlCommand(
                "INSERT INTO reeve_cluster.change_log (aggregate_type, aggregate_key) VALUES ('t', 'k') RETURNING seq, entry_token", connection);
            await using var reader = await insert.ExecuteReaderAsync();
            await reader.ReadAsync();
            entries.Add(new Cursor(reader.GetInt64(0), reader.GetGuid(1)));
        }

        var cursor = @case switch
        {
            "names a row" => entries[1],
            "names a row with another token" => entries[1] with { Token = Guid.NewGuid() },
            "names a row without its token" => entries[1] with { Token = null },
            "names no row" => await Deleted(connection, entries[1]),
            "names a row retention deleted" => await Retained(connection, entries[0], above: entries[1]),
            "is above the head" => new Cursor(entries[2].Seq + 10, Guid.NewGuid()),
            "is 0 with horizon 0" => await Emptied(connection),
            "is 0 with a horizon above 0" => await Retained(connection, Cursor.Empty, above: entries[1]),
            "is 0 with horizon 0 on a log with entries" => Cursor.Empty,
            _ => throw new ArgumentOutOfRangeException(nameof(@case)),
        };

        var sql = await SqlAsync(connection, cursor);
        var (rowToken, horizon) = await FactsAsync(connection, cursor);
        var csharp = CursorValidity.IsValid(cursor, rowToken, horizon);

        Assert.Equal(valid, sql);
        Assert.Equal(sql, csharp);
    }

    [Fact]
    public async Task The_SQL_predicate_answers_false_never_null_for_a_null_seq()
    {
        var log = await LogRig.CreateAsync(postgres);
        await using var connection = await log.OpenAsync();
        await using var command = new NpgsqlCommand("SELECT reeve_cluster.cursor_valid(NULL, NULL)", connection);

        Assert.Equal(false, await command.ExecuteScalarAsync());
    }

    private static async Task<Cursor> Deleted(NpgsqlConnection connection, Cursor entry)
    {
        await ExecuteAsync(connection, $"DELETE FROM reeve_cluster.change_log WHERE seq = {entry.Seq}");
        return entry;
    }

    private static async Task<Cursor> Retained(NpgsqlConnection connection, Cursor cursor, Cursor above)
    {
        await ExecuteAsync(connection, $"DELETE FROM reeve_cluster.change_log WHERE seq < {above.Seq}; UPDATE reeve_cluster.cluster SET horizon_seq = {above.Seq}");
        return cursor;
    }

    private static async Task<Cursor> Emptied(NpgsqlConnection connection)
    {
        await ExecuteAsync(connection, "DELETE FROM reeve_cluster.change_log");
        return Cursor.Empty;
    }

    private static async Task ExecuteAsync(NpgsqlConnection connection, string sql)
    {
        await using var command = new NpgsqlCommand(sql, connection);
        await command.ExecuteNonQueryAsync();
    }

    private static async Task<bool> SqlAsync(NpgsqlConnection connection, Cursor cursor)
    {
        await using var command = new NpgsqlCommand("SELECT reeve_cluster.cursor_valid(@seq, @token)", connection);
        command.Parameters.AddWithValue("seq", cursor.Seq);
        command.Parameters.Add(new NpgsqlParameter("token", NpgsqlDbType.Uuid) { Value = (object?)cursor.Token ?? DBNull.Value });
        return (bool)(await command.ExecuteScalarAsync())!;
    }

    private static async Task<(Guid? RowToken, long Horizon)> FactsAsync(NpgsqlConnection connection, Cursor cursor)
    {
        await using var command = new NpgsqlCommand(
            "SELECT (SELECT entry_token FROM reeve_cluster.change_log WHERE seq = @seq), (SELECT horizon_seq FROM reeve_cluster.cluster)", connection);
        command.Parameters.AddWithValue("seq", cursor.Seq);
        await using var reader = await command.ExecuteReaderAsync();
        await reader.ReadAsync();
        return (reader.IsDBNull(0) ? null : reader.GetGuid(0), reader.GetInt64(1));
    }
}
