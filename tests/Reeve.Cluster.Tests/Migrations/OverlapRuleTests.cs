using System.Text.RegularExpressions;
using Microsoft.EntityFrameworkCore.Infrastructure;
using Microsoft.EntityFrameworkCore.Migrations;
using Reeve.Cluster.Ask;
using Reeve.Cluster.Migrations;
using Reeve.Cluster.Redis.Fence;
using Reeve.Cluster.Redis.Relay;
using Reeve.Cluster.Redis;
using Reeve.Cluster.Wakeup;

namespace Unit.Migrations;

public sealed partial class OverlapRuleTests
{
    private static readonly Dictionary<string, (string Statement, string Reason)> Exceptions = new()
    {
    };

    private static IEnumerable<string> ViolationsOf(string id, string script) => ViolationsOf(Exceptions, id, script);

    private static IEnumerable<string> ViolationsOf(
        Dictionary<string, (string Statement, string Reason)> exceptions, string id, string script)
    {
        if (!exceptions.TryGetValue(id, out var exception))
        {
            return Violations(script);
        }

        var parts = script.Split(exception.Statement);
        return parts.Length == 2
            ? Violations(string.Concat(parts))
            : [$"the exception names a statement that the migration does not hold exactly once: {exception.Statement}"];
    }

    private static ClusterMigrationsDbContext Context() => new ClusterMigrationsDesignTimeFactory().CreateDbContext([]);

    private static List<(string Id, string Sql)> ScriptsAfterTheBaseline()
    {
        using var context = Context();
        var migrator = context.GetService<IMigrator>();
        var ids = context.GetService<IMigrationsAssembly>().Migrations.Keys.Order(StringComparer.Ordinal).ToList();
        var scripts = new List<(string, string)>();
        for (var i = 1; i < ids.Count; i++)
        {
            scripts.Add((ids[i], migrator.GenerateScript(fromMigration: ids[i - 1], toMigration: ids[i], MigrationsSqlGenerationOptions.Default)));
        }

        return scripts;
    }

    [Fact]
    public void Every_migration_after_the_baseline_only_adds()
    {
        var violations = ScriptsAfterTheBaseline()
            .SelectMany(m => ViolationsOf(m.Id, m.Sql).Select(v => $"{m.Id}: {v}"))
            .ToList();

        Assert.True(violations.Count == 0, "A migration that is not additive breaks the build one release older:\n" + string.Join('\n', violations));
    }

    [Fact]
    public void The_scan_reads_the_SQL_EF_Core_generates_without_a_database()
    {
        using var context = Context();
        var migrator = context.GetService<IMigrator>();
        var baseline = context.GetService<IMigrationsAssembly>().Migrations.Keys.Order(StringComparer.Ordinal).First();

        var script = migrator.GenerateScript(fromMigration: null, toMigration: baseline, MigrationsSqlGenerationOptions.Default);

        Assert.Contains("CREATE TABLE reeve_cluster.member", script, StringComparison.Ordinal);
        Assert.Contains("__ClusterMigrationsHistory", script, StringComparison.Ordinal);
    }

    [Fact]
    public void An_exception_names_a_migration_the_build_ships()
    {
        using var context = Context();
        var ids = context.GetService<IMigrationsAssembly>().Migrations.Keys.ToHashSet(StringComparer.Ordinal);

        Assert.All(Exceptions, e => Assert.Contains(e.Key, ids));
        Assert.All(Exceptions, e => Assert.False(string.IsNullOrWhiteSpace(e.Value.Reason)));
    }

    [Fact]
    public void An_exception_covers_its_statement_and_nothing_else_in_the_migration()
    {
        const string id = "20990101000000_Made_up";
        const string covered = "DROP TABLE IF EXISTS reeve_cluster.old_table;";
        var exceptions = new Dictionary<string, (string Statement, string Reason)> { [id] = (covered, "a test entry") };

        Assert.Contains(Violations(covered), v => v.Contains("drops", StringComparison.OrdinalIgnoreCase));
        Assert.Empty(ViolationsOf(exceptions, id, covered));

        Assert.NotEmpty(ViolationsOf(exceptions, id, covered + "\nDROP TABLE reeve_cluster.member;"));
        Assert.NotEmpty(ViolationsOf(exceptions, id, covered + "\n" + covered));
        Assert.NotEmpty(ViolationsOf(exceptions, id, "SELECT 1;"));
        Assert.NotEmpty(ViolationsOf(exceptions, "20991231000000_Other", covered));
    }

    [Theory]
    [InlineData("DROP TABLE reeve_cluster.member;", "drops")]
    [InlineData("ALTER TABLE reeve_cluster.cluster DROP COLUMN horizon_seq;", "drops")]
    [InlineData("drop index reeve_cluster.command_active;", "drops")]
    [InlineData("ALTER TABLE reeve_cluster.member RENAME COLUMN version TO build;", "renames")]
    [InlineData("ALTER TABLE reeve_cluster.member RENAME TO node;", "renames")]
    [InlineData("ALTER TABLE reeve_cluster.member ALTER COLUMN version TYPE varchar(20);", "retypes")]
    [InlineData("ALTER TABLE reeve_cluster.member ALTER COLUMN version SET NOT NULL;", "sets NOT NULL")]
    [InlineData("ALTER TABLE reeve_cluster.member ADD COLUMN zone text NOT NULL;", "NOT NULL column without a DEFAULT")]
    [InlineData("CREATE OR REPLACE FUNCTION reeve_cluster.draw_token() RETURNS bigint AS $$ SELECT 1 $$ LANGUAGE sql;", "replaces")]
    [InlineData("TRUNCATE reeve_cluster.change_log;", "truncates")]
    [InlineData("ALTER TABLE reeve_cluster.member DROP zone;", "drops")]
    [InlineData("ALTER TABLE reeve_cluster.member DROP CONSTRAINT member_pkey;", "drops")]
    [InlineData("DROP MATERIALIZED VIEW reeve_cluster.v;", "drops")]
    [InlineData("DROP PROCEDURE reeve_cluster.p();", "drops")]
    [InlineData("DROP EXTENSION pgcrypto;", "drops")]
    [InlineData("DROP POLICY p ON reeve_cluster.member;", "drops")]
    [InlineData("ALTER TABLE reeve_cluster.member ALTER version TYPE varchar(20);", "retypes")]
    [InlineData("ALTER TABLE reeve_cluster.member ALTER version SET DATA TYPE varchar(20);", "retypes")]
    [InlineData("ALTER TABLE reeve_cluster.member ADD zone int NOT NULL;", "NOT NULL column without a DEFAULT")]
    [InlineData("ALTER TABLE reeve_cluster.member ADD COLUMN price numeric(10,2) NOT NULL;", "NOT NULL column without a DEFAULT")]
    [InlineData("ALTER TABLE reeve_cluster.member ADD COLUMN a text, ADD COLUMN b numeric(10,2) NOT NULL;", "NOT NULL column without a DEFAULT")]
    [InlineData("ALTER TABLE reeve_cluster.member ADD CONSTRAINT zone_ok CHECK (zone > 0);", "constraint")]
    [InlineData("ALTER TABLE reeve_cluster.member ADD UNIQUE (zone);", "constraint")]
    [InlineData("ALTER TABLE reeve_cluster.member ADD PRIMARY KEY (zone);", "constraint")]
    [InlineData("CREATE UNIQUE INDEX member_zone ON reeve_cluster.member (zone);", "unique index")]
    [InlineData("CREATE TRIGGER t BEFORE INSERT ON reeve_cluster.member FOR EACH ROW EXECUTE FUNCTION reeve_cluster.f();", "trigger")]
    [InlineData("UPDATE reeve_cluster.cluster SET protocol_floor = 2;", "rewrites rows")]
    [InlineData("DELETE FROM reeve_cluster.member;", "rewrites rows")]
    [InlineData("DO $$ BEGIN DROP TABLE reeve_cluster.member; END $$;", "drops")]
    [InlineData("DO $do$ BEGIN ALTER TABLE reeve_cluster.cluster DROP COLUMN horizon_seq; END $do$;", "drops")]
    [InlineData("DO $$ BEGIN ALTER TABLE reeve_cluster.member ADD COLUMN zone text; END $$;", "a hot table")]
    [InlineData("DO $mig$ BEGIN EXECUTE 'x'; ALTER TABLE IF EXISTS reeve_cluster.change_log ADD COLUMN y int; END $mig$ LANGUAGE plpgsql;", "a hot table")]
    [InlineData("CREATE FUNCTION reeve_cluster.f() RETURNS void LANGUAGE sql AS $f$ SELECT 1 $f$;\nDO $$ BEGIN DROP INDEX reeve_cluster.command_active; END $$;", "drops")]
    [InlineData("ALTER TABLE reeve_cluster.member ADD COLUMN zone text;", "a hot table")]
    [InlineData("ALTER TABLE reeve_cluster.\"cluster\" ADD COLUMN x int;", "a hot table")]
    [InlineData("ALTER TABLE IF EXISTS ONLY reeve_cluster.change_log ADD COLUMN y int;", "a hot table")]
    [InlineData("CREATE INDEX member_zone ON reeve_cluster.member (zone);", "a hot table")]
    [InlineData("CREATE INDEX IF NOT EXISTS \"IX_change_log_x\" ON ONLY reeve_cluster.\"change_log\" (x);", "a hot table")]
    public void The_scan_finds_what_breaks_an_older_build(string sql, string fragment)
    {
        var found = Violations(sql);

        Assert.Contains(found, v => v.Contains(fragment, StringComparison.OrdinalIgnoreCase));
    }

    [Theory]
    [InlineData("ALTER TABLE reeve_cluster.t ADD COLUMN floor_moved_by text, ADD COLUMN floor_moved_at timestamptz;")]
    [InlineData("ALTER TABLE reeve_cluster.t ADD COLUMN zone text NOT NULL DEFAULT 'a';")]
    [InlineData("CREATE TABLE reeve_cluster.t (id bigint PRIMARY KEY, n text NOT NULL);")]
    [InlineData("CREATE INDEX t_n ON reeve_cluster.t (n);")]
    [InlineData("CREATE FUNCTION reeve_cluster.f2() RETURNS int LANGUAGE sql AS $$ SELECT 1 $$;")]
    [InlineData("CREATE FUNCTION reeve_cluster.f4() RETURNS void LANGUAGE plpgsql AS $fn$ BEGIN DROP TABLE reeve_cluster.member; END $fn$;")]
    [InlineData("DO $$ BEGIN CREATE TABLE IF NOT EXISTS reeve_cluster.t (id bigint PRIMARY KEY); END $$;")]
    [InlineData("-- DROP TABLE in a comment\nCREATE SEQUENCE reeve_cluster.s CACHE 1;")]
    [InlineData("ALTER TABLE reeve_cluster.t ADD COLUMN price numeric(10,2) NOT NULL DEFAULT 0;")]
    [InlineData("ALTER TABLE reeve_cluster.t ADD COLUMN IF NOT EXISTS a text, ADD b numeric(10,2) NOT NULL DEFAULT 0, ADD c int;")]
    [InlineData("CREATE FUNCTION reeve_cluster.f3() RETURNS void LANGUAGE sql AS $$ UPDATE reeve_cluster.cluster SET protocol_floor = 2; DELETE FROM reeve_cluster.t; $$;")]
    public void The_scan_passes_what_only_adds(string sql) => Assert.Empty(Violations(sql));

    [Fact]
    public void The_ask_records_formats_of_0_4_still_decode_and_are_still_what_the_writer_writes()
    {
        AssertGolden([1, (byte)'P'], AskCell.Pending);
        AssertGolden([1, (byte)'T'], AskCell.Taken);
        AssertGolden([1, (byte)'W'], AskCell.Withdrawn);
        AssertGolden([1, (byte)'D', (byte)DeclineReason.Absent], AskCell.Declined(DeclineReason.Absent));
        AssertGolden(
            [1, (byte)'A', (byte)AskAnswerKind.Response, 1, 0, 0, 0, (byte)'r', 0],
            AskCell.Answered(new AskAnswer(AskAnswerKind.Response, new byte[] { (byte)'r' }, null)));
    }

    private static readonly byte[] RequestIdBytes = [0x33, 0x22, 0x11, 0x00, 0x55, 0x44, 0x77, 0x66, 0x88, 0x99, 0xaa, 0xbb, 0xcc, 0xdd, 0xee, 0xff];
    private static readonly byte[] CallerBytes = [0xcc, 0xdd, 0xee, 0xff, 0xaa, 0xbb, 0x88, 0x99, 0x77, 0x66, 0x55, 0x44, 0x33, 0x22, 0x11, 0x00];
    private static readonly Guid RequestId = Guid.Parse("00112233-4455-6677-8899-aabbccddeeff");
    private static readonly Guid Caller = Guid.Parse("ffeeddcc-bbaa-9988-7766-554433221100");

    [Fact]
    public void The_ask_request_message_of_0_4_still_decodes_and_is_still_what_the_writer_writes()
    {
        byte[] window = [0xc4, 0x09, 0, 0, 0, 0, 0, 0];
        byte[] payload = [3, 0, 0, 0, 1, 2, 3];
        byte[] member = [1, (byte)'Q', .. RequestIdBytes, .. CallerBytes, .. window, 1, .. payload];
        byte[] keyed =
        [
            1, (byte)'Q', .. RequestIdBytes, .. CallerBytes, .. window, 0,
            9, .. "scheduler"u8, 2, (byte)'k', (byte)'1', 42, 0, 0, 0, 0, 0, 0, 0, .. payload,
        ];

        var memberMessage = new AskRequestMessage(RequestId, Caller, TimeSpan.FromMilliseconds(2500), new AskDelivery(null, null, null, new byte[] { 1, 2, 3 }));
        var keyedMessage = new AskRequestMessage(RequestId, Caller, TimeSpan.FromMilliseconds(2500), new AskDelivery("scheduler", "k1", 42, new byte[] { 1, 2, 3 }));
        Assert.Equal(member, memberMessage.Encode());
        Assert.Equal(keyed, keyedMessage.Encode());

        var memberBack = AskRequestMessage.TryDecode(member);
        var keyedBack = AskRequestMessage.TryDecode(keyed);
        Assert.NotNull(memberBack);
        Assert.NotNull(keyedBack);
        Assert.Equal((RequestId, Caller, TimeSpan.FromMilliseconds(2500)), (memberBack.RequestId, memberBack.CallerProcess, memberBack.Window));
        Assert.True(memberBack.Delivery.IsMember);
        Assert.Equal(new byte[] { 1, 2, 3 }, memberBack.Delivery.Payload.ToArray());
        Assert.Equal(("scheduler", "k1", 42L), (keyedBack.Delivery.Kind, keyedBack.Delivery.Key, keyedBack.Delivery.Token));
    }

    [Fact]
    public void The_ask_reply_message_is_the_sixteen_bytes_of_the_request_id_and_has_no_version()
    {
        Assert.Equal(RequestIdBytes, RequestId.ToByteArray());
        Assert.Equal(RequestId, new Guid(RequestIdBytes));
    }

    [Fact]
    public void A_reader_ignores_an_ask_record_of_a_version_it_does_not_know()
    {
        Assert.Null(AskCellCodec.Decode(new byte[] { 2, (byte)'P' }));
    }

    [Fact]
    public void The_redis_wake_up_of_0_4_still_decodes_and_is_still_what_the_writer_writes()
    {
        Assert.Equal("Applied|12|r1|", RedisWakeupChannel.Encode(new Wakeup(WakeupKind.Applied, 12), "r1"));
        Assert.Equal("OwnerNudge|0|r1|key", RedisWakeupChannel.Encode(new Wakeup(WakeupKind.OwnerNudge, 0, "key"), "r1"));

        Assert.True(RedisWakeupChannel.TryDecode("Applied|12|r1|", out var applied, out var origin));
        Assert.Equal((WakeupKind.Applied, 12L, (string?)null, "r1"), (applied.Kind, applied.Seq, applied.Target, origin));
        Assert.True(RedisWakeupChannel.TryDecode("OwnerNudge|0|r1|key", out var nudge, out _));
        Assert.Equal("key", nudge.Target);
    }

    [Fact]
    public void A_field_appended_to_the_redis_wake_up_is_folded_into_the_target_so_a_change_to_it_is_a_new_kind()
    {
        Assert.True(RedisWakeupChannel.TryDecode("Applied|12|r1|t|extra", out var wakeup, out var origin));
        Assert.Equal((WakeupKind.Applied, 12L, "t|extra", "r1"), (wakeup.Kind, wakeup.Seq, wakeup.Target, origin));
    }

    [Fact]
    public void A_reader_ignores_a_wake_up_kind_it_does_not_know()
    {
        Assert.False(RedisWakeupChannel.TryDecode("FromTheFuture|1|r1|", out _, out _));
    }

    [Fact]
    public void The_redis_key_spaces_and_channel_names_of_0_4_are_still_what_the_writers_build()
    {
        Assert.Equal("{reeve:claim:k}", RedisPrimitiveKeys.ClaimHashTag("reeve:", "k"));
        Assert.Equal("{reeve:flight:k}", RedisPrimitiveKeys.FlightHashTag("reeve:", "k"));
        Assert.Equal("{reeve:budget:k}", RedisPrimitiveKeys.BudgetHashTag("reeve:", "k"));
        Assert.Equal("{reeve:counter:k}", RedisPrimitiveKeys.CounterHashTag("reeve:", "k"));
        Assert.Equal("{reeve:slot:k}", RedisPrimitiveKeys.SlotHashTag("reeve:", "k"));
        Assert.Equal("{reeve:owned:consumer:k}", TenureFence.HashTagOf("reeve:", "consumer", "k"));
        Assert.Equal("reeve:wakeup", RedisWakeupChannel.ChannelName("reeve:"));

        var process = Guid.Parse("00112233-4455-6677-8899-aabbccddeeff");
        Assert.Equal("ask:q:00112233445566778899aabbccddeeff:r1", AskAddress.RequestChannel("r1", process));
        Assert.Equal("ask:r:00112233445566778899aabbccddeeff", AskAddress.ReplyChannel(process));
        Assert.Equal("ask:c:00112233445566778899aabbccddeeff", AskAddress.Record(process));
    }

    private static void AssertGolden(byte[] bytes, AskCell cell)
    {
        Assert.Equal(bytes, AskCellCodec.Encode(cell));
        var decoded = AskCellCodec.Decode(bytes);
        Assert.NotNull(decoded);
        Assert.Equal(cell.Kind, decoded.Kind);
        Assert.Equal(cell.Reason, decoded.Reason);
    }

    private static List<string> Violations(string sql)
    {
        var code = FunctionBody().Replace(LineComment().Replace(sql, string.Empty), m => m.Groups["head"].Value + "$$ $$");
        var found = new List<string>();
        if (HotTableAlter().IsMatch(code) || HotTableIndex().IsMatch(code))
        {
            found.Add("alters or indexes a hot table (member, cluster, change_log) before the runner sets lock_timeout with retry");
        }

        if (Drop().IsMatch(code))
        {
            found.Add("drops an object");
        }

        if (Rename().IsMatch(code))
        {
            found.Add("renames an object");
        }

        if (Retype().IsMatch(code))
        {
            found.Add("retypes a column");
        }

        if (SetNotNull().IsMatch(code))
        {
            found.Add("sets NOT NULL on a column that exists");
        }

        foreach (Match add in AddClause().Matches(code))
        {
            var clause = ClauseFrom(code, add.Index);
            if (Constraint().IsMatch(clause))
            {
                found.Add("adds a constraint to a table that exists: " + clause.Trim());
            }
            else if (clause.Contains("NOT NULL", StringComparison.OrdinalIgnoreCase) && !clause.Contains("DEFAULT", StringComparison.OrdinalIgnoreCase))
            {
                found.Add("adds a NOT NULL column without a DEFAULT: " + clause.Trim());
            }
        }

        if (UniqueIndex().IsMatch(code))
        {
            found.Add("creates a unique index");
        }

        if (Trigger().IsMatch(code))
        {
            found.Add("creates a trigger");
        }

        if (Dml().IsMatch(code))
        {
            found.Add("rewrites rows (UPDATE or DELETE)");
        }

        if (OrReplace().IsMatch(code))
        {
            found.Add("replaces an object that exists (CREATE OR REPLACE); a changed function takes a new name");
        }

        if (Truncate().IsMatch(code))
        {
            found.Add("truncates a table");
        }

        return found;
    }

    private static string ClauseFrom(string code, int start)
    {
        var depth = 0;
        for (var i = start; i < code.Length; i++)
        {
            switch (code[i])
            {
                case '(':
                    depth++;
                    break;
                case ')':
                    depth--;
                    break;
                case ',' when depth == 0:
                case ';' when depth == 0:
                    return code[start..i];
            }
        }

        return code[start..];
    }

    [GeneratedRegex(@"--[^\n]*", RegexOptions.CultureInvariant)]
    private static partial Regex LineComment();

    [GeneratedRegex(@"(?<head>\bCREATE\s+(?:OR\s+REPLACE\s+)?(?:FUNCTION|PROCEDURE)\b[^$;]*?)\$(?<tag>\w*)\$.*?\$\k<tag>\$", RegexOptions.IgnoreCase | RegexOptions.Singleline | RegexOptions.CultureInvariant)]
    private static partial Regex FunctionBody();

    [GeneratedRegex(@"\bDROP\b", RegexOptions.IgnoreCase | RegexOptions.CultureInvariant)]
    private static partial Regex Drop();

    [GeneratedRegex(@"\bALTER\s+TABLE\s+(?:IF\s+EXISTS\s+)?(?:ONLY\s+)?(?:""?reeve_cluster""?\s*\.\s*)?""?(?:member|cluster|change_log)""?(?=\s|;|$)", RegexOptions.IgnoreCase | RegexOptions.CultureInvariant)]
    private static partial Regex HotTableAlter();

    [GeneratedRegex(@"\bCREATE\s+(?:UNIQUE\s+)?INDEX\s+(?:CONCURRENTLY\s+)?(?:IF\s+NOT\s+EXISTS\s+)?(?:""?\w+""?\s+)?ON\s+(?:ONLY\s+)?(?:""?reeve_cluster""?\s*\.\s*)?""?(?:member|cluster|change_log)""?(?=\s|\(|;|$)", RegexOptions.IgnoreCase | RegexOptions.CultureInvariant)]
    private static partial Regex HotTableIndex();

    [GeneratedRegex(@"\bRENAME\b", RegexOptions.IgnoreCase | RegexOptions.CultureInvariant)]
    private static partial Regex Rename();

    [GeneratedRegex(@"\bALTER\s+(COLUMN\s+)?\w+\s+(SET\s+DATA\s+)?TYPE\b", RegexOptions.IgnoreCase | RegexOptions.CultureInvariant)]
    private static partial Regex Retype();

    [GeneratedRegex(@"\bSET\s+NOT\s+NULL\b", RegexOptions.IgnoreCase | RegexOptions.CultureInvariant)]
    private static partial Regex SetNotNull();

    [GeneratedRegex(@"\bADD\b(?!\s+VALUE\b)", RegexOptions.IgnoreCase | RegexOptions.CultureInvariant)]
    private static partial Regex AddClause();

    [GeneratedRegex(@"^ADD\s+(CONSTRAINT|PRIMARY|UNIQUE|CHECK|FOREIGN|EXCLUDE)\b", RegexOptions.IgnoreCase | RegexOptions.CultureInvariant)]
    private static partial Regex Constraint();

    [GeneratedRegex(@"\bCREATE\s+UNIQUE\s+INDEX\b", RegexOptions.IgnoreCase | RegexOptions.CultureInvariant)]
    private static partial Regex UniqueIndex();

    [GeneratedRegex(@"\bCREATE\s+(OR\s+REPLACE\s+)?(CONSTRAINT\s+)?TRIGGER\b", RegexOptions.IgnoreCase | RegexOptions.CultureInvariant)]
    private static partial Regex Trigger();

    [GeneratedRegex(@"\b(UPDATE\s+[\w.""]+\s+SET|DELETE\s+FROM)\b", RegexOptions.IgnoreCase | RegexOptions.CultureInvariant)]
    private static partial Regex Dml();

    [GeneratedRegex(@"\bCREATE\s+OR\s+REPLACE\b", RegexOptions.IgnoreCase | RegexOptions.CultureInvariant)]
    private static partial Regex OrReplace();

    [GeneratedRegex(@"\bTRUNCATE\b", RegexOptions.IgnoreCase | RegexOptions.CultureInvariant)]
    private static partial Regex Truncate();
}
