using System.Text.RegularExpressions;
using Reeve.Cluster.Claims;
using Reeve.Cluster.Commands;
using Reeve.Cluster.Owned;
using Reeve.Cluster.Membership;
using Reeve.Cluster.Slots;

namespace Unit.Log;

public sealed class TransactionShapeTests
{
    private static readonly Dictionary<string, string[]> ModelShapes = new(StringComparer.Ordinal)
    {
        ["append"] = ["biz", "CL"],
        ["activation"] = ["CL", "member"],
        ["floorRaise"] = ["CL", "cluster"],
        ["renew"] = ["member"],
        ["fence3"] = ["member"],
        ["cursor"] = ["member"],
        ["drainWrite"] = ["member"],
        ["retention"] = ["cluster"],

        ["ackRecord"] = ["ack"],
        ["skipOpen"] = ["ack", "skip"],
        ["skipClose"] = ["skip", "CL"],

        ["owned"] = ["owned"],
        ["fencedTx"] = ["owned", "biz", "CL"],
        ["placer"] = ["placer", "owned"],
        ["relRequest"] = ["placer", "relreq"],
        ["release"] = ["owned", "relreq"],
        ["join"] = ["member"],

        ["claimGrant"] = ["claim"],
        ["claimRelease"] = ["claim"],
        ["onceRecord"] = ["claim"],
        ["claimRetake"] = ["claim"],
        ["claimResult"] = ["claim"],
        ["claimRetention"] = ["claim"],
        ["claimFenced"] = ["claim", "biz", "CL"],
        ["claimInUnit"] = ["owned", "claim", "biz", "CL"],
        ["budget"] = ["budget"],
        ["counter"] = ["counter"],

        ["slotAcquire"] = ["slotkey", "slot"],
        ["slotRenew"] = ["slot"],
        ["slotRelease"] = ["slot"],
        ["slotFenced"] = ["slot", "biz", "CL"],
        ["cmdInsert"] = ["command"],
        ["cmdResend"] = ["command"],
        ["cmdSweep"] = ["command"],
        ["cmdTake"] = ["owned", "command"],
        ["cmdComplete"] = ["owned", "command"],
        ["cmdRetake"] = ["owned", "command"],
        ["cmdInDoubt"] = ["owned", "command"],
    };

    private static readonly string[] RowsNotAfterCl = ["member", "owned", "relreq", "cluster", "ack"];

    [Fact]
    public void I4_the_lock_graph_of_every_shape_is_acyclic()
    {
        var edges = Edges(ModelShapes);
        foreach (var node in edges.Keys)
        {
            Assert.False(Reaches(edges, node, node), $"the lock graph has a cycle through {node}");
        }
    }

    [Fact]
    public void I4_no_transaction_that_appends_writes_member_owned_release_request_cluster_or_change_ack_after_the_change_log_lock()
    {
        foreach (var (name, locks) in ModelShapes)
        {
            var cl = Array.IndexOf(locks, "CL");
            if (cl < 0 || name is "activation" or "floorRaise")
            {
                continue;
            }

            Assert.DoesNotContain(locks.Skip(cl + 1), RowsNotAfterCl.Contains);
        }
    }

    [Fact]
    public void I4_the_join_takes_only_its_member_row()
    {
        Assert.DoesNotContain("pg_advisory", MemberStore.JoinSql, StringComparison.Ordinal);
        Assert.DoesNotContain("setval", MemberStore.JoinSql, StringComparison.Ordinal);
        var relations = Regex.Matches(MemberStore.JoinSql, @"reeve_cluster\.(\w+)").Select(m => m.Groups[1].Value).ToHashSet(StringComparer.Ordinal);
        Assert.Equal(["member", "member_epoch"], relations.Order(StringComparer.Ordinal));
        Assert.Equal(["member"], ModelShapes["join"]);
    }

    [Theory]
    [InlineData("claimGrant")]
    [InlineData("claimRelease")]
    [InlineData("claimRetention")]
    public void I4_the_claim_statements_take_the_claim_row_and_nothing_else(string shape)
    {
        var sql = shape switch
        {
            "claimGrant" => new[] { ClaimStore.InsertSql, ClaimStore.LockSql, ClaimStore.TakeoverSql },
            "claimRelease" => [ClaimStore.ReleaseSql],
            _ => [RetentionStore.ClaimsSql],
        };
        Assert.Equal(["claim"], ModelShapes[shape]);
        foreach (var statement in sql)
        {
            var relations = Regex.Matches(statement, @"reeve_cluster\.(\w+)").Select(m => m.Groups[1].Value).ToHashSet(StringComparer.Ordinal);
            Assert.Contains("claim", relations);
            Assert.Subset(new HashSet<string>(["claim", "member"], StringComparer.Ordinal), relations);

            Assert.DoesNotMatch(@"\bFOR\s+(UPDATE|SHARE|KEY\s+SHARE)\b", statement);
            if (statement != ClaimStore.LockSql)
            {
                Assert.DoesNotMatch(@"\bFOR\s+NO\s+KEY\s+UPDATE\b", statement);
            }

            Assert.DoesNotContain("pg_advisory", statement, StringComparison.Ordinal);
            Assert.DoesNotContain("draw_token", statement, StringComparison.Ordinal);
            Assert.DoesNotContain("change_log", statement, StringComparison.Ordinal);
        }
    }

    [Theory]
    [InlineData("onceRecord")]
    [InlineData("claimRetake")]
    public void I4_the_once_statements_take_the_claim_row_and_nothing_else(string shape)
    {
        var sql = shape switch
        {
            "onceRecord" => new[] { OnceStore.InsertSql, OnceStore.ReadSql, OnceStore.CompleteSql },
            _ => [OnceStore.ListSql, OnceStore.RetakeLockSql, OnceStore.RetakeSql],
        };
        Assert.Equal(["claim"], ModelShapes[shape]);
        foreach (var statement in sql)
        {
            Assert.NotEmpty(statement);
            var relations = Regex.Matches(statement, @"reeve_cluster\.(\w+)").Select(m => m.Groups[1].Value).ToHashSet(StringComparer.Ordinal);
            Assert.Contains("claim", relations);
            Assert.Subset(new HashSet<string>(["claim", "member"], StringComparer.Ordinal), relations);
            Assert.DoesNotMatch(@"\bFOR\s+(UPDATE|SHARE|KEY\s+SHARE)\b", statement);
            if (statement != OnceStore.RetakeLockSql)
            {
                Assert.DoesNotMatch(@"\bFOR\s+NO\s+KEY\s+UPDATE\b", statement);
            }

            Assert.DoesNotContain("pg_advisory", statement, StringComparison.Ordinal);
            Assert.DoesNotContain("draw_token", statement, StringComparison.Ordinal);
            Assert.DoesNotContain("change_log", statement, StringComparison.Ordinal);
        }

        if (shape == "claimRetake")
        {
            Assert.Contains("completed_at IS NULL", OnceStore.RetakeSql, StringComparison.Ordinal);
            Assert.Contains("once_mode", OnceStore.RetakeSql, StringComparison.Ordinal);
            Assert.Contains("NOT (", OnceStore.RetakeSql, StringComparison.Ordinal);
            Assert.Contains("c.token = @expected", OnceStore.RetakeSql, StringComparison.Ordinal);
        }
    }

    [Fact]
    public void I4_the_result_write_is_one_conditional_update_of_the_claim_row_under_claim_is()
    {
        var sql = FlightStore.ResultSql;
        Assert.Equal(["claim"], ModelShapes["claimResult"]);
        Assert.StartsWith("UPDATE reeve_cluster.claim", sql.TrimStart(), StringComparison.Ordinal);
        Assert.Contains("reeve_cluster.claim_is(@key, @token, @timeline)", sql, StringComparison.Ordinal);
        Assert.Contains("c.token = @token", sql, StringComparison.Ordinal);
        Assert.Contains("result_expires_at", sql, StringComparison.Ordinal);
        Assert.DoesNotMatch(@"\bFOR\s+(NO\s+KEY\s+UPDATE|UPDATE|SHARE|KEY\s+SHARE)\b", sql);
        var relations = Regex.Matches(sql, @"reeve_cluster\.(\w+)").Select(m => m.Groups[1].Value).ToHashSet(StringComparer.Ordinal);
        Assert.Equal(["claim", "claim_is"], relations.Order(StringComparer.Ordinal));
        Assert.DoesNotContain("pg_advisory", sql, StringComparison.Ordinal);
        Assert.DoesNotContain("change_log", sql, StringComparison.Ordinal);
    }

    [Theory]
    [InlineData("claimGrant")]
    [InlineData("claimRelease")]
    public void I4_the_single_flight_claim_statements_take_the_claim_row_and_nothing_else(string shape)
    {
        var sql = shape == "claimGrant" ? new[] { FlightStore.InsertSql, FlightStore.ReadSql, FlightStore.TakeoverSql } : [FlightStore.ReleaseSql];
        Assert.Equal(["claim"], ModelShapes[shape]);
        foreach (var statement in sql)
        {
            Assert.NotEmpty(statement);
            var relations = Regex.Matches(statement, @"reeve_cluster\.(\w+)").Select(m => m.Groups[1].Value).ToHashSet(StringComparer.Ordinal);
            Assert.Contains("claim", relations);
            Assert.Subset(new HashSet<string>(["claim", "member"], StringComparer.Ordinal), relations);
            Assert.DoesNotMatch(@"\bFOR\s+(NO\s+KEY\s+UPDATE|UPDATE|SHARE|KEY\s+SHARE)\b", statement);
            Assert.DoesNotContain("pg_advisory", statement, StringComparison.Ordinal);
            Assert.DoesNotContain("change_log", statement, StringComparison.Ordinal);
        }

        if (shape == "claimGrant")
        {
            Assert.Contains("c.token = @expected", FlightStore.TakeoverSql, StringComparison.Ordinal);
            Assert.Contains("NOT (", FlightStore.TakeoverSql, StringComparison.Ordinal);
            Assert.Contains("result_expires_at", FlightStore.TakeoverSql, StringComparison.Ordinal);
        }
        else
        {
            Assert.Contains("result IS NULL", FlightStore.ReleaseSql, StringComparison.Ordinal);
            Assert.Contains("token = @token", FlightStore.ReleaseSql, StringComparison.Ordinal);
        }
    }

    [Fact]
    public void I4_the_budget_consume_takes_the_budget_rows_and_nothing_else()
    {
        var sql = BudgetStore.ConsumeSql;
        Assert.Equal(["budget"], ModelShapes["budget"]);
        var relations = Regex.Matches(sql, @"reeve_cluster\.(\w+)").Select(m => m.Groups[1].Value).ToHashSet(StringComparer.Ordinal);
        Assert.Equal(["budget", "budget_id"], relations.Order(StringComparer.Ordinal));
        Assert.Contains("clock_timestamp()", sql, StringComparison.Ordinal);
        Assert.Contains("ON CONFLICT", sql, StringComparison.Ordinal);
        Assert.DoesNotMatch(@"\bFOR\s+(NO\s+KEY\s+UPDATE|UPDATE|SHARE|KEY\s+SHARE)\b", sql);
        Assert.DoesNotContain("pg_advisory", sql, StringComparison.Ordinal);
        Assert.DoesNotContain("change_log", sql, StringComparison.Ordinal);
    }

    [Fact]
    public void I4_the_counter_increment_takes_its_counter_row_and_nothing_else()
    {
        var sql = CounterStore.IncrementSql;
        Assert.Equal(["counter"], ModelShapes["counter"]);
        var relations = Regex.Matches(sql, @"reeve_cluster\.(\w+)").Select(m => m.Groups[1].Value).ToHashSet(StringComparer.Ordinal);
        Assert.Equal(["counter"], relations.Order(StringComparer.Ordinal));
        Assert.Contains("ON CONFLICT", sql, StringComparison.Ordinal);
        Assert.DoesNotMatch(@"\bFOR\s+(NO\s+KEY\s+UPDATE|UPDATE|SHARE|KEY\s+SHARE)\b", sql);
        Assert.DoesNotContain("pg_advisory", sql, StringComparison.Ordinal);
        Assert.DoesNotContain("change_log", sql, StringComparison.Ordinal);
    }

    [Fact]
    public void I4_the_stores_of_single_flight_budgets_and_counters_never_take_a_connection_of_the_caller()
    {
        foreach (var store in new[] { typeof(FlightStore), typeof(BudgetStore), typeof(CounterStore) })
        {
            var parameters = store.GetMethods().SelectMany(m => m.GetParameters()).Select(p => p.ParameterType);
            Assert.DoesNotContain(typeof(Npgsql.NpgsqlConnection), parameters);
            Assert.DoesNotContain(typeof(Npgsql.NpgsqlTransaction), parameters);
        }
    }

    [Fact]
    public void I4_the_slot_acquire_takes_the_keys_advisory_lock_first_and_then_only_slot_rows()
    {
        Assert.Equal(["slotkey", "slot"], ModelShapes["slotAcquire"]);
        Assert.Contains("reeve_cluster.slot_lock(@key, @timeline)", SlotStore.LockSql, StringComparison.Ordinal);
        foreach (var statement in new[] { SlotStore.ReacquireSql, SlotStore.DeleteInvalidSql, SlotStore.InsertSql })
        {
            var relations = Regex.Matches(statement, @"reeve_cluster\.(\w+)").Select(m => m.Groups[1].Value).ToHashSet(StringComparer.Ordinal);
            Assert.Contains("slot", relations);
            Assert.Subset(new HashSet<string>(["slot", "member"], StringComparer.Ordinal), relations);
            Assert.DoesNotMatch(@"\bFOR\s+(NO\s+KEY\s+UPDATE|UPDATE|SHARE|KEY\s+SHARE)\b", statement);
            Assert.DoesNotContain("pg_advisory", statement, StringComparison.Ordinal);
            Assert.DoesNotContain("reeve_cluster.claim", statement, StringComparison.Ordinal);
            Assert.DoesNotContain("change_log", statement, StringComparison.Ordinal);
        }

        Assert.DoesNotMatch(@"\bFOR\s+(NO\s+KEY\s+UPDATE|UPDATE|SHARE|KEY\s+SHARE)\b", SlotStore.LockSql);
        Assert.DoesNotContain("reeve_cluster.slot ", SlotStore.LockSql, StringComparison.Ordinal);
    }

    [Fact]
    public void I35_the_acquires_statements_are_the_specified_ones_in_the_specified_shape()
    {
        Assert.StartsWith("UPDATE reeve_cluster.slot s", SlotStore.ReacquireSql.TrimStart(), StringComparison.Ordinal);
        Assert.Contains("RETURNING s.token", SlotStore.ReacquireSql, StringComparison.Ordinal);
        Assert.StartsWith("DELETE FROM reeve_cluster.slot s", SlotStore.DeleteInvalidSql.TrimStart(), StringComparison.Ordinal);
        Assert.Contains("NOT (", SlotStore.DeleteInvalidSql, StringComparison.Ordinal);

        var countStart = SlotStore.InsertSql.IndexOf("count(*)", StringComparison.Ordinal);
        Assert.True(countStart > 0, "the insert counts the key's rows");
        var count = SlotStore.InsertSql[countStart..];
        Assert.DoesNotContain("expires_at", count, StringComparison.Ordinal);
        Assert.DoesNotContain("member", count, StringComparison.Ordinal);
        Assert.DoesNotContain("slot_valid", count, StringComparison.Ordinal);
        Assert.Contains("< @limit", SlotStore.InsertSql, StringComparison.Ordinal);
        Assert.Contains("ON CONFLICT (key, id) DO NOTHING", SlotStore.InsertSql, StringComparison.Ordinal);

        Assert.Contains(SlotStore.ValidRow, SlotStore.ReacquireSql, StringComparison.Ordinal);
        Assert.Contains(SlotStore.ValidRow, SlotStore.DeleteInvalidSql, StringComparison.Ordinal);
    }

    [Fact]
    public void I35_only_a_delete_removes_a_slot_row_and_the_updates_set_expires_at_and_nothing_else()
    {
        foreach (var update in new[] { SlotStore.ReacquireSql, SlotStore.RenewSql })
        {
            var set = Regex.Match(update, @"\bSET\s+(?<assign>.*?)\s+(WHERE|FROM)\b", RegexOptions.Singleline);
            Assert.True(set.Success);
            Assert.Matches(@"^expires_at\s*=\s*clock_timestamp\(\)\s*\+\s*@ttl$", set.Groups["assign"].Value.Trim());
        }

        Assert.StartsWith("DELETE FROM reeve_cluster.slot", SlotStore.ReleaseSql.TrimStart(), StringComparison.Ordinal);
        Assert.StartsWith("DELETE FROM reeve_cluster.slot", SlotStore.ReleaseAnySql.TrimStart(), StringComparison.Ordinal);
    }

    [Fact]
    public void I4_the_slot_renewal_is_one_conditional_update_of_one_slot_row_under_slot_is()
    {
        Assert.Equal(["slot"], ModelShapes["slotRenew"]);
        Assert.StartsWith("UPDATE reeve_cluster.slot s", SlotStore.RenewSql.TrimStart(), StringComparison.Ordinal);
        Assert.Contains("reeve_cluster.slot_is(@key, @id, @token, @timeline)", SlotStore.RenewSql, StringComparison.Ordinal);
        Assert.Contains("s.token = @token", SlotStore.RenewSql, StringComparison.Ordinal);
        Assert.Contains(SlotStore.ValidRow, SlotStore.RenewSql, StringComparison.Ordinal);
        Assert.DoesNotMatch(@"\bFOR\s+(NO\s+KEY\s+UPDATE|UPDATE|SHARE|KEY\s+SHARE)\b", SlotStore.RenewSql);
        Assert.DoesNotContain("pg_advisory", SlotStore.RenewSql, StringComparison.Ordinal);
    }

    [Fact]
    public void I4_the_holder_read_is_one_select_of_slot_and_member_rows_judged_by_the_slot_validity_and_takes_no_lock()
    {
        var read = SlotStore.HoldersSql;
        Assert.StartsWith("SELECT ", read.TrimStart(), StringComparison.Ordinal);
        var relations = Regex.Matches(read, @"reeve_cluster\.(\w+)").Select(m => m.Groups[1].Value).ToHashSet(StringComparer.Ordinal);
        Assert.True(relations.SetEquals(["slot", "member"]), string.Join(", ", relations));
        Assert.Contains("s.key = @key", read, StringComparison.Ordinal);
        Assert.Contains(SlotStore.ValidRow, read, StringComparison.Ordinal);
        Assert.Contains("ORDER BY s.id COLLATE \"C\"", read, StringComparison.Ordinal);
        Assert.DoesNotMatch(@"\bFOR\s+(NO\s+KEY\s+UPDATE|UPDATE|SHARE|KEY\s+SHARE)\b", read);
        Assert.DoesNotMatch(@"\b(UPDATE|DELETE|INSERT)\b", read);
        Assert.DoesNotContain("pg_advisory", read, StringComparison.Ordinal);
        Assert.DoesNotContain("slot_is", read, StringComparison.Ordinal);
        Assert.DoesNotContain("slot_lock", read, StringComparison.Ordinal);
        Assert.DoesNotContain("token", read, StringComparison.Ordinal);
    }

    [Fact]
    public void I4_the_slot_release_takes_the_slot_row_and_nothing_else_and_judges_no_validity()
    {
        Assert.Equal(["slot"], ModelShapes["slotRelease"]);
        foreach (var statement in new[] { SlotStore.ReleaseSql, SlotStore.ReleaseAnySql })
        {
            var relations = Regex.Matches(statement, @"reeve_cluster\.(\w+)").Select(m => m.Groups[1].Value).ToHashSet(StringComparer.Ordinal);
            Assert.Equal(["slot"], relations);
            Assert.DoesNotMatch(@"\bFOR\s+(NO\s+KEY\s+UPDATE|UPDATE|SHARE|KEY\s+SHARE)\b", statement);
            Assert.DoesNotContain("expires_at", statement, StringComparison.Ordinal);
            Assert.DoesNotContain("pg_advisory", statement, StringComparison.Ordinal);
        }

        Assert.Contains("AND token = @token", SlotStore.ReleaseSql, StringComparison.Ordinal);
        Assert.DoesNotContain("token = @token", SlotStore.ReleaseAnySql, StringComparison.Ordinal);
    }

    [Fact]
    public void I11_the_slot_fenced_transaction_asserts_the_slot_row_FOR_KEY_SHARE_after_the_timeline_check_and_holds_no_advisory_lock()
    {
        Assert.Equal(["slot", "biz", "CL"], ModelShapes["slotFenced"]);
    }

    [Fact]
    public void I4_the_slot_store_never_takes_a_connection_of_the_caller()
    {
        var parameters = typeof(SlotStore).GetMethods().SelectMany(m => m.GetParameters()).Select(p => p.ParameterType);
        Assert.DoesNotContain(typeof(Npgsql.NpgsqlConnection), parameters);
        Assert.DoesNotContain(typeof(Npgsql.NpgsqlTransaction), parameters);
    }

    [Fact]
    public void I4_the_once_store_never_takes_a_connection_of_the_caller()
    {
        var parameters = typeof(OnceStore).GetMethods().SelectMany(m => m.GetParameters()).Select(p => p.ParameterType);
        Assert.DoesNotContain(typeof(Npgsql.NpgsqlConnection), parameters);
        Assert.DoesNotContain(typeof(Npgsql.NpgsqlTransaction), parameters);
    }

    [Theory]
    [InlineData("cmdInsert")]
    [InlineData("cmdResend")]
    public void I4_the_command_insert_and_resend_lock_the_command_row_and_nothing_else(string shape)
    {
        var sql = shape == "cmdInsert" ? CommandStore.InsertSql : CommandStore.ResendSql;
        Assert.Equal(["command"], ModelShapes[shape]);
        Assert.Equal(["command"], Relations(sql));
        Assert.DoesNotMatch(@"\bFOR\s+(NO\s+KEY\s+UPDATE|UPDATE|SHARE|KEY\s+SHARE)\b", sql);
        Assert.DoesNotContain("pg_advisory", sql, StringComparison.Ordinal);
        Assert.DoesNotContain("change_log", sql, StringComparison.Ordinal);
        Assert.DoesNotContain("assert_owner", sql, StringComparison.Ordinal);
        if (shape == "cmdInsert")
        {
            Assert.Contains("ON CONFLICT (kind, key, idempotency_key) DO NOTHING", sql, StringComparison.Ordinal);
        }
        else
        {
            Assert.Contains("c.state = 'expired'", sql, StringComparison.Ordinal);
        }
    }

    [Theory]
    [InlineData("cmdTake")]
    [InlineData("cmdComplete")]
    public void I4_the_command_take_and_completion_are_fenced_transactions_that_lock_owned_then_command(string shape)
    {
        var sql = shape == "cmdTake" ? CommandStore.TakeSql : CommandStore.CompleteSql;
        Assert.Equal(["owned", "command"], ModelShapes[shape]);
        Assert.Contains("assert_owner", FencedTransactions.AssertSql, StringComparison.Ordinal);
        Assert.Equal(["command"], Relations(sql));
        Assert.StartsWith("UPDATE reeve_cluster.command", sql.TrimStart(), StringComparison.Ordinal);
        Assert.DoesNotMatch(@"\bFOR\s+(NO\s+KEY\s+UPDATE|UPDATE|SHARE|KEY\s+SHARE)\b", sql);
        Assert.DoesNotContain("pg_advisory", sql, StringComparison.Ordinal);
        Assert.DoesNotContain("change_log", sql, StringComparison.Ordinal);
        if (shape == "cmdTake")
        {
            Assert.Contains("c.state = 'pending'", sql, StringComparison.Ordinal);
            Assert.Contains("NOT EXISTS", sql, StringComparison.Ordinal);
        }
        else
        {
            Assert.Contains("c.state = 'taken' AND c.taken_token = @token", sql, StringComparison.Ordinal);
        }
    }

    [Theory]
    [InlineData("cmdRetake")]
    [InlineData("cmdInDoubt")]
    public void I4_the_command_retake_and_in_doubt_verdict_are_fenced_transactions_that_lock_owned_then_command(string shape)
    {
        var sql = shape == "cmdRetake" ? CommandStore.RetakeSql : CommandStore.InDoubtSql;
        Assert.Equal(["owned", "command"], ModelShapes[shape]);
        Assert.Equal(["command"], Relations(sql));
        Assert.StartsWith("UPDATE reeve_cluster.command", sql.TrimStart(), StringComparison.Ordinal);
        Assert.DoesNotMatch(@"\bFOR\s+(NO\s+KEY\s+UPDATE|UPDATE|SHARE|KEY\s+SHARE)\b", sql);
        Assert.DoesNotContain("pg_advisory", sql, StringComparison.Ordinal);
        Assert.DoesNotContain("change_log", sql, StringComparison.Ordinal);
        Assert.Contains("c.state = 'taken' AND c.taken_token = @old AND c.taken_token < @token", sql, StringComparison.Ordinal);
        Assert.Contains(shape == "cmdRetake" ? "SET taken_token = @token" : "SET state = 'in_doubt'", sql, StringComparison.Ordinal);
    }

    [Theory]
    [InlineData("expire")]
    [InlineData("retention")]
    public void I4_the_command_sweeps_lock_only_the_command_row_and_the_expire_reads_owned_without_a_lock(string which)
    {
        var sql = which == "expire" ? CommandStore.ExpireSql : CommandStore.RetentionSql;
        Assert.Equal(["command"], ModelShapes["cmdSweep"]);
        Assert.DoesNotMatch(@"\bFOR\s+(NO\s+KEY\s+UPDATE|UPDATE|SHARE|KEY\s+SHARE)\b", sql);
        Assert.DoesNotContain("assert_owner", sql, StringComparison.Ordinal);
        Assert.DoesNotContain("pg_advisory", sql, StringComparison.Ordinal);
        Assert.DoesNotContain("change_log", sql, StringComparison.Ordinal);
        if (which == "expire")
        {
            Assert.StartsWith("UPDATE reeve_cluster.command", sql.TrimStart(), StringComparison.Ordinal);
            Assert.Equal(["command", "member", "owned"], Relations(sql));
            Assert.Contains("c.state = 'pending' AND c.created_at < clock_timestamp() - @ttl", sql, StringComparison.Ordinal);
            Assert.Contains("m.lease_expires_at > clock_timestamp()", sql, StringComparison.Ordinal);
        }
        else
        {
            Assert.StartsWith("DELETE FROM reeve_cluster.command", sql.TrimStart(), StringComparison.Ordinal);
            Assert.Equal(["command"], Relations(sql));
            Assert.Contains("state IN ('done', 'in_doubt', 'expired')", sql, StringComparison.Ordinal);
        }
    }

    [Fact]
    public void I4_the_command_store_never_takes_a_connection_of_the_caller()
    {
        var parameters = typeof(CommandStore).GetMethods().SelectMany(m => m.GetParameters()).Select(p => p.ParameterType);
        Assert.DoesNotContain(typeof(Npgsql.NpgsqlConnection), parameters);
        Assert.DoesNotContain(typeof(Npgsql.NpgsqlTransaction), parameters);
    }

    private static string[] Relations(string sql) =>
        [.. Regex.Matches(sql, @"reeve_cluster\.(\w+)").Select(m => m.Groups[1].Value).ToHashSet(StringComparer.Ordinal).Order(StringComparer.Ordinal)];

    [Fact]
    public void I4_the_claim_read_takes_no_lock_and_the_grant_never_enlists_in_the_callers_transaction()
    {
        Assert.Contains("reeve_cluster.claim", ClaimStore.ReadSql, StringComparison.Ordinal);
        Assert.DoesNotMatch(@"\bFOR\s+(NO\s+KEY\s+UPDATE|UPDATE|SHARE|KEY\s+SHARE)\b", ClaimStore.ReadSql);

        var parameters = typeof(ClaimStore).GetMethods().SelectMany(m => m.GetParameters()).Select(p => p.ParameterType);
        Assert.DoesNotContain(typeof(Npgsql.NpgsqlConnection), parameters);
        Assert.DoesNotContain(typeof(Npgsql.NpgsqlTransaction), parameters);
    }

    [Fact]
    public void I4_the_cycle_check_is_not_vacuous()
    {
        Assert.True(Reaches(Edges(new Dictionary<string, string[]> { ["open"] = ["ack", "CL"], ["late"] = ["CL", "ack"] }), "ack", "ack"));
    }

    private static Dictionary<string, HashSet<string>> Edges(IEnumerable<KeyValuePair<string, string[]>> shapes)
    {
        var edges = new Dictionary<string, HashSet<string>>(StringComparer.Ordinal);
        foreach (var (_, locks) in shapes)
        {
            for (var i = 0; i < locks.Length; i++)
            {
                if (!edges.TryGetValue(locks[i], out var to))
                {
                    edges[locks[i]] = to = new HashSet<string>(StringComparer.Ordinal);
                }

                for (var j = i + 1; j < locks.Length; j++)
                {
                    to.Add(locks[j]);
                }
            }
        }

        return edges;
    }

    private static bool Reaches(Dictionary<string, HashSet<string>> edges, string from, string target)
    {
        var seen = new HashSet<string>(StringComparer.Ordinal);
        var stack = new Stack<string>(edges.GetValueOrDefault(from) ?? []);
        while (stack.TryPop(out var node))
        {
            if (node == target)
            {
                return true;
            }

            if (seen.Add(node))
            {
                foreach (var next in edges.GetValueOrDefault(node) ?? [])
                {
                    stack.Push(next);
                }
            }
        }

        return false;
    }
}
