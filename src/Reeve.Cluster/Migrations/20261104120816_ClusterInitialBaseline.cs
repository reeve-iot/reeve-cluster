using System;
using Microsoft.EntityFrameworkCore.Migrations;
using Npgsql.EntityFrameworkCore.PostgreSQL.Metadata;

#nullable disable

namespace Reeve.Cluster.Migrations
{
    internal partial class ClusterInitialBaseline : Migration
    {
        protected override void Up(MigrationBuilder migrationBuilder)
        {
            migrationBuilder.EnsureSchema(
                name: "reeve_cluster");

            migrationBuilder.CreateSequence(
                name: "change_log_seq",
                schema: "reeve_cluster");

            migrationBuilder.CreateSequence(
                name: "member_epoch",
                schema: "reeve_cluster");

            migrationBuilder.CreateSequence(
                name: "owned_token",
                schema: "reeve_cluster");

            migrationBuilder.CreateTable(
                name: "budget",
                schema: "reeve_cluster",
                columns: table => new
                {
                    key = table.Column<string>(type: "text", nullable: false),
                    window_start = table.Column<DateTimeOffset>(type: "timestamp with time zone", nullable: false),
                    used = table.Column<int>(type: "integer", nullable: false),
                    window_end = table.Column<DateTimeOffset>(type: "timestamp with time zone", nullable: false, defaultValueSql: "'infinity'")
                },
                constraints: table =>
                {
                    table.PrimaryKey("budget_pkey", x => new { x.key, x.window_start });
                });

            migrationBuilder.CreateTable(
                name: "budget_id",
                schema: "reeve_cluster",
                columns: table => new
                {
                    key = table.Column<string>(type: "text", nullable: false),
                    window_start = table.Column<DateTimeOffset>(type: "timestamp with time zone", nullable: false),
                    id = table.Column<string>(type: "text", nullable: false)
                },
                constraints: table =>
                {
                    table.PrimaryKey("budget_id_pkey", x => new { x.key, x.window_start, x.id });
                });

            migrationBuilder.CreateTable(
                name: "change_ack",
                schema: "reeve_cluster",
                columns: table => new
                {
                    id = table.Column<bool>(type: "boolean", nullable: false),
                    acked_seq = table.Column<long>(type: "bigint", nullable: false)
                },
                constraints: table =>
                {
                    table.PrimaryKey("change_ack_pkey", x => x.id);
                    table.CheckConstraint("change_ack_acked_seq_check", "acked_seq >= 0");
                    table.CheckConstraint("change_ack_id_check", "id");
                });

            migrationBuilder.CreateTable(
                name: "change_log",
                schema: "reeve_cluster",
                columns: table => new
                {
                    seq = table.Column<long>(type: "bigint", nullable: false),
                    entry_token = table.Column<Guid>(type: "uuid", nullable: false),
                    xid = table.Column<ulong>(type: "xid8", nullable: false),
                    aggregate_type = table.Column<string>(type: "text", nullable: false),
                    aggregate_key = table.Column<string>(type: "text", nullable: true),
                    created_at = table.Column<DateTimeOffset>(type: "timestamp with time zone", nullable: false, defaultValueSql: "clock_timestamp()")
                },
                constraints: table =>
                {
                    table.PrimaryKey("change_log_pkey", x => x.seq);
                    table.CheckConstraint("change_log_aggregate_type_check", "aggregate_type <> ''");
                });

            migrationBuilder.CreateTable(
                name: "change_skip",
                schema: "reeve_cluster",
                columns: table => new
                {
                    id = table.Column<long>(type: "bigint", nullable: false)
                        .Annotation("Npgsql:ValueGenerationStrategy", NpgsqlValueGenerationStrategy.IdentityAlwaysColumn),
                    aggregate_type = table.Column<string>(type: "text", nullable: false),
                    from_seq = table.Column<long>(type: "bigint", nullable: false),
                    until_seq = table.Column<long>(type: "bigint", nullable: true),
                    reason = table.Column<string>(type: "text", nullable: false),
                    opened_by = table.Column<string>(type: "text", nullable: false),
                    opened_at = table.Column<DateTimeOffset>(type: "timestamp with time zone", nullable: false, defaultValueSql: "clock_timestamp()"),
                    closed_by = table.Column<string>(type: "text", nullable: true),
                    closed_at = table.Column<DateTimeOffset>(type: "timestamp with time zone", nullable: true)
                },
                constraints: table =>
                {
                    table.PrimaryKey("change_skip_pkey", x => x.id);
                    table.CheckConstraint("change_skip_aggregate_type_check", "aggregate_type <> ''");
                    table.CheckConstraint("change_skip_check", "(closed_by IS NULL) = (closed_at IS NULL)");
                    table.CheckConstraint("change_skip_from_seq_check", "from_seq > 0");
                });

            migrationBuilder.CreateTable(
                name: "claim",
                schema: "reeve_cluster",
                columns: table => new
                {
                    key = table.Column<string>(type: "text", nullable: false),
                    holder_replica = table.Column<string>(type: "text", nullable: false),
                    holder_epoch = table.Column<long>(type: "bigint", nullable: false),
                    token = table.Column<Guid>(type: "uuid", nullable: false),
                    expires_at = table.Column<DateTimeOffset>(type: "timestamp with time zone", nullable: false),
                    once_mode = table.Column<string>(type: "text", nullable: true),
                    once_effect = table.Column<string>(type: "text", nullable: true),
                    once_ttl = table.Column<TimeSpan>(type: "interval", nullable: true),
                    completed_at = table.Column<DateTimeOffset>(type: "timestamp with time zone", nullable: true),
                    result = table.Column<byte[]>(type: "bytea", nullable: true),
                    result_expires_at = table.Column<DateTimeOffset>(type: "timestamp with time zone", nullable: true)
                },
                constraints: table =>
                {
                    table.PrimaryKey("claim_pkey", x => x.key);
                    table.CheckConstraint("claim_once_mode_check", "once_mode IN ('at_most_once', 'at_least_once')");
                    table.CheckConstraint("claim_once_shape", "(once_mode IS NULL AND once_effect IS NULL AND once_ttl IS NULL AND completed_at IS NULL) OR (once_mode IS NOT NULL AND once_effect IS NOT NULL AND once_ttl IS NOT NULL)");
                    table.CheckConstraint("claim_result_shape", "(result IS NULL) = (result_expires_at IS NULL)");
                });

            migrationBuilder.CreateTable(
                name: "cluster",
                schema: "reeve_cluster",
                columns: table => new
                {
                    id = table.Column<bool>(type: "boolean", nullable: false, defaultValueSql: "true"),
                    protocol_floor = table.Column<int>(type: "integer", nullable: false, defaultValueSql: "0"),
                    version_floor = table.Column<string>(type: "text", nullable: true),
                    horizon_seq = table.Column<long>(type: "bigint", nullable: false, defaultValueSql: "0"),
                    floor_moved_by = table.Column<string>(type: "text", nullable: true),
                    floor_moved_at = table.Column<DateTimeOffset>(type: "timestamp with time zone", nullable: true)
                },
                constraints: table =>
                {
                    table.PrimaryKey("cluster_pkey", x => x.id);
                    table.CheckConstraint("cluster_horizon_seq_check", "horizon_seq >= 0");
                    table.CheckConstraint("cluster_id_check", "id");
                });

            migrationBuilder.CreateTable(
                name: "command",
                schema: "reeve_cluster",
                columns: table => new
                {
                    id = table.Column<long>(type: "bigint", nullable: false)
                        .Annotation("Npgsql:ValueGenerationStrategy", NpgsqlValueGenerationStrategy.IdentityAlwaysColumn),
                    kind = table.Column<string>(type: "text", nullable: false),
                    key = table.Column<string>(type: "text", nullable: false),
                    idempotency_key = table.Column<string>(type: "text", nullable: false),
                    payload = table.Column<byte[]>(type: "bytea", nullable: false),
                    state = table.Column<string>(type: "text", nullable: false),
                    taken_token = table.Column<long>(type: "bigint", nullable: true),
                    taken_at = table.Column<DateTimeOffset>(type: "timestamp with time zone", nullable: true),
                    result = table.Column<byte[]>(type: "bytea", nullable: true),
                    error = table.Column<string>(type: "text", nullable: true),
                    created_at = table.Column<DateTimeOffset>(type: "timestamp with time zone", nullable: false, defaultValueSql: "now()"),
                    completed_at = table.Column<DateTimeOffset>(type: "timestamp with time zone", nullable: true)
                },
                constraints: table =>
                {
                    table.PrimaryKey("command_pkey", x => x.id);
                    table.UniqueConstraint("command_idempotency", x => new { x.kind, x.key, x.idempotency_key });
                    table.CheckConstraint("command_completed_at_shape", "state NOT IN ('pending', 'taken') OR completed_at IS NULL");
                    table.CheckConstraint("command_error_shape", "error IS NULL OR (state = 'done' AND result IS NULL)");
                    table.CheckConstraint("command_state", "state IN ('pending', 'taken', 'done', 'in_doubt', 'expired')");
                    table.CheckConstraint("command_taken_at_shape", "(taken_token IS NULL) = (taken_at IS NULL)");
                    table.CheckConstraint("command_token_shape", "(state IN ('pending', 'expired')) = (taken_token IS NULL)");
                });

            migrationBuilder.CreateTable(
                name: "counter",
                schema: "reeve_cluster",
                columns: table => new
                {
                    key = table.Column<string>(type: "text", nullable: false),
                    value = table.Column<long>(type: "bigint", nullable: false),
                    updated_at = table.Column<DateTimeOffset>(type: "timestamp with time zone", nullable: false, defaultValueSql: "now()")
                },
                constraints: table =>
                {
                    table.PrimaryKey("counter_pkey", x => x.key);
                });

            migrationBuilder.CreateTable(
                name: "fence_history",
                schema: "reeve_cluster",
                columns: table => new
                {
                    replica_id = table.Column<string>(type: "text", nullable: false),
                    epoch = table.Column<long>(type: "bigint", nullable: false),
                    trigger = table.Column<string>(type: "text", nullable: false),
                    fenced_at = table.Column<DateTimeOffset>(type: "timestamp with time zone", nullable: false, defaultValueSql: "clock_timestamp()"),
                    span = table.Column<TimeSpan>(type: "interval", nullable: false)
                },
                constraints: table =>
                {
                    table.PrimaryKey("fence_history_pkey", x => new { x.replica_id, x.epoch });
                });

            migrationBuilder.CreateTable(
                name: "member",
                schema: "reeve_cluster",
                columns: table => new
                {
                    replica_id = table.Column<string>(type: "text", nullable: false),
                    epoch = table.Column<long>(type: "bigint", nullable: false),
                    state = table.Column<string>(type: "text", nullable: false),
                    lease_expires_at = table.Column<DateTimeOffset>(type: "timestamp with time zone", nullable: false),
                    serving_since = table.Column<DateTimeOffset>(type: "timestamp with time zone", nullable: true),
                    activated_at_seq = table.Column<long>(type: "bigint", nullable: true),
                    applied_seq = table.Column<long>(type: "bigint", nullable: false, defaultValueSql: "0"),
                    applied_token = table.Column<Guid>(type: "uuid", nullable: true),
                    capabilities = table.Column<string[]>(type: "text[]", nullable: false, defaultValueSql: "'{}'"),
                    version = table.Column<string>(type: "text", nullable: false),
                    protocol = table.Column<int>(type: "integer", nullable: false),
                    process_id = table.Column<Guid>(type: "uuid", nullable: false)
                },
                constraints: table =>
                {
                    table.PrimaryKey("member_pkey", x => x.replica_id);
                    table.CheckConstraint("member_state_check", "state IN ('joining', 'active', 'fenced', 'draining')");
                });

            migrationBuilder.CreateTable(
                name: "member_status",
                schema: "reeve_cluster",
                columns: table => new
                {
                    replica_id = table.Column<string>(type: "text", nullable: false),
                    epoch = table.Column<long>(type: "bigint", nullable: false),
                    section = table.Column<string>(type: "text", nullable: false),
                    payload = table.Column<string>(type: "jsonb", nullable: false),
                    updated_at = table.Column<DateTimeOffset>(type: "timestamp with time zone", nullable: false, defaultValueSql: "clock_timestamp()")
                },
                constraints: table =>
                {
                    table.PrimaryKey("member_status_pkey", x => new { x.replica_id, x.epoch, x.section });
                });

            migrationBuilder.CreateTable(
                name: "owned",
                schema: "reeve_cluster",
                columns: table => new
                {
                    kind = table.Column<string>(type: "text", nullable: false),
                    key = table.Column<string>(type: "text", nullable: false),
                    owner_replica = table.Column<string>(type: "text", nullable: true),
                    owner_epoch = table.Column<long>(type: "bigint", nullable: true),
                    token = table.Column<long>(type: "bigint", nullable: false),
                    updated_at = table.Column<DateTimeOffset>(type: "timestamp with time zone", nullable: false, defaultValueSql: "clock_timestamp()")
                },
                constraints: table =>
                {
                    table.PrimaryKey("owned_pkey", x => new { x.kind, x.key });
                    table.CheckConstraint("owned_check", "(owner_replica IS NULL) = (owner_epoch IS NULL)");
                });

            migrationBuilder.CreateTable(
                name: "release_request",
                schema: "reeve_cluster",
                columns: table => new
                {
                    kind = table.Column<string>(type: "text", nullable: false),
                    key = table.Column<string>(type: "text", nullable: false),
                    token = table.Column<long>(type: "bigint", nullable: false),
                    requested_at = table.Column<DateTimeOffset>(type: "timestamp with time zone", nullable: false)
                },
                constraints: table =>
                {
                    table.PrimaryKey("release_request_pkey", x => new { x.kind, x.key });
                });

            migrationBuilder.CreateTable(
                name: "slot",
                schema: "reeve_cluster",
                columns: table => new
                {
                    key = table.Column<string>(type: "text", nullable: false),
                    id = table.Column<string>(type: "text", nullable: false),
                    holder_replica = table.Column<string>(type: "text", nullable: false),
                    holder_epoch = table.Column<long>(type: "bigint", nullable: false),
                    token = table.Column<Guid>(type: "uuid", nullable: false),
                    expires_at = table.Column<DateTimeOffset>(type: "timestamp with time zone", nullable: false)
                },
                constraints: table =>
                {
                    table.PrimaryKey("slot_pkey", x => new { x.key, x.id });
                });

            migrationBuilder.CreateIndex(
                name: "budget_window_end",
                schema: "reeve_cluster",
                table: "budget",
                column: "window_end");

            migrationBuilder.CreateIndex(
                name: "claim_expires_at",
                schema: "reeve_cluster",
                table: "claim",
                column: "expires_at");

            migrationBuilder.CreateIndex(
                name: "claim_expires_plain",
                schema: "reeve_cluster",
                table: "claim",
                column: "expires_at",
                filter: "once_mode IS NULL");

            migrationBuilder.CreateIndex(
                name: "claim_once_retake",
                schema: "reeve_cluster",
                table: "claim",
                column: "expires_at",
                filter: "once_mode = 'at_least_once' AND completed_at IS NULL");

            migrationBuilder.CreateIndex(
                name: "command_active",
                schema: "reeve_cluster",
                table: "command",
                columns: new[] { "kind", "key", "created_at", "id" },
                filter: "state IN ('pending', 'taken')");

            migrationBuilder.CreateIndex(
                name: "command_finished",
                schema: "reeve_cluster",
                table: "command",
                column: "completed_at",
                filter: "state IN ('done', 'in_doubt', 'expired')");

            migrationBuilder.CreateIndex(
                name: "command_one_taken",
                schema: "reeve_cluster",
                table: "command",
                columns: new[] { "kind", "key" },
                unique: true,
                filter: "state = 'taken'");

            migrationBuilder.CreateIndex(
                name: "owned_owner",
                schema: "reeve_cluster",
                table: "owned",
                column: "owner_replica",
                filter: "owner_replica IS NOT NULL");

            migrationBuilder.Sql("""
                INSERT INTO reeve_cluster.cluster (id) VALUES (true);
                """);

            migrationBuilder.Sql("""
                CREATE FUNCTION reeve_cluster.change_log_assign() RETURNS trigger
                LANGUAGE plpgsql AS $$
                BEGIN
                    PERFORM pg_advisory_xact_lock(5928220721301704519);
                    NEW.seq := nextval('reeve_cluster.change_log_seq');
                    NEW.xid := pg_current_xact_id();
                    NEW.entry_token := gen_random_uuid();
                    RETURN NEW;
                END
                $$;
                """);

            migrationBuilder.Sql("""
                CREATE TRIGGER change_log_assign BEFORE INSERT ON reeve_cluster.change_log
                    FOR EACH ROW EXECUTE FUNCTION reeve_cluster.change_log_assign();
                """);

            migrationBuilder.Sql("""
                CREATE FUNCTION reeve_cluster.server_timeline()
                RETURNS bigint
                LANGUAGE sql
                VOLATILE
                AS $$
                    SELECT ('x' || lpad(left(pg_walfile_name(pg_current_wal_insert_lsn()), 8), 16, '0'))::bit(64)::bigint
                $$;
                """);

            migrationBuilder.Sql("""
                CREATE FUNCTION reeve_cluster.owned_touch()
                RETURNS trigger
                LANGUAGE plpgsql
                AS $$
                BEGIN
                    NEW.updated_at := clock_timestamp();
                    RETURN NEW;
                END
                $$;
                """);

            migrationBuilder.Sql("""
                CREATE TRIGGER owned_touch BEFORE UPDATE ON reeve_cluster.owned
                    FOR EACH ROW EXECUTE FUNCTION reeve_cluster.owned_touch();
                """);

            migrationBuilder.Sql("""
                CREATE FUNCTION reeve_cluster.draw_token()
                RETURNS bigint
                LANGUAGE sql
                VOLATILE
                AS $$
                    SELECT reeve_cluster.server_timeline() * 1099511627776 + nextval('reeve_cluster.owned_token')
                $$;
                """);

            migrationBuilder.Sql("""
                CREATE FUNCTION reeve_cluster.owner_is(p_kind text, p_key text, p_token bigint, p_timeline bigint)
                RETURNS boolean
                LANGUAGE plpgsql
                VOLATILE
                AS $$
                DECLARE
                    v_owner text;
                    v_epoch bigint;
                    v_token bigint;
                BEGIN
                    IF reeve_cluster.server_timeline() IS DISTINCT FROM p_timeline THEN
                        RAISE EXCEPTION 'timeline_changed' USING ERRCODE = 'RC002';
                    END IF;

                    SELECT o.owner_replica, o.owner_epoch, o.token
                      INTO v_owner, v_epoch, v_token
                      FROM reeve_cluster.owned o
                     WHERE o.kind = p_kind AND o.key = p_key
                       FOR SHARE;

                    IF NOT FOUND OR v_owner IS NULL OR v_token IS DISTINCT FROM p_token THEN
                        RETURN false;
                    END IF;

                    RETURN EXISTS (SELECT 1
                                     FROM reeve_cluster.member m
                                    WHERE m.replica_id = v_owner
                                      AND m.epoch = v_epoch
                                      AND m.state = 'active'
                                      AND m.lease_expires_at > clock_timestamp());
                END
                $$;
                """);

            migrationBuilder.Sql("""
                CREATE FUNCTION reeve_cluster.assert_owner(p_kind text, p_key text, p_token bigint, p_timeline bigint)
                RETURNS void
                LANGUAGE plpgsql
                VOLATILE
                AS $$
                BEGIN
                    IF NOT reeve_cluster.owner_is(p_kind, p_key, p_token, p_timeline) THEN
                        RAISE EXCEPTION 'ownership_lost' USING ERRCODE = 'RC001';
                    END IF;
                END
                $$;
                """);

            migrationBuilder.Sql("""
                INSERT INTO reeve_cluster.change_ack (id, acked_seq) VALUES (true, 0);
                """);

            migrationBuilder.Sql("""
                CREATE FUNCTION reeve_cluster.cursor_valid(p_seq bigint, p_token uuid) RETURNS boolean
                LANGUAGE sql STABLE AS $$
                    SELECT coalesce(
                        EXISTS (SELECT 1 FROM reeve_cluster.change_log l WHERE l.seq = p_seq AND l.entry_token = p_token)
                        OR (p_seq = 0 AND (SELECT c.horizon_seq = 0 FROM reeve_cluster.cluster c)),
                        false)
                $$;
                """);

            migrationBuilder.Sql("""
                CREATE FUNCTION reeve_cluster.claim_valid(p_key text, p_token uuid)
                RETURNS boolean
                LANGUAGE sql
                VOLATILE
                AS $$
                    SELECT EXISTS (
                        SELECT 1
                          FROM reeve_cluster.claim c
                         WHERE c.key = p_key
                           AND c.token = p_token
                           AND c.expires_at > clock_timestamp()
                           AND EXISTS (SELECT 1
                                         FROM reeve_cluster.member m
                                        WHERE m.replica_id = c.holder_replica
                                          AND m.epoch = c.holder_epoch
                                          AND m.state = 'active'
                                          AND m.lease_expires_at > clock_timestamp()))
                $$;
                """);

            migrationBuilder.Sql("""
                CREATE FUNCTION reeve_cluster.claim_is(p_key text, p_token uuid, p_timeline bigint)
                RETURNS boolean
                LANGUAGE plpgsql
                VOLATILE
                AS $$
                BEGIN
                    IF reeve_cluster.server_timeline() IS DISTINCT FROM p_timeline THEN
                        RAISE EXCEPTION 'timeline_changed' USING ERRCODE = 'RC002';
                    END IF;

                    RETURN reeve_cluster.claim_valid(p_key, p_token);
                END
                $$;
                """);

            migrationBuilder.Sql("""
                CREATE FUNCTION reeve_cluster.assert_claim(p_key text, p_token uuid, p_timeline bigint)
                RETURNS void
                LANGUAGE plpgsql
                VOLATILE
                AS $$
                DECLARE
                    v_holder text;
                    v_epoch bigint;
                    v_token uuid;
                    v_expires timestamptz;
                BEGIN
                    IF reeve_cluster.server_timeline() IS DISTINCT FROM p_timeline THEN
                        RAISE EXCEPTION 'timeline_changed' USING ERRCODE = 'RC002';
                    END IF;

                    SELECT c.holder_replica, c.holder_epoch, c.token, c.expires_at
                      INTO v_holder, v_epoch, v_token, v_expires
                      FROM reeve_cluster.claim c
                     WHERE c.key = p_key
                       FOR SHARE;

                    IF NOT FOUND
                       OR v_token IS DISTINCT FROM p_token
                       OR v_expires <= clock_timestamp()
                       OR NOT EXISTS (SELECT 1
                                        FROM reeve_cluster.member m
                                       WHERE m.replica_id = v_holder
                                         AND m.epoch = v_epoch
                                         AND m.state = 'active'
                                         AND m.lease_expires_at > clock_timestamp()) THEN
                        RAISE EXCEPTION 'claim_lost' USING ERRCODE = 'RC003';
                    END IF;
                END
                $$;
                """);

            migrationBuilder.Sql("""
                CREATE FUNCTION reeve_cluster.slot_valid(p_key text, p_id text, p_token uuid)
                RETURNS boolean
                LANGUAGE sql
                VOLATILE
                AS $$
                    SELECT EXISTS (
                        SELECT 1
                          FROM reeve_cluster.slot s
                         WHERE s.key = p_key
                           AND s.id = p_id
                           AND s.token = p_token
                           AND s.expires_at > clock_timestamp()
                           AND EXISTS (SELECT 1
                                         FROM reeve_cluster.member m
                                        WHERE m.replica_id = s.holder_replica
                                          AND m.epoch = s.holder_epoch
                                          AND m.state IN ('active', 'draining')))
                $$;
                """);

            migrationBuilder.Sql("""
                CREATE FUNCTION reeve_cluster.slot_is(p_key text, p_id text, p_token uuid, p_timeline bigint)
                RETURNS boolean
                LANGUAGE plpgsql
                VOLATILE
                AS $$
                BEGIN
                    IF reeve_cluster.server_timeline() IS DISTINCT FROM p_timeline THEN
                        RAISE EXCEPTION 'timeline_changed' USING ERRCODE = 'RC002';
                    END IF;

                    RETURN reeve_cluster.slot_valid(p_key, p_id, p_token);
                END
                $$;
                """);

            migrationBuilder.Sql("""
                CREATE FUNCTION reeve_cluster.slot_lock(p_key text, p_timeline bigint)
                RETURNS boolean
                LANGUAGE plpgsql
                VOLATILE
                AS $$
                BEGIN
                    IF reeve_cluster.server_timeline() IS DISTINCT FROM p_timeline THEN
                        RAISE EXCEPTION 'timeline_changed' USING ERRCODE = 'RC002';
                    END IF;

                    PERFORM pg_advisory_xact_lock(1397509972, hashtext(p_key));
                    RETURN true;
                END
                $$;
                """);

            migrationBuilder.Sql("""
                CREATE FUNCTION reeve_cluster.assert_slot(p_key text, p_id text, p_token uuid, p_timeline bigint)
                RETURNS void
                LANGUAGE plpgsql
                VOLATILE
                AS $$
                DECLARE
                    v_holder text;
                    v_epoch bigint;
                    v_token uuid;
                    v_expires timestamptz;
                BEGIN
                    IF reeve_cluster.server_timeline() IS DISTINCT FROM p_timeline THEN
                        RAISE EXCEPTION 'timeline_changed' USING ERRCODE = 'RC002';
                    END IF;

                    SELECT s.holder_replica, s.holder_epoch, s.token, s.expires_at
                      INTO v_holder, v_epoch, v_token, v_expires
                      FROM reeve_cluster.slot s
                     WHERE s.key = p_key
                       AND s.id = p_id
                       FOR KEY SHARE;

                    IF NOT FOUND
                       OR v_token IS DISTINCT FROM p_token
                       OR v_expires <= clock_timestamp()
                       OR NOT EXISTS (SELECT 1
                                        FROM reeve_cluster.member m
                                       WHERE m.replica_id = v_holder
                                         AND m.epoch = v_epoch
                                         AND m.state IN ('active', 'draining')) THEN
                        RAISE EXCEPTION 'slot_lost' USING ERRCODE = 'RC004';
                    END IF;
                END
                $$;
                """);
        }

        protected override void Down(MigrationBuilder migrationBuilder)
        {
            migrationBuilder.DropTable(
                name: "budget",
                schema: "reeve_cluster");

            migrationBuilder.DropTable(
                name: "budget_id",
                schema: "reeve_cluster");

            migrationBuilder.DropTable(
                name: "change_ack",
                schema: "reeve_cluster");

            migrationBuilder.DropTable(
                name: "change_log",
                schema: "reeve_cluster");

            migrationBuilder.DropTable(
                name: "change_skip",
                schema: "reeve_cluster");

            migrationBuilder.DropTable(
                name: "claim",
                schema: "reeve_cluster");

            migrationBuilder.DropTable(
                name: "cluster",
                schema: "reeve_cluster");

            migrationBuilder.DropTable(
                name: "command",
                schema: "reeve_cluster");

            migrationBuilder.DropTable(
                name: "counter",
                schema: "reeve_cluster");

            migrationBuilder.DropTable(
                name: "fence_history",
                schema: "reeve_cluster");

            migrationBuilder.DropTable(
                name: "member",
                schema: "reeve_cluster");

            migrationBuilder.DropTable(
                name: "member_status",
                schema: "reeve_cluster");

            migrationBuilder.DropTable(
                name: "owned",
                schema: "reeve_cluster");

            migrationBuilder.DropTable(
                name: "release_request",
                schema: "reeve_cluster");

            migrationBuilder.DropTable(
                name: "slot",
                schema: "reeve_cluster");

            migrationBuilder.DropSequence(
                name: "change_log_seq",
                schema: "reeve_cluster");

            migrationBuilder.DropSequence(
                name: "member_epoch",
                schema: "reeve_cluster");

            migrationBuilder.DropSequence(
                name: "owned_token",
                schema: "reeve_cluster");

            migrationBuilder.Sql("DROP FUNCTION reeve_cluster.change_log_assign();");
            migrationBuilder.Sql("DROP FUNCTION reeve_cluster.server_timeline();");
            migrationBuilder.Sql("DROP FUNCTION reeve_cluster.owned_touch();");
            migrationBuilder.Sql("DROP FUNCTION reeve_cluster.draw_token();");
            migrationBuilder.Sql("DROP FUNCTION reeve_cluster.owner_is(text, text, bigint, bigint);");
            migrationBuilder.Sql("DROP FUNCTION reeve_cluster.assert_owner(text, text, bigint, bigint);");
            migrationBuilder.Sql("DROP FUNCTION reeve_cluster.cursor_valid(bigint, uuid);");
            migrationBuilder.Sql("DROP FUNCTION reeve_cluster.claim_valid(text, uuid);");
            migrationBuilder.Sql("DROP FUNCTION reeve_cluster.claim_is(text, uuid, bigint);");
            migrationBuilder.Sql("DROP FUNCTION reeve_cluster.assert_claim(text, uuid, bigint);");
            migrationBuilder.Sql("DROP FUNCTION reeve_cluster.slot_valid(text, text, uuid);");
            migrationBuilder.Sql("DROP FUNCTION reeve_cluster.slot_is(text, text, uuid, bigint);");
            migrationBuilder.Sql("DROP FUNCTION reeve_cluster.slot_lock(text, bigint);");
            migrationBuilder.Sql("DROP FUNCTION reeve_cluster.assert_slot(text, text, uuid, bigint);");
        }
    }
}
