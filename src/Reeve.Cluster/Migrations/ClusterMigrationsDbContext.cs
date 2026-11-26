using System.Diagnostics.CodeAnalysis;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Metadata.Builders;

namespace Reeve.Cluster.Migrations;

internal sealed class ClusterMigrationsDbContext : DbContext
{
    public const string Unsupported = "The schema migrations are EF Core migrations, which EF Core does not support under trimming or NativeAOT. Run them from a JIT-compiled process.";

    public const string Schema = "reeve_cluster";

    public const string HistoryTable = "__ClusterMigrationsHistory";

    [RequiresUnreferencedCode(Unsupported)]
    [RequiresDynamicCode(Unsupported)]
    public ClusterMigrationsDbContext(DbContextOptions<ClusterMigrationsDbContext> options)
        : base(options)
    {
    }

    protected override void OnModelCreating(ModelBuilder modelBuilder)
    {
        ArgumentNullException.ThrowIfNull(modelBuilder);

        modelBuilder.HasSequence<long>("member_epoch", Schema);
        modelBuilder.HasSequence<long>("change_log_seq", Schema);
        modelBuilder.HasSequence<long>("owned_token", Schema);

        modelBuilder.Entity<MemberRow>(e =>
        {
            e.ToTable("member", Schema, t => t.HasCheckConstraint("member_state_check", "state IN ('joining', 'active', 'fenced', 'draining')"));
            e.HasKey(x => x.ReplicaId).HasName("member_pkey");
            e.Property(x => x.ReplicaId).HasColumnName("replica_id");
            e.Property(x => x.Epoch).HasColumnName("epoch");
            e.Property(x => x.State).HasColumnName("state");
            e.Property(x => x.LeaseExpiresAt).HasColumnName("lease_expires_at");
            e.Property(x => x.ServingSince).HasColumnName("serving_since");
            e.Property(x => x.ActivatedAtSeq).HasColumnName("activated_at_seq");
            e.Property(x => x.AppliedSeq).HasColumnName("applied_seq").HasDefaultValueSql("0");
            e.Property(x => x.AppliedToken).HasColumnName("applied_token");
            e.Property(x => x.Capabilities).HasColumnName("capabilities").HasColumnType("text[]").HasDefaultValueSql("'{}'");
            e.Property(x => x.Version).HasColumnName("version");
            e.Property(x => x.Protocol).HasColumnName("protocol");
            e.Property(x => x.ProcessId).HasColumnName("process_id");
        });

        modelBuilder.Entity<ClusterRow>(e =>
        {
            e.ToTable("cluster", Schema, t =>
            {
                t.HasCheckConstraint("cluster_id_check", "id");
                t.HasCheckConstraint("cluster_horizon_seq_check", "horizon_seq >= 0");
            });
            e.HasKey(x => x.Id).HasName("cluster_pkey");
            e.Property(x => x.Id).HasColumnName("id").ValueGeneratedNever().HasDefaultValueSql("true");
            e.Property(x => x.ProtocolFloor).HasColumnName("protocol_floor").HasDefaultValueSql("0");
            e.Property(x => x.VersionFloor).HasColumnName("version_floor");
            e.Property(x => x.HorizonSeq).HasColumnName("horizon_seq").HasDefaultValueSql("0");
            e.Property(x => x.FloorMovedBy).HasColumnName("floor_moved_by");
            e.Property(x => x.FloorMovedAt).HasColumnName("floor_moved_at");
        });

        modelBuilder.Entity<FenceHistoryRow>(e =>
        {
            e.ToTable("fence_history", Schema);
            e.HasKey(x => new { x.ReplicaId, x.Epoch }).HasName("fence_history_pkey");
            e.Property(x => x.ReplicaId).HasColumnName("replica_id");
            e.Property(x => x.Epoch).HasColumnName("epoch");
            e.Property(x => x.Trigger).HasColumnName("trigger");
            e.Property(x => x.FencedAt).HasColumnName("fenced_at").HasDefaultValueSql("clock_timestamp()");
            e.Property(x => x.Span).HasColumnName("span").HasColumnType("interval");
        });

        modelBuilder.Entity<ChangeLogRow>(e =>
        {
            e.ToTable("change_log", Schema, t => t.HasCheckConstraint("change_log_aggregate_type_check", "aggregate_type <> ''"));
            e.HasKey(x => x.Seq).HasName("change_log_pkey");
            e.Property(x => x.Seq).HasColumnName("seq").ValueGeneratedNever();
            e.Property(x => x.EntryToken).HasColumnName("entry_token");
            e.Property(x => x.Xid).HasColumnName("xid").HasColumnType("xid8");
            e.Property(x => x.AggregateType).HasColumnName("aggregate_type");
            e.Property(x => x.AggregateKey).HasColumnName("aggregate_key");
            e.Property(x => x.CreatedAt).HasColumnName("created_at").HasDefaultValueSql("clock_timestamp()");
        });

        modelBuilder.Entity<OwnedRow>(e =>
        {
            e.ToTable("owned", Schema, t => t.HasCheckConstraint("owned_check", "(owner_replica IS NULL) = (owner_epoch IS NULL)"));
            e.HasKey(x => new { x.Kind, x.Key }).HasName("owned_pkey");
            e.Property(x => x.Kind).HasColumnName("kind");
            e.Property(x => x.Key).HasColumnName("key");
            e.Property(x => x.OwnerReplica).HasColumnName("owner_replica");
            e.Property(x => x.OwnerEpoch).HasColumnName("owner_epoch");
            e.Property(x => x.Token).HasColumnName("token");
            e.Property(x => x.UpdatedAt).HasColumnName("updated_at").HasDefaultValueSql("clock_timestamp()");
            e.HasIndex(x => x.OwnerReplica, "owned_owner").HasFilter("owner_replica IS NOT NULL");
        });

        modelBuilder.Entity<ReleaseRequestRow>(e =>
        {
            e.ToTable("release_request", Schema);
            e.HasKey(x => new { x.Kind, x.Key }).HasName("release_request_pkey");
            e.Property(x => x.Kind).HasColumnName("kind");
            e.Property(x => x.Key).HasColumnName("key");
            e.Property(x => x.Token).HasColumnName("token");
            e.Property(x => x.RequestedAt).HasColumnName("requested_at");
        });

        modelBuilder.Entity<ChangeAckRow>(e =>
        {
            e.ToTable("change_ack", Schema, t =>
            {
                t.HasCheckConstraint("change_ack_id_check", "id");
                t.HasCheckConstraint("change_ack_acked_seq_check", "acked_seq >= 0");
            });
            e.HasKey(x => x.Id).HasName("change_ack_pkey");
            e.Property(x => x.Id).HasColumnName("id").ValueGeneratedNever();
            e.Property(x => x.AckedSeq).HasColumnName("acked_seq");
        });

        modelBuilder.Entity<ChangeSkipRow>(e =>
        {
            e.ToTable("change_skip", Schema, t =>
            {
                t.HasCheckConstraint("change_skip_aggregate_type_check", "aggregate_type <> ''");
                t.HasCheckConstraint("change_skip_from_seq_check", "from_seq > 0");
                t.HasCheckConstraint("change_skip_check", "(closed_by IS NULL) = (closed_at IS NULL)");
            });
            e.HasKey(x => x.Id).HasName("change_skip_pkey");
            e.Property(x => x.Id).HasColumnName("id").UseIdentityAlwaysColumn();
            e.Property(x => x.AggregateType).HasColumnName("aggregate_type");
            e.Property(x => x.FromSeq).HasColumnName("from_seq");
            e.Property(x => x.UntilSeq).HasColumnName("until_seq");
            e.Property(x => x.Reason).HasColumnName("reason");
            e.Property(x => x.OpenedBy).HasColumnName("opened_by");
            e.Property(x => x.OpenedAt).HasColumnName("opened_at").HasDefaultValueSql("clock_timestamp()");
            e.Property(x => x.ClosedBy).HasColumnName("closed_by");
            e.Property(x => x.ClosedAt).HasColumnName("closed_at");
        });

        modelBuilder.Entity<ClaimRow>(e =>
        {
            e.ToTable("claim", Schema, t =>
            {
                t.HasCheckConstraint("claim_once_mode_check", "once_mode IN ('at_most_once', 'at_least_once')");
                t.HasCheckConstraint(
                    "claim_once_shape",
                    "(once_mode IS NULL AND once_effect IS NULL AND once_ttl IS NULL AND completed_at IS NULL) OR (once_mode IS NOT NULL AND once_effect IS NOT NULL AND once_ttl IS NOT NULL)");
                t.HasCheckConstraint("claim_result_shape", "(result IS NULL) = (result_expires_at IS NULL)");
            });
            e.HasKey(x => x.Key).HasName("claim_pkey");
            e.Property(x => x.Key).HasColumnName("key");
            e.Property(x => x.HolderReplica).HasColumnName("holder_replica");
            e.Property(x => x.HolderEpoch).HasColumnName("holder_epoch");
            e.Property(x => x.Token).HasColumnName("token");
            e.Property(x => x.ExpiresAt).HasColumnName("expires_at");
            e.Property(x => x.OnceMode).HasColumnName("once_mode");
            e.Property(x => x.OnceEffect).HasColumnName("once_effect");
            e.Property(x => x.OnceTtl).HasColumnName("once_ttl").HasColumnType("interval");
            e.Property(x => x.CompletedAt).HasColumnName("completed_at");
            e.Property(x => x.Result).HasColumnName("result");
            e.Property(x => x.ResultExpiresAt).HasColumnName("result_expires_at");
            e.HasIndex(x => x.ExpiresAt, "claim_expires_at");
            e.HasIndex(x => x.ExpiresAt, "claim_once_retake").HasFilter("once_mode = 'at_least_once' AND completed_at IS NULL");
            e.HasIndex(x => x.ExpiresAt, "claim_expires_plain").HasFilter("once_mode IS NULL");
        });

        modelBuilder.Entity<BudgetRow>(e =>
        {
            e.ToTable("budget", Schema);
            e.HasKey(x => new { x.Key, x.WindowStart }).HasName("budget_pkey");
            e.Property(x => x.Key).HasColumnName("key");
            e.Property(x => x.WindowStart).HasColumnName("window_start");
            e.Property(x => x.Used).HasColumnName("used");
            e.Property(x => x.WindowEnd).HasColumnName("window_end").HasDefaultValueSql("'infinity'");
            e.HasIndex(x => x.WindowEnd, "budget_window_end");
        });

        modelBuilder.Entity<BudgetIdRow>(e =>
        {
            e.ToTable("budget_id", Schema);
            e.HasKey(x => new { x.Key, x.WindowStart, x.Id }).HasName("budget_id_pkey");
            e.Property(x => x.Key).HasColumnName("key");
            e.Property(x => x.WindowStart).HasColumnName("window_start");
            e.Property(x => x.Id).HasColumnName("id");
        });

        modelBuilder.Entity<CounterRow>(e =>
        {
            e.ToTable("counter", Schema);
            e.HasKey(x => x.Key).HasName("counter_pkey");
            e.Property(x => x.Key).HasColumnName("key");
            e.Property(x => x.Value).HasColumnName("value");
            e.Property(x => x.UpdatedAt).HasColumnName("updated_at").HasDefaultValueSql("now()");
        });

        modelBuilder.Entity<SlotRow>(e =>
        {
            e.ToTable("slot", Schema);
            e.HasKey(x => new { x.Key, x.Id }).HasName("slot_pkey");
            e.Property(x => x.Key).HasColumnName("key");
            e.Property(x => x.Id).HasColumnName("id");
            e.Property(x => x.HolderReplica).HasColumnName("holder_replica");
            e.Property(x => x.HolderEpoch).HasColumnName("holder_epoch");
            e.Property(x => x.Token).HasColumnName("token");
            e.Property(x => x.ExpiresAt).HasColumnName("expires_at");
        });

        modelBuilder.Entity<CommandRow>(e => ConfigureCommand(e));

        modelBuilder.Entity<MemberStatusRow>(e =>
        {
            e.ToTable("member_status", Schema);
            e.HasKey(x => new { x.ReplicaId, x.Epoch, x.Section }).HasName("member_status_pkey");
            e.Property(x => x.ReplicaId).HasColumnName("replica_id");
            e.Property(x => x.Epoch).HasColumnName("epoch");
            e.Property(x => x.Section).HasColumnName("section");
            e.Property(x => x.Payload).HasColumnName("payload").HasColumnType("jsonb");
            e.Property(x => x.UpdatedAt).HasColumnName("updated_at").HasDefaultValueSql("clock_timestamp()");
        });
    }

    private static void ConfigureCommand(EntityTypeBuilder<CommandRow> e)
    {
        e.ToTable("command", Schema, t =>
        {
            t.HasCheckConstraint("command_state", "state IN ('pending', 'taken', 'done', 'in_doubt', 'expired')");
            t.HasCheckConstraint("command_token_shape", "(state IN ('pending', 'expired')) = (taken_token IS NULL)");
            t.HasCheckConstraint("command_taken_at_shape", "(taken_token IS NULL) = (taken_at IS NULL)");
            t.HasCheckConstraint("command_completed_at_shape", "state NOT IN ('pending', 'taken') OR completed_at IS NULL");
            t.HasCheckConstraint("command_error_shape", "error IS NULL OR (state = 'done' AND result IS NULL)");
        });
        e.HasKey(x => x.Id).HasName("command_pkey");
        e.HasAlternateKey(x => new { x.Kind, x.Key, x.IdempotencyKey }).HasName("command_idempotency");
        e.Property(x => x.Id).HasColumnName("id").UseIdentityAlwaysColumn();
        e.Property(x => x.Kind).HasColumnName("kind");
        e.Property(x => x.Key).HasColumnName("key");
        e.Property(x => x.IdempotencyKey).HasColumnName("idempotency_key");
        e.Property(x => x.Payload).HasColumnName("payload");
        e.Property(x => x.State).HasColumnName("state");
        e.Property(x => x.TakenToken).HasColumnName("taken_token");
        e.Property(x => x.TakenAt).HasColumnName("taken_at");
        e.Property(x => x.Result).HasColumnName("result");
        e.Property(x => x.Error).HasColumnName("error");
        e.Property(x => x.CreatedAt).HasColumnName("created_at").HasDefaultValueSql("now()");
        e.Property(x => x.CompletedAt).HasColumnName("completed_at");

        e.HasIndex(x => new { x.Kind, x.Key }, "command_one_taken").IsUnique().HasFilter("state = 'taken'");
        e.HasIndex(x => new { x.Kind, x.Key, x.CreatedAt, x.Id }, "command_active").HasFilter("state IN ('pending', 'taken')");
        e.HasIndex(x => x.CompletedAt, "command_finished").HasFilter("state IN ('done', 'in_doubt', 'expired')");
    }
}
