namespace Reeve.Cluster.Core;

internal enum OptionsViolation
{
    ReplicaIdMissing,

    ConnectionStringMissing,

    ReconcileTimeoutMissing,

    ProxyReadTimeoutMissing,

    ActionTimeBudgetMissing,

    NotPositive,

    DurationTooLong,

    RenewIntervalTooLong,

    StopBudgetTooShort,

    ApplyStallLimitTooShort,

    AckTimeoutTooShort,

    ReleaseGraceMismatch,

    HttpBoundExceeded,

    TargetNotPrimary,

    CommandPendingTtlTooShort,
}

internal sealed record OptionsProblem(OptionsViolation Rule, string Setting, string Message);

internal static class OptionsValidator
{
    public static readonly TimeSpan MaxTimerDuration = TimeSpan.FromMilliseconds(uint.MaxValue - 1d);

    public static readonly TimeSpan MaxCommandAge = TimeSpan.FromDays(3650);

    public static IReadOnlyList<OptionsProblem> Validate(ClusterOptions options, bool aspNetCoreInUse, IReadOnlyCollection<Owned.UnitKind>? commandKinds = null)
    {
        ArgumentNullException.ThrowIfNull(options);

        var problems = new List<OptionsProblem>();

        void Add(OptionsViolation rule, string setting, string message) => problems.Add(new OptionsProblem(rule, setting, message));

        if (string.IsNullOrWhiteSpace(options.ReplicaId))
        {
            Add(OptionsViolation.ReplicaIdMissing, nameof(ClusterOptions.ReplicaId), "ReplicaId is required: the deployment names each replica (for example from a REPLICA_ID environment variable).");
        }

        if (string.IsNullOrWhiteSpace(options.ConnectionString))
        {
            Add(OptionsViolation.ConnectionStringMissing, nameof(ClusterOptions.ConnectionString), "ConnectionString is required: the primary of the database that holds the reeve_cluster schema.");
        }
        else
        {
            var refusal = PrimaryTarget.Refusal(options.ConnectionString);
            if (refusal is not null)
            {
                Add(OptionsViolation.TargetNotPrimary, nameof(ClusterOptions.ConnectionString), $"ConnectionString must name the primary: {refusal}.");
            }
        }

        if (options.ReconcileTimeout is null)
        {
            Add(OptionsViolation.ReconcileTimeoutMissing, nameof(ClusterOptions.ReconcileTimeout), "ReconcileTimeout is required: the application bounds one reconciler attempt.");
        }

        if (aspNetCoreInUse)
        {
            if (options.ProxyReadTimeout is null)
            {
                Add(OptionsViolation.ProxyReadTimeoutMissing, nameof(ClusterOptions.ProxyReadTimeout), "ProxyReadTimeout is required when the ASP.NET Core package is used: the shortest time any balancer or proxy waits for a response to start.");
            }

            if (options.ActionTimeBudget is null)
            {
                Add(OptionsViolation.ActionTimeBudgetMissing, nameof(ClusterOptions.ActionTimeBudget), "ActionTimeBudget is required when the ASP.NET Core package is used: the longest an endpoint's own work takes.");
            }
        }

        var positivityFailed = false;
        void Positive(string setting, TimeSpan? value)
        {
            if (value.HasValue && value.Value <= TimeSpan.Zero)
            {
                positivityFailed = true;
                Add(OptionsViolation.NotPositive, setting, $"{setting} must be positive, not {value.Value}.");
            }
        }

        void NotNegative(string setting, TimeSpan value)
        {
            if (value < TimeSpan.Zero)
            {
                positivityFailed = true;
                Add(OptionsViolation.NotPositive, setting, $"{setting} must not be negative, not {value}.");
            }
        }

        Positive(nameof(ClusterOptions.LeaseTtl), options.LeaseTtl);
        Positive(nameof(ClusterOptions.SafetyMargin), options.SafetyMargin);
        Positive(nameof(ClusterOptions.StopBudget), options.StopBudget);
        Positive(nameof(ClusterOptions.RenewTimeout), options.RenewTimeout);
        Positive(nameof(ClusterOptions.RenewInterval), options.RenewInterval);
        Positive(nameof(ClusterOptions.ReconcileTimeout), options.ReconcileTimeout);
        Positive(nameof(ClusterOptions.AckPollInterval), options.AckPollInterval);
        Positive(nameof(ClusterOptions.ApplyPollInterval), options.ApplyPollInterval);
        Positive(nameof(ClusterOptions.RetentionInterval), options.RetentionInterval);
        Positive(nameof(ClusterOptions.ReplicationCheckInterval), options.ReplicationCheckInterval);
        Positive(nameof(ClusterOptions.StatusReportInterval), options.StatusReportInterval);
        Positive(nameof(ClusterOptions.ReadIndexTimeout), options.ReadIndexTimeout);
        Positive(nameof(ClusterOptions.FencedTransactionTimeout), options.FencedTransactionTimeout);
        Positive(nameof(ClusterOptions.PlacementInterval), options.PlacementInterval);
        NotNegative(nameof(ClusterOptions.StabilisationWindow), options.StabilisationWindow);
        Positive(nameof(ClusterOptions.OwnedRowRetention), options.OwnedRowRetention);
        Positive(nameof(ClusterOptions.CommandPendingTtl), options.CommandPendingTtl);
        Positive(nameof(ClusterOptions.CommandRetention), options.CommandRetention);
        Positive(nameof(ClusterOptions.OnceRetention), options.OnceRetention);
        Positive(nameof(ClusterOptions.CounterRetention), options.CounterRetention);
        NotNegative(nameof(ClusterOptions.RejoinBackoff), options.RejoinBackoff);
        if (aspNetCoreInUse)
        {
            Positive(nameof(ClusterOptions.ProxyReadTimeout), options.ProxyReadTimeout);
            Positive(nameof(ClusterOptions.ActionTimeBudget), options.ActionTimeBudget);
        }

        if (options.HintLimit <= 0)
        {
            positivityFailed = true;
            Add(OptionsViolation.NotPositive, nameof(ClusterOptions.HintLimit), $"HintLimit must be positive, not {options.HintLimit}.");
        }

        if (positivityFailed)
        {
            return problems;
        }

        var durationTooLong = false;
        void AtMostTimerLimit(string setting, TimeSpan value)
        {
            if (value > MaxTimerDuration)
            {
                durationTooLong = true;
                Add(OptionsViolation.DurationTooLong, setting, $"{setting} must be at most {MaxTimerDuration} (the longest wait Task.Delay and the timers accept), not {value}.");
            }
        }

        void AtMostCommandAge(string setting, TimeSpan value)
        {
            if (value > MaxCommandAge)
            {
                Add(OptionsViolation.DurationTooLong, setting, $"{setting} must be at most {MaxCommandAge} (the sweeps subtract it from the database's clock), not {value}.");
            }
        }

        AtMostCommandAge(nameof(ClusterOptions.CommandPendingTtl), options.CommandPendingTtl);
        AtMostCommandAge(nameof(ClusterOptions.CommandRetention), options.CommandRetention);
        if (options.OnceRetention.HasValue)
        {
            AtMostCommandAge(nameof(ClusterOptions.OnceRetention), options.OnceRetention.Value);
        }

        if (options.CounterRetention.HasValue)
        {
            AtMostCommandAge(nameof(ClusterOptions.CounterRetention), options.CounterRetention.Value);
        }

        AtMostTimerLimit(nameof(ClusterOptions.LeaseTtl), options.LeaseTtl);
        AtMostTimerLimit(nameof(ClusterOptions.StopBudget), options.StopBudget);
        AtMostTimerLimit(nameof(ClusterOptions.RenewTimeout), options.RenewTimeout);
        AtMostTimerLimit(nameof(ClusterOptions.RenewInterval), options.RenewInterval);
        if (options.ReconcileTimeout.HasValue)
        {
            AtMostTimerLimit(nameof(ClusterOptions.ReconcileTimeout), options.ReconcileTimeout.Value);
        }

        AtMostTimerLimit(nameof(ClusterOptions.AckPollInterval), options.AckPollInterval);
        AtMostTimerLimit(nameof(ClusterOptions.ApplyPollInterval), options.ApplyPollInterval);
        AtMostTimerLimit(nameof(ClusterOptions.RetentionInterval), options.RetentionInterval);
        AtMostTimerLimit(nameof(ClusterOptions.ReplicationCheckInterval), options.ReplicationCheckInterval);
        AtMostTimerLimit(nameof(ClusterOptions.StatusReportInterval), options.StatusReportInterval);
        AtMostTimerLimit(nameof(ClusterOptions.ReadIndexTimeout), options.ReadIndexTimeout);
        AtMostTimerLimit(nameof(ClusterOptions.FencedTransactionTimeout), options.FencedTransactionTimeout);
        AtMostTimerLimit(nameof(ClusterOptions.PlacementInterval), options.PlacementInterval);
        AtMostTimerLimit(nameof(ClusterOptions.StabilisationWindow), options.StabilisationWindow);
        AtMostTimerLimit(nameof(ClusterOptions.RejoinBackoff), options.RejoinBackoff);

        if (options.ReconcileTimeout is null || options.ReconcileTimeout.Value <= MaxTimerDuration)
        {
            AtMostTimerLimit(nameof(ClusterOptions.ApplyStallLimit), options.ApplyStallLimit);
        }

        if (!durationTooLong)
        {
            AtMostTimerLimit(nameof(ClusterOptions.AckTimeout), options.AckTimeout);
        }

        if (durationTooLong)
        {
            return problems;
        }

        if (options.RenewInterval > options.RenewIntervalLimit)
        {
            Add(
                OptionsViolation.RenewIntervalTooLong,
                nameof(ClusterOptions.RenewInterval),
                $"RenewInterval {options.RenewInterval} must be at most (LeaseTtl {options.LeaseTtl} - SafetyMargin {options.SafetyMargin} - StopBudget {options.StopBudget} - RenewTimeout {options.RenewTimeout}) / 3 = {options.RenewIntervalLimit}.");
        }

        if (options.StopBudget * 2 <= options.SafetyMargin)
        {
            Add(
                OptionsViolation.StopBudgetTooShort,
                nameof(ClusterOptions.StopBudget),
                $"StopBudget {options.StopBudget} must be more than SafetyMargin {options.SafetyMargin} / 2.");
        }

        if (options.ReconcileTimeout.HasValue)
        {
            var reconcileTimeout = options.ReconcileTimeout.Value;
            if (options.ApplyStallLimit < 2 * reconcileTimeout)
            {
                Add(
                    OptionsViolation.ApplyStallLimitTooShort,
                    nameof(ClusterOptions.ApplyStallLimit),
                    $"ApplyStallLimit {options.ApplyStallLimit} must be at least 2 x ReconcileTimeout {reconcileTimeout}.");
            }

            if (options.AckTimeout <= options.AckTimeoutRelationBound)
            {
                Add(
                    OptionsViolation.AckTimeoutTooShort,
                    nameof(ClusterOptions.AckTimeout),
                    $"AckTimeout {options.AckTimeout} must be more than max(LeaseTtl {options.LeaseTtl}, ReconcileTimeout {reconcileTimeout} + ApplyStallLimit {options.ApplyStallLimit} + RenewTimeout {options.RenewTimeout}) + AckPollInterval {options.AckPollInterval} = {options.AckTimeoutRelationBound}.");
            }
        }

        if (options.ReleaseGrace != options.ReleaseGraceRelationValue)
        {
            Add(
                OptionsViolation.ReleaseGraceMismatch,
                nameof(ClusterOptions.ReleaseGrace),
                $"ReleaseGrace {options.ReleaseGrace} must equal 2 x StopBudget {options.StopBudget} + PlacementInterval {options.PlacementInterval} = {options.ReleaseGraceRelationValue}.");
        }

        if (commandKinds is { Count: > 0 })
        {
            var binding = commandKinds
                .Select(k => (Kind: k.Kind, Needed: k.FailoverBound(options) + options.PlacementInterval))
                .MaxBy(k => k.Needed);
            if (options.CommandPendingTtl < binding.Needed)
            {
                Add(
                    OptionsViolation.CommandPendingTtlTooShort,
                    nameof(ClusterOptions.CommandPendingTtl),
                    $"CommandPendingTtl {options.CommandPendingTtl} must be at least the failover bound of kind '{binding.Kind}' ({binding.Needed - options.PlacementInterval}) + PlacementInterval {options.PlacementInterval} = {binding.Needed}; set it to {binding.Needed} or more.");
            }
        }

        if (aspNetCoreInUse && options.ReconcileTimeout.HasValue && options.ProxyReadTimeout.HasValue && options.ActionTimeBudget.HasValue)
        {
            var proxy = options.ProxyReadTimeout.Value;
            var action = options.ActionTimeBudget.Value;
            var sum = options.ReadIndexTimeout + action + options.AckTimeout;
            if (sum >= proxy)
            {
                Add(
                    OptionsViolation.HttpBoundExceeded,
                    nameof(ClusterOptions.ProxyReadTimeout),
                    $"ProxyReadTimeout {proxy} must exceed ReadIndexTimeout {options.ReadIndexTimeout} + ActionTimeBudget {action} + AckTimeout {options.AckTimeout} = {sum} (the HTTP bound), so a response starts before anything in front of the replica gives up.");
            }
        }

        return problems;
    }
}
