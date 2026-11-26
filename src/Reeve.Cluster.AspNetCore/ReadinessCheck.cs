using System.Globalization;
using Microsoft.Extensions.Diagnostics.HealthChecks;
using Reeve.Cluster.Log;
using Reeve.Cluster.Membership;

namespace Reeve.Cluster.AspNetCore;

internal sealed class ReadinessCheck(IPhaseGate gate, IEnumerable<SkipMonitor> skips) : IHealthCheck
{
    public Task<HealthCheckResult> CheckHealthAsync(HealthCheckContext context, CancellationToken cancellationToken = default)
    {
        var check = gate.CheckServing();
        var reasons = gate.Readiness.Current;
        var data = new Dictionary<string, object> { ["phase"] = check.Phase.ToString() };
        foreach (var reason in reasons)
        {
            data[reason.Code] = reason.Description;
        }

        var open = skips.SelectMany(m => m.Last?.Skips ?? []).ToList();
        foreach (var skip in open)
        {
            data["skip:" + skip.Id.ToString(CultureInfo.InvariantCulture)] = Describe(skip);
        }

        var critical = string.Concat(open.Select(s => $" critical: skip open, {Describe(s)}."));
        if (check.IsServing && reasons.Count == 0)
        {
            return Task.FromResult(HealthCheckResult.Healthy("Serving." + critical, data));
        }

        var named = check.IsServing
            ? string.Empty
            : check.Outcome == ServingCheckOutcome.FenceStartPassed ? $"fence start passed (phase {check.Phase})" : $"phase {check.Phase}";
        var description = string.Join("; ", reasons.Select(r => $"{r.Code}: {r.Description}").Prepend(named).Where(p => p.Length > 0));
        return Task.FromResult(HealthCheckResult.Unhealthy($"Not ready: {description}.{critical}", data: data));
    }

    private static string Describe(OpenSkip skip) =>
        string.Create(CultureInfo.InvariantCulture, $"{skip.AggregateType} from seq {skip.FromSeq} (skip {skip.Id}, opened by {skip.OpenedBy}: {skip.Reason})");
}
