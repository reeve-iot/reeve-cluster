using Microsoft.AspNetCore.Builder;
using Microsoft.Extensions.DependencyInjection;

namespace Reeve.Cluster.Harness.Replicas;

internal sealed class ReplicaSpec
{
    public Action<ClusterOptions>? Configure { get; init; }

    public Action<ClusterOptions>? Profile { get; init; }

    public TimeSpan DriverTimeout { get; init; } = TimeSpan.FromMinutes(5);

    public IReadOnlyList<Type> Reconcilers { get; init; } = [];

    public Type FenceSink { get; init; } = typeof(NoFenceSink);

    public Action<IServiceCollection>? ConfigureServices { get; init; }

    public Action<WebApplication>? ConfigureWebApp { get; init; }

    public bool Redis { get; init; }

    public double ClockRate { get; init; } = 1.0;

    public string? ConnectionString { get; init; }
}
