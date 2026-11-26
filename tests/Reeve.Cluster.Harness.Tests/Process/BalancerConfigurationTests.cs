using Microsoft.Extensions.Configuration;
using Reeve.Cluster.Harness.Processes;

namespace Harness.Process;

public sealed class BalancerConfigurationTests
{
    [Fact]
    public void The_balancer_is_the_specs_nginx_configuration()
    {
        var configuration = Balancer.Configuration([("r1", 5001), ("r2", 5002)], 5000, TimeSpan.FromSeconds(30));

        Assert.Contains("proxy_next_upstream error http_503;", configuration);
        Assert.DoesNotContain("non_idempotent", configuration);
        Assert.DoesNotContain("timeout http_503", configuration);
        Assert.DoesNotContain("error timeout", configuration);
        Assert.Contains("proxy_read_timeout 30s;", configuration);
        Assert.Contains("listen 127.0.0.1:5000;", configuration);
        Assert.Contains("upstream cluster { server 127.0.0.1:5001 max_fails=0; server 127.0.0.1:5002 max_fails=0; }", configuration);
        Assert.Contains("upstream first_r2 { server 127.0.0.1:5002 max_fails=0; server 127.0.0.1:5001 max_fails=0 backup; }", configuration);
        Assert.Contains("r1 first_r1; r2 first_r2;", configuration);
    }

    [Fact]
    public void The_childs_environment_binds_to_the_options_the_test_reads()
    {
        var options = ProcessCluster.Options("r2", o =>
        {
            o.LeaseTtl = TimeSpan.FromSeconds(30);
            o.RenewInterval = TimeSpan.FromMilliseconds(2500);
            o.ProxyReadTimeout = TimeSpan.FromSeconds(60);
        });
        var environment = SampleProcess.Environment("r2", 5002, options).ToDictionary(p => p.Key, p => p.Value);
        var configuration = new ConfigurationBuilder()
            .AddInMemoryCollection(environment.Where(p => p.Key.StartsWith("Cluster__", StringComparison.Ordinal)).Select(p => new KeyValuePair<string, string?>(p.Key.Replace("__", ":", StringComparison.Ordinal), p.Value)))
            .Build();

        var bound = new ClusterOptions();
        configuration.GetSection(ClusterOptions.SectionName).Bind(bound);

        Assert.Equal("r2", bound.ReplicaId);
        Assert.Equal(options.LeaseTtl, bound.LeaseTtl);
        Assert.Equal(options.RenewInterval, bound.RenewInterval);
        Assert.Equal(options.RejoinBackoff, bound.RejoinBackoff);
        Assert.Equal(options.ReconcileTimeout, bound.ReconcileTimeout);
        Assert.Equal(options.AckTimeout, bound.AckTimeout);
        Assert.Equal(options.ProxyReadTimeout, bound.ProxyReadTimeout);
        Assert.Equal(options.ActionTimeBudget, bound.ActionTimeBudget);
        Assert.False(bound.CheckSynchronousReplication);
    }
}
