using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Mvc;
using Microsoft.AspNetCore.SignalR;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Diagnostics.HealthChecks;
using Microsoft.Extensions.Options;
using Reeve.Cluster;
using Reeve.Cluster.AspNetCore;
using Reeve.Cluster.Hosting;
using Reeve.Cluster.Membership;

namespace Microsoft.Extensions.DependencyInjection;

/// <summary>Registers the ASP.NET Core package.</summary>
public static class ReeveClusterAspNetCoreServiceCollectionExtensions
{
    /// <summary>
    /// Registers the fence middleware's services, the acknowledgement's result filter (MVC) and hub filter, the fence and drain
    /// participants, and an <c>IExceptionHandler</c> that answers an exception after a refused commit by the fenced-request rule and
    /// declines every other one; it is registered ahead of the application's handlers, whichever is registered first. It tells the options validator
    /// that ASP.NET Core is in use, which makes <c>ProxyReadTimeout</c> and <c>ActionTimeBudget</c> required and validates the HTTP
    /// bound. Call it after <c>AddReeveCluster</c>.
    /// </summary>
    /// <param name="services">The service collection.</param>
    /// <param name="configure">Sets <see cref="ReeveClusterHttpOptions"/>; optional.</param>
    public static IServiceCollection AddReeveClusterAspNetCore(this IServiceCollection services, Action<ReeveClusterHttpOptions>? configure = null)
    {
        ArgumentNullException.ThrowIfNull(services);

        services.TryAddEnumerable(ServiceDescriptor.Singleton<Microsoft.Extensions.Hosting.IHostedService, HubFenceSinkCheck>());

        var options = services.AddOptions<ReeveClusterHttpOptions>().ValidateOnStart();
        if (configure is not null)
        {
            options.Configure(configure);
        }

        services.TryAddEnumerable(ServiceDescriptor.Singleton<IValidateOptions<ReeveClusterHttpOptions>, HttpOptionsValidation>());
        AddRequestFlowStartupFilter(services);

        services.TryAddSingleton<AspNetCoreMarker>();
        services.TryAddSingleton<RequestTracker>();
        services.TryAddSingleton<Admission>();
        services.TryAddSingleton<ClusterConnections>();
        services.TryAddSingleton<ErrorEnvelope>();
        services.TryAddEnumerable(ServiceDescriptor.Singleton<IPostConfigureOptions<Microsoft.AspNetCore.Http.Json.JsonOptions>, ClusterHttpJsonSetup>());
        services.TryAddSingleton<HttpAnswers>();
        AddRefusedRequestExceptionHandler(services);
        services.TryAddSingleton<AcknowledgementStep>();
        services.TryAddEnumerable(ServiceDescriptor.Singleton<IFenceParticipant, HttpFenceParticipant>());
        services.TryAddEnumerable(ServiceDescriptor.Singleton<IDrainParticipant, HttpDrainCloseParticipant>());
        services.TryAddEnumerable(ServiceDescriptor.Singleton<IDrainParticipant, HttpDrainAwaitParticipant>());

        services.Configure<MvcOptions>(mvc => mvc.Filters.AddService<AcknowledgementResultFilter>());
        services.TryAddSingleton<AcknowledgementResultFilter>();

        services.Configure<HubOptions>(hubs => hubs.AddFilter<ClusterHubFilter>());
        services.TryAddSingleton<ClusterHubFilter>();
        return services;
    }

    private static void AddRequestFlowStartupFilter(IServiceCollection services)
    {
        if (!services.Any(d => d is { IsKeyedService: false } && d.ServiceType == typeof(IStartupFilter)
            && d.ImplementationType == typeof(HttpRequestFlowStartupFilter)))
        {
            services.Insert(0, ServiceDescriptor.Singleton<IStartupFilter, HttpRequestFlowStartupFilter>());
        }
    }

    private static void AddRefusedRequestExceptionHandler(IServiceCollection services)
    {
        if (!services.Any(d => d is { IsKeyedService: false } && d.ServiceType == typeof(Microsoft.AspNetCore.Diagnostics.IExceptionHandler) && d.ImplementationType == typeof(RefusedRequestExceptionHandler)))
        {
            services.Insert(0, ServiceDescriptor.Singleton<Microsoft.AspNetCore.Diagnostics.IExceptionHandler, RefusedRequestExceptionHandler>());
        }
    }

    /// <summary>Adds the readiness check (<see cref="Microsoft.Extensions.Diagnostics.HealthChecks.IHealthCheck"/>): healthy while the replica is serving and no readiness reason is raised.</summary>
    /// <param name="builder">The health checks builder.</param>
    /// <param name="name">The check's name.</param>
    /// <param name="tags">Tags to select it by, for a readiness endpoint's predicate.</param>
    public static IHealthChecksBuilder AddReeveClusterReadiness(this IHealthChecksBuilder builder, string name = "reeve-cluster", IEnumerable<string>? tags = null)
    {
        ArgumentNullException.ThrowIfNull(builder);
        return builder.AddCheck<ReadinessCheck>(name, failureStatus: null, tags ?? []);
    }
}

internal sealed class HttpOptionsValidation : IValidateOptions<ReeveClusterHttpOptions>
{
    public ValidateOptionsResult Validate(string? name, ReeveClusterHttpOptions options)
    {
        var problems = options.Problems();
        return problems.Count == 0 ? ValidateOptionsResult.Success : ValidateOptionsResult.Fail(problems);
    }
}

internal sealed class HttpRequestFlowStartupFilter : IStartupFilter
{
    public Action<IApplicationBuilder> Configure(Action<IApplicationBuilder> next) => app =>
    {
        app.Use(nextPipeline => async context =>
        {
            using (HttpRequestFlow.Enter())
            {
                await nextPipeline(context).ConfigureAwait(false);
            }
        });
        next(app);
    };
}
