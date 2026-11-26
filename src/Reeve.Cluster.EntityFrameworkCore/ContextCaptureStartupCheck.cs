using System.Reflection;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Infrastructure;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;

namespace Reeve.Cluster.EntityFrameworkCore;

internal sealed record DbContextServiceCollection(IServiceCollection Services);

internal sealed class ContextCaptureStartupCheck(
    DbContextServiceCollection registrations,
    IServiceScopeFactory scopes,
    IClusterMapping mapping) : IHostedLifecycleService
{
    public Task StartingAsync(CancellationToken cancellationToken) => CheckAsync(cancellationToken);

    public Task StartAsync(CancellationToken cancellationToken) => Task.CompletedTask;

    public Task StartedAsync(CancellationToken cancellationToken) => Task.CompletedTask;

    public Task StoppingAsync(CancellationToken cancellationToken) => Task.CompletedTask;

    public Task StopAsync(CancellationToken cancellationToken) => Task.CompletedTask;

    public Task StoppedAsync(CancellationToken cancellationToken) => Task.CompletedTask;

    private async Task CheckAsync(CancellationToken cancellationToken)
    {
        var contextTypes = registrations.Services
            .Where(descriptor => !descriptor.IsKeyedService
                && !descriptor.ServiceType.IsGenericTypeDefinition
                && descriptor.ServiceType.IsGenericType
                && descriptor.ServiceType.GetGenericTypeDefinition() == typeof(DbContextOptions<>))
            .Select(descriptor => descriptor.ServiceType.GenericTypeArguments[0])
            .Distinct()
            .ToArray();

        var scope = scopes.CreateAsyncScope();
        await using (scope.ConfigureAwait(false))
        {
            foreach (var contextType in contextTypes)
            {
                cancellationToken.ThrowIfCancellationRequested();
                await CheckContextAsync(scope.ServiceProvider, contextType, cancellationToken).ConfigureAwait(false);
            }
        }
    }

    private async Task CheckContextAsync(IServiceProvider services, Type contextType, CancellationToken cancellationToken)
    {
        var optionsType = typeof(DbContextOptions<>).MakeGenericType(contextType);
        object options;
        try
        {
            options = services.GetRequiredService(optionsType);
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            throw;
        }
        catch (Exception exception)
        {
            throw Refusal(contextType,
                $"The DbContextOptions<{contextType.Name}> for {contextType.FullName} must resolve outside an HTTP request at host start.",
                Unwrap(exception));
        }

        if (HasCapture((IDbContextOptions)options))
        {
            return;
        }

        DbContext context;
        try
        {
            context = await CreateContextAsync(services, contextType, cancellationToken).ConfigureAwait(false);
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            throw;
        }
        catch (Exception exception)
        {
            throw Refusal(contextType, $"The start check could not construct context {contextType.FullName}.", Unwrap(exception));
        }

        Type? mappedType;
        await using (context.ConfigureAwait(false))
        {
            if (HasCapture(context.GetService<IDbContextOptions>()))
            {
                return;
            }

            try
            {
                mappedType = context.Model.GetEntityTypes()
                    .FirstOrDefault(entity => mapping.Find(entity.ClrType) is not null)?.ClrType;
            }
            catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
            {
                throw;
            }
            catch (Exception exception)
            {
                throw Refusal(contextType, $"The start check could not build the model for context {contextType.FullName}.", Unwrap(exception));
            }
        }

        if (mappedType is not null)
        {
            throw Refusal(contextType,
                $"Context {contextType.FullName} has mapped entity {mappedType.FullName} but no ClusterCaptureInterceptor. "
                + "Add UseReeveClusterCapture(services) to its options.");
        }
    }

    private static bool HasCapture(IDbContextOptions options) => options.Extensions
        .OfType<CoreOptionsExtension>()
        .Any(extension => extension.Interceptors?.Any(interceptor => interceptor is ClusterCaptureInterceptor) == true);

    private static async ValueTask<DbContext> CreateContextAsync(
        IServiceProvider services, Type contextType, CancellationToken cancellationToken)
    {
        if (services.GetService(contextType) is DbContext registered)
        {
            return registered;
        }

        var factoryType = typeof(IDbContextFactory<>).MakeGenericType(contextType);
        if (services.GetService(factoryType) is { } factory)
        {
            var method = factoryType.GetMethod(nameof(IDbContextFactory<DbContext>.CreateDbContextAsync), [typeof(CancellationToken)])!;
            var task = (Task)method.Invoke(factory, [cancellationToken])!;
            await task.ConfigureAwait(false);
            return (DbContext)task.GetType().GetProperty("Result")!.GetValue(task)!;
        }

        var optionsType = typeof(DbContextOptions<>).MakeGenericType(contextType);
        var options = services.GetRequiredService(optionsType);
        return (DbContext)ActivatorUtilities.CreateInstance(services, contextType, options);
    }

    private static Exception Unwrap(Exception exception) =>
        exception is TargetInvocationException { InnerException: { } inner } ? inner : exception;

    private static ClusterConfigurationException Refusal(Type contextType, string message, Exception? inner = null) =>
        inner is null
            ? new ClusterConfigurationException(ClusterConfigurationReason.ContextWithoutCapture, message)
            : new ClusterConfigurationException(ClusterConfigurationReason.ContextWithoutCapture, message, inner);
}
