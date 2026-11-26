using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace Reeve.Cluster.Harness.Replicas;

internal sealed class HarnessLoggerProvider(string replicaId, Action<string> sink, LogLevel minimum = LogLevel.Information) : ILoggerProvider
{
    public ILogger CreateLogger(string categoryName) => new Logger(replicaId, categoryName, sink, minimum);

    public void Dispose()
    {
    }

    private sealed class Logger(string replicaId, string category, Action<string> sink, LogLevel minimum) : ILogger
    {
        public IDisposable? BeginScope<TState>(TState state)
            where TState : notnull => null;

        public bool IsEnabled(LogLevel logLevel) => logLevel >= minimum;

        public void Log<TState>(LogLevel logLevel, EventId eventId, TState state, Exception? exception, Func<TState, Exception?, string> formatter)
        {
            if (!IsEnabled(logLevel))
            {
                return;
            }

            try
            {
                sink($"[{replicaId}] {logLevel} {category}: {formatter(state, exception)}{(exception is null ? string.Empty : " " + exception)}");
            }
            catch (InvalidOperationException)
            {
            }
        }
    }
}

internal sealed class HarnessHostLifetime : IHostLifetime
{
    public Task WaitForStartAsync(CancellationToken cancellationToken) => Task.CompletedTask;

    public Task StopAsync(CancellationToken cancellationToken) => Task.CompletedTask;
}
