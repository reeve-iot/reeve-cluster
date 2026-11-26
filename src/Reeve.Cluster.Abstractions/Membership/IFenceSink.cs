namespace Reeve.Cluster;

/// <summary>
/// The application's port for fence step 4: it aborts the client connections that stay open — WebSocket and SignalR
/// connections, streaming responses. A required port: an application with no such connections
/// says so by registering <see cref="NoFenceSink"/>. Started at step 4 and not awaited past <c>StopBudget</c>.
/// </summary>
public interface IFenceSink
{
    /// <summary>Aborts the connections that stay open.</summary>
    ValueTask AbortConnectionsAsync(CancellationToken cancellationToken);
}

/// <summary>The sink of an application that holds no connection open past a request.</summary>
public sealed class NoFenceSink : IFenceSink
{
    /// <inheritdoc/>
    public ValueTask AbortConnectionsAsync(CancellationToken cancellationToken) => ValueTask.CompletedTask;
}
