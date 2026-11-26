using Xunit;

namespace Reeve.Cluster.Harness.Stack;

public sealed class StackFixture : IAsyncLifetime
{
    private readonly ClusterStack stack = new();

    internal ClusterStack Stack => stack;

    public Task InitializeAsync() => stack.InitializeAsync();

    public Task DisposeAsync() => stack.DisposeAsync();
}
