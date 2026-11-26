using Reeve.Cluster.Harness.Stack;

namespace Harness.SelfTests;

[CollectionDefinition(Name)]
public sealed class StackCollection : ICollectionFixture<StackFixture>
{
    public const string Name = "harness-stack";
}
