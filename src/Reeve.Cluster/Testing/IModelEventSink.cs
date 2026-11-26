namespace Reeve.Cluster;

internal readonly record struct ModelEvent(string Action, IReadOnlyDictionary<string, object?>? State)
{
    public ModelEvent(string action)
        : this(action, null)
    {
    }
}

internal interface IModelEventSink
{
    void Emit(in ModelEvent modelEvent);
}

internal sealed class NoModelEvents : IModelEventSink
{
    public static NoModelEvents Instance { get; } = new();

    private NoModelEvents()
    {
    }

    public void Emit(in ModelEvent modelEvent)
    {
    }
}
