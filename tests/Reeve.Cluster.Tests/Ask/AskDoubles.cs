using Reeve.Cluster.Ask;

namespace Unit.Ask;

internal sealed class ScriptedDirectory : IAskDirectory
{
    private readonly Queue<AskOwner?> owners = new();
    private readonly Dictionary<string, AskMemberRow?> members = [];

    public AskOwner? Otherwise { get; set; }

    public Exception? Fails { get; set; }

    public Action? OnResolve { get; set; }

    public int OwnerResolutions { get; private set; }

    public int MemberResolutions { get; private set; }

    public ScriptedDirectory Then(AskOwner? owner)
    {
        owners.Enqueue(owner);
        return this;
    }

    public ScriptedDirectory Member(string replicaId, AskMemberRow? row)
    {
        members[replicaId] = row;
        return this;
    }

    public Task<AskOwner?> ResolveOwnerAsync(string kind, string key, CancellationToken cancellationToken)
    {
        OwnerResolutions++;
        OnResolve?.Invoke();
        if (Fails is not null)
        {
            return Task.FromException<AskOwner?>(Fails);
        }

        return Task.FromResult(owners.Count > 0 ? owners.Dequeue() : Otherwise);
    }

    public Task<AskMemberRow?> ResolveMemberAsync(string replicaId, CancellationToken cancellationToken)
    {
        MemberResolutions++;
        OnResolve?.Invoke();
        if (Fails is not null)
        {
            return Task.FromException<AskMemberRow?>(Fails);
        }

        return Task.FromResult(members.GetValueOrDefault(replicaId));
    }
}

internal sealed class ScriptedTransport : IAskTransport
{
    private readonly Queue<DeliveryOutcome> outcomes = new();
    private readonly List<(AskTarget Target, AskDelivery Delivery)> deliveries = [];
    private DeliveryOutcome last = new(DeliveryKind.NotTaken, null);

    public IReadOnlyList<(AskTarget Target, AskDelivery Delivery)> Deliveries => deliveries;

    public ScriptedTransport Then(DeliveryOutcome outcome)
    {
        outcomes.Enqueue(outcome);
        return this;
    }

    public List<TimeSpan?> TakeBounds { get; } = [];

    public Task<DeliveryOutcome> DeliverAsync(AskTarget target, AskDelivery delivery, TimeSpan timeout, CancellationToken cancellationToken) =>
        Deliver(target, delivery, null);

    public Task<DeliveryOutcome> DeliverAsync(AskTarget target, AskDelivery delivery, TimeSpan timeout, TimeSpan takeTimeout, CancellationToken cancellationToken) =>
        Deliver(target, delivery, takeTimeout);

    private Task<DeliveryOutcome> Deliver(AskTarget target, AskDelivery delivery, TimeSpan? takeTimeout)
    {
        TakeBounds.Add(takeTimeout);
        deliveries.Add((target, delivery));
        if (outcomes.Count > 0)
        {
            last = outcomes.Dequeue();
        }

        return Task.FromResult(last);
    }

    public static DeliveryOutcome Declines => new(DeliveryKind.Declined, null);

    public static DeliveryOutcome DeclinesFor(DeclineReason reason) => new(DeliveryKind.Declined, null, reason);

    public static DeliveryOutcome Answers(params byte[] response) => new(DeliveryKind.Answered, new AskAnswer(AskAnswerKind.Response, response, null));
}

internal sealed class ScriptedInbound : IAskInbound
{
    private readonly Queue<Func<AskTaking>> takings = new();
    private readonly List<AskDelivery> received = [];

    public IReadOnlyList<AskDelivery> Received => received;

    public ScriptedInbound Then(Func<AskTaking> taking)
    {
        takings.Enqueue(taking);
        return this;
    }

    public ScriptedInbound Declining() => Then(() => AskTaking.Declined);

    public ScriptedInbound Answering(params byte[] response) =>
        Then(() => AskTaking.Taken(Task.FromResult(new AskAnswer(AskAnswerKind.Response, response, null))));

    public ValueTask<AskTaking> TakeAsync(AskDelivery delivery, CancellationToken answerBy)
    {
        received.Add(delivery);
        return ValueTask.FromResult(takings.Count > 0 ? takings.Dequeue()() : AskTaking.Declined);
    }

    public ValueTask<AskReservation> ReserveAsync(AskDelivery delivery, CancellationToken answerBy) =>
        throw new NotSupportedException("A local dispatch does not reserve.");
}
