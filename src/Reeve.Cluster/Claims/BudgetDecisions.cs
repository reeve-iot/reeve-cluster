namespace Reeve.Cluster.Claims;

internal static class BudgetDecisions
{
    public static BudgetResult Verdict(BudgetStatement statement, bool cancellationRequestedBeforeReturn) => (statement, cancellationRequestedBeforeReturn) switch
    {
        (_, true) => new BudgetResult.Unknown(),
        ({ Inserted: false }, false) => new BudgetResult.AlreadyCounted(),
        ({ Used: { } used }, false) => new BudgetResult.Granted(used),
        _ => new BudgetResult.Exhausted(),
    };
}
