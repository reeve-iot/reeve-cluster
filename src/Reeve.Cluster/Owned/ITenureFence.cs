namespace Reeve.Cluster.Owned;

internal interface ITenureFence
{
    Task OnAcquiringAsync(OwnedContext tenure, CancellationToken stop);

    Task OnFencedAsync(OwnedContext tenure, CancellationToken cancellationToken);
}

internal sealed class NoTenureFence : ITenureFence
{
    public Task OnAcquiringAsync(OwnedContext tenure, CancellationToken stop) => Task.CompletedTask;

    public Task OnFencedAsync(OwnedContext tenure, CancellationToken cancellationToken) => Task.CompletedTask;
}
