using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Builder;
using Reeve.Cluster;
using Reeve.Cluster.Core;

namespace Unit.Http;

public sealed class HttpRequestFlowTests
{
    [Fact]
    public async Task Endpoint_and_middleware_before_cluster_see_request_marker()
    {
        var middlewareSawMarker = false;
        await using var rig = HttpRig.Create(
            beforeCluster: app => app.Use(async (context, next) =>
            {
                middlewareSawMarker = HttpRequestFlow.IsInside;
                await next();
            }));
        await Admit(rig);

        Assert.Equal("true", await rig.Client.GetStringAsync("/min/marker"));
        Assert.True(middlewareSawMarker);
    }

    [Fact]
    public async Task Work_resumed_after_response_sees_marker_ended()
    {
        var continuation = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var observed = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
        await using var rig = HttpRig.Create(afterFence: app => app.Use(async (context, next) =>
        {
            if (context.Request.Path != "/min/marker") { await next(); return; }
            _ = Task.Run(async () => { await continuation.Task; observed.SetResult(HttpRequestFlow.IsInside); });
            await context.Response.WriteAsync(HttpRequestFlow.IsInside.ToString().ToLowerInvariant());
        }));
        await Admit(rig);

        Assert.Equal("true", await rig.Client.GetStringAsync("/min/marker"));
        continuation.SetResult();
        Assert.False(await observed.Task.WaitAsync(TimeSpan.FromSeconds(5)));
    }

    private static async Task Admit(HttpRig rig)
    {
        await rig.PublishAsync(5);
        rig.ToCatchingUp();
        rig.Gate.Apply(MemberTrigger.ActivationCommitted);
        rig.Gate.Apply(MemberTrigger.AppliedToActivationHead);
    }

    [Fact]
    public void Outside_request_marker_is_false() => Assert.False(HttpRequestFlow.IsInside);
}
