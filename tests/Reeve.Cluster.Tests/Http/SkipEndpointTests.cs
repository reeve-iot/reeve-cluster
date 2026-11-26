using System.Net;
using System.Net.Http.Json;
using System.Text.Json;
using System.Security.Claims;
using System.Text.Encodings.Web;
using Microsoft.AspNetCore.Authentication;
using Microsoft.AspNetCore.Authorization;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace Unit.Http;

public sealed class SkipEndpointTests : IAsyncDisposable
{
    private readonly HttpRig rig = HttpRig.Create();

    public ValueTask DisposeAsync() => rig.DisposeAsync();

    [Fact]
    public async Task An_unprotected_skip_open_answers_403_without_calling_the_change_skip()
    {
        await rig.PublishAsync(5);
        rig.ToServing();

        var response = await rig.Client.PostAsync("/skip-unprotected", JsonContent.Create(new { aggregateType = "Widget", fromSeq = 6, reason = "repair", openedBy = "operator" }));

        Assert.Equal(HttpStatusCode.Forbidden, response.StatusCode);
        using var body = JsonDocument.Parse(await response.Content.ReadAsStringAsync());
        Assert.Equal("skip_endpoint_unprotected", body.RootElement.GetProperty("errorCode").GetString());
        Assert.Equal(0, rig.Skips.Opens);
    }

    [Fact]
    public async Task Allow_anonymous_skip_group_reaches_the_open_handler()
    {
        await rig.PublishAsync(5);
        rig.ToServing();

        var response = await rig.Client.PostAsync("/skip-anonymous", JsonContent.Create(new { aggregateType = "Widget", fromSeq = 6, reason = "repair", openedBy = "operator" }));

        Assert.Equal(HttpStatusCode.Created, response.StatusCode);
        Assert.Equal(1, rig.Skips.Opens);
    }

    [Fact]
    public async Task An_unprotected_skip_close_answers_403_without_calling_the_change_skip()
    {
        await rig.PublishAsync(5);
        rig.ToServing();

        var response = await rig.Client.PostAsync("/skip-unprotected/9/close", JsonContent.Create(new { closedBy = "operator" }));

        Assert.Equal(HttpStatusCode.Forbidden, response.StatusCode);
        Assert.Equal(0, rig.Skips.Closes);
    }

    [Fact]
    public async Task A_fallback_policy_counts_as_protection_for_the_skip_guard()
    {
        await using var fallbackRig = HttpRig.Create(before: services => services.AddAuthorization(options =>
            options.FallbackPolicy = new AuthorizationPolicyBuilder().RequireAssertion(_ => true).Build()));
        await fallbackRig.PublishAsync(5);
        fallbackRig.ToServing();

        var response = await fallbackRig.Client.PostAsync("/skip-unprotected", JsonContent.Create(new { aggregateType = "Widget", fromSeq = 6, reason = "repair", openedBy = "operator" }));

        Assert.Equal(HttpStatusCode.Created, response.StatusCode);
        Assert.Equal(1, fallbackRig.Skips.Opens);
    }

    [Fact]
    public async Task An_authenticated_request_to_a_require_authorization_group_passes_the_skip_guard()
    {
        await using var authorizedRig = HttpRig.Create(
            before: services => services.AddAuthentication("test").AddScheme<AuthenticationSchemeOptions, TestAuthenticationHandler>("test", _ => { }),
            beforeFence: app =>
            {
                app.UseRouting();
                app.UseAuthentication();
                app.UseAuthorization();
            });
        await authorizedRig.PublishAsync(5);
        authorizedRig.ToServing();

        var response = await authorizedRig.Client.PostAsync("/skip-authorized", JsonContent.Create(new { aggregateType = "Widget", fromSeq = 6, reason = "repair", openedBy = "ignored" }));

        Assert.Equal(HttpStatusCode.Created, response.StatusCode);
        Assert.Equal(1, authorizedRig.Skips.Opens);
    }

    private sealed class TestAuthenticationHandler(
        IOptionsMonitor<AuthenticationSchemeOptions> options, ILoggerFactory logger, UrlEncoder encoder)
        : AuthenticationHandler<AuthenticationSchemeOptions>(options, logger, encoder)
    {
        protected override Task<AuthenticateResult> HandleAuthenticateAsync()
        {
            var identity = new ClaimsIdentity([new Claim(ClaimTypes.Name, "operator")], Scheme.Name);
            var ticket = new AuthenticationTicket(new ClaimsPrincipal(identity), Scheme.Name);
            return Task.FromResult(AuthenticateResult.Success(ticket));
        }
    }
}
