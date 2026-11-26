using System.Security.Claims;
using System.Security.Cryptography;
using System.Text;
using System.Text.Encodings.Web;
using Microsoft.AspNetCore.Authentication;
using Microsoft.Extensions.Options;

namespace Reeve.Cluster.Sample;

public sealed class TokenService(IConfiguration configuration)
{
    private readonly byte[] key = Encoding.UTF8.GetBytes(
        configuration["Sample:SigningKey"] ?? throw new InvalidOperationException("Sample:SigningKey is required."));

    private readonly byte[] password = Encoding.UTF8.GetBytes(
        configuration["Sample:OperatorPassword"] ?? throw new InvalidOperationException("Sample:OperatorPassword is required."));

    public bool PasswordMatches(string? candidate) =>
        CryptographicOperations.FixedTimeEquals(SHA256.HashData(password), SHA256.HashData(Encoding.UTF8.GetBytes(candidate ?? "")));

    public string Issue(string user, DateTimeOffset expires)
    {
        var payload = $"{Convert.ToBase64String(Encoding.UTF8.GetBytes(user))}.{expires.ToUnixTimeSeconds()}";
        return $"{payload}.{Sign(payload)}";
    }

    public string? Validate(string token, DateTimeOffset now)
    {
        var parts = token.Split('.');
        if (parts.Length != 3 || !CryptographicOperations.FixedTimeEquals(Encoding.ASCII.GetBytes(Sign($"{parts[0]}.{parts[1]}")), Encoding.ASCII.GetBytes(parts[2])))
        {
            return null;
        }

        if (!long.TryParse(parts[1], out var expires) || DateTimeOffset.FromUnixTimeSeconds(expires) <= now)
        {
            return null;
        }

        return Encoding.UTF8.GetString(Convert.FromBase64String(parts[0]));
    }

    private string Sign(string payload) => Convert.ToBase64String(HMACSHA256.HashData(key, Encoding.UTF8.GetBytes(payload)));
}

public sealed class TokenAuthenticationHandler(
    IOptionsMonitor<AuthenticationSchemeOptions> options, ILoggerFactory logger, UrlEncoder encoder, TokenService tokens, TimeProvider time)
    : AuthenticationHandler<AuthenticationSchemeOptions>(options, logger, encoder)
{
    public const string SchemeName = "Token";

    protected override Task<AuthenticateResult> HandleAuthenticateAsync()
    {
        var header = Request.Headers.Authorization.ToString();
        if (!header.StartsWith("Bearer ", StringComparison.Ordinal))
        {
            return Task.FromResult(AuthenticateResult.NoResult());
        }

        var user = tokens.Validate(header["Bearer ".Length..], time.GetUtcNow());
        if (user is null)
        {
            return Task.FromResult(AuthenticateResult.Fail("Invalid or expired token."));
        }

        var identity = new ClaimsIdentity([new Claim(ClaimTypes.Name, user)], SchemeName);
        return Task.FromResult(AuthenticateResult.Success(new AuthenticationTicket(new ClaimsPrincipal(identity), SchemeName)));
    }
}
