using Microsoft.AspNetCore.Authentication;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Diagnostics.HealthChecks;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.Diagnostics.HealthChecks;
using Microsoft.Extensions.Options;
using Reeve.Cluster;
using Reeve.Cluster.AspNetCore;
using Reeve.Cluster.Sample;

var builder = WebApplication.CreateBuilder(args);

var connectionString = builder.Configuration["Cluster:ConnectionString"]
    ?? throw new InvalidOperationException("Cluster:ConnectionString is required.");

builder.Services.AddReeveCluster(
    new ClusterPorts([typeof(FlagReconciler), typeof(QuotaReconciler), typeof(SettingReconciler)], typeof(AspNetCoreFenceSink)),
    options => builder.Configuration.GetSection(ClusterOptions.SectionName).Bind(options));

builder.Services.AddReeveClusterEntityFrameworkCore(mapping =>
{
    mapping.Entity<Flag>("flag").Key(f => f.Name).AllColumns();
    mapping.Entity<Quota>("quota").Key(q => q.Tenant).Columns(q => q.DailyLimit);
    mapping.Entity<Setting>("setting").Key(s => s.Name).AllColumns();
});
builder.Services.AddDbContext<SampleDb>((services, options) => options.UseNpgsql(connectionString).UseReeveClusterCapture(services));

builder.Services.AddReeveClusterAspNetCore();

var mixedFleet = builder.Configuration.GetValue<bool>("Sample:MixedFleet");
if (mixedFleet)
{
    MixedFleet.AddServices(builder);
}

builder.Services.AddHealthChecks().AddReeveClusterReadiness(tags: ["ready"]);
builder.Services.AddSignalR();

builder.Services.AddSingleton<TokenService>();
builder.Services.AddAuthentication(TokenAuthenticationHandler.SchemeName).AddScheme<AuthenticationSchemeOptions, TokenAuthenticationHandler>(TokenAuthenticationHandler.SchemeName, null);
builder.Services.AddAuthorization();

var app = builder.Build();

if (args is ["--lower-floor", var floorVersion])
{
    var lowering = await app.Services.GetRequiredService<IClusterFloorCommand>().LowerVersionFloorAsync(floorVersion, "mixed-version-harness");
    Console.WriteLine($"harness: floor-lowering {lowering}");
    Environment.ExitCode = lowering == FloorLowering.Lowered ? 0 : 1;
    return;
}

await app.Services.GetRequiredService<IClusterMigrationRunner>().MigrateAsync();
await SampleSchema.EnsureAsync(connectionString, CancellationToken.None);

app.UseReeveCluster();
app.UseAuthentication();
app.UseAuthorization();

app.MapPost("/auth/sign-in", (SignInRequest request, TokenService tokens, TimeProvider time) =>
        tokens.PasswordMatches(request.Password)
            ? Results.Ok(new { token = tokens.Issue(request.User, time.GetUtcNow().AddHours(1)) })
            : Results.Json(new { error = "invalid_credentials" }, statusCode: StatusCodes.Status401Unauthorized))
    .AllowWhenFenced().SkipReadIndex();
app.MapHealthChecks("/health/ready", new HealthCheckOptions { Predicate = check => check.Tags.Contains("ready") }).AllowWhenFenced().SkipReadIndex();
app.MapHealthChecks("/health/live", new HealthCheckOptions { Predicate = _ => false }).AllowWhenFenced().SkipReadIndex();

app.MapReeveClusterStatus("/status").RequireAuthorization();

app.MapGet("/phase", async (HealthCheckService health, IOptions<ClusterOptions> options, CancellationToken cancellationToken) =>
{
    var report = await health.CheckHealthAsync(check => check.Tags.Contains("ready"), cancellationToken);
    return Results.Ok(new
    {
        replicaId = options.Value.ReplicaId,
        phase = report.Entries.Values.Select(e => e.Data.TryGetValue("phase", out var phase) ? phase as string : null).FirstOrDefault(p => p is not null),
    });
}).AllowWhenFenced().SkipReadIndex();

app.MapReeveClusterSkips("/ops/skips").RequireAuthorization();

app.MapGet("/flags/{name}", (string name, IClusterScopeAccessor scopes) =>
    scopes.Current!.PinnedGeneration!.Get<FlagState>("flag").Flags.TryGetValue(name, out var enabled)
        ? Results.Ok(new { name, enabled })
        : Results.NotFound());
app.MapGet("/quotas/{tenant}", (string tenant, IClusterScopeAccessor scopes) =>
    scopes.Current!.PinnedGeneration!.Get<QuotaState>("quota").Limits.TryGetValue(tenant, out var limit)
        ? Results.Ok(new { tenant, dailyLimit = limit })
        : Results.NotFound());

app.MapGet("/config", (IClusterScopeAccessor scopes) => ConfigView.Of(scopes));

var writes = app.MapGroup("").AcknowledgeWrites();

writes.MapPut("/flags/{name}", async (string name, FlagBody body, string? hold, SampleDb db, CancellationToken cancellationToken) =>
{
    await HoldAsync(db, hold, cancellationToken);
    await using var transaction = await db.Database.BeginTransactionAsync(cancellationToken);
    await UpsertFlagAsync(db, name, body.Enabled, cancellationToken);
    await transaction.CommitAsync(cancellationToken);
    return Results.Ok(new { name, enabled = body.Enabled });
});

writes.MapPut("/quotas/{tenant}", async (string tenant, QuotaBody body, SampleDb db, CancellationToken cancellationToken) =>
{
    var quota = await db.Quotas.FindAsync([tenant], cancellationToken);
    if (quota is null)
    {
        db.Quotas.Add(new Quota { Tenant = tenant, DailyLimit = body.DailyLimit });
    }
    else
    {
        quota.DailyLimit = body.DailyLimit;
    }

    await db.SaveChangesAsync(cancellationToken);
    return Results.Ok(new { tenant, dailyLimit = body.DailyLimit });
});

writes.MapPost("/config", async (ConfigChange change, SampleDb db, CancellationToken cancellationToken) =>
{
    await using var transaction = await db.Database.BeginTransactionAsync(cancellationToken);
    foreach (var (tenant, limit) in (change.Quotas ?? []).OrderBy(pair => pair.Key, StringComparer.Ordinal))
    {
        var quota = await db.Quotas.FindAsync([tenant], cancellationToken);
        if (quota is null)
        {
            db.Quotas.Add(new Quota { Tenant = tenant, DailyLimit = limit });
        }
        else
        {
            quota.DailyLimit = limit;
        }
    }

    foreach (var (name, value) in (change.Settings ?? []).OrderBy(pair => pair.Key, StringComparer.Ordinal))
    {
        var setting = await db.Settings.FindAsync([name], cancellationToken);
        if (setting is null)
        {
            db.Settings.Add(new Setting { Name = name, Value = value });
        }
        else
        {
            setting.Value = value;
        }
    }

    await db.SaveChangesAsync(cancellationToken);
    await transaction.CommitAsync(cancellationToken);
    return Results.Ok();
});

writes.MapPost("/flags/import", async (FlagBody[] items, int? pauseMs, string? gate, string? hold, SampleDb db, CancellationToken cancellationToken) =>
{
    await HoldAsync(db, hold, cancellationToken);
    foreach (var (item, index) in items.Select((item, index) => (item, index)))
    {
        if (index > 0 && !string.IsNullOrEmpty(gate))
        {
            await WaitForFlagAsync(db, gate, TimeSpan.FromMilliseconds(Math.Min(pauseMs ?? 60_000, 60_000)), cancellationToken);
        }
        else if (index > 0 && pauseMs is > 0)
        {
            await Task.Delay(Math.Min(pauseMs.Value, 60_000), cancellationToken);
        }

        await using var transaction = await db.Database.BeginTransactionAsync(cancellationToken);
        await UpsertFlagAsync(db, item.Name ?? throw new BadHttpRequestException("name is required"), item.Enabled, cancellationToken);
        await transaction.CommitAsync(cancellationToken);
    }

    return Results.Ok(new { imported = items.Length });
});

app.MapHub<FlagHub>("/hubs/flags");

if (mixedFleet)
{
    MixedFleet.Map(app);
}

app.Run();

static async Task HoldAsync(SampleDb db, string? hold, CancellationToken cancellationToken)
{
    if (string.IsNullOrEmpty(hold))
    {
        return;
    }

    Console.WriteLine($"harness: hold {hold}");
    await WaitForFlagAsync(db, hold, TimeSpan.FromMinutes(1), cancellationToken);
}

static async Task WaitForFlagAsync(SampleDb db, string name, TimeSpan max, CancellationToken cancellationToken)
{
    var until = DateTime.UtcNow + max;
    while (DateTime.UtcNow < until && !await db.Flags.AsNoTracking().AnyAsync(flag => flag.Name == name, cancellationToken))
    {
        await Task.Delay(50, cancellationToken);
    }
}

static async Task UpsertFlagAsync(SampleDb db, string name, bool enabled, CancellationToken cancellationToken)
{
    var flag = await db.Flags.FindAsync([name], cancellationToken);
    if (flag is null)
    {
        db.Flags.Add(new Flag { Name = name, Enabled = enabled });
    }
    else
    {
        flag.Enabled = enabled;
    }

    await db.SaveChangesAsync(cancellationToken);
}

public sealed record SignInRequest(string User, string Password);

public sealed record FlagBody(bool Enabled, string? Name = null);

public sealed record QuotaBody(int DailyLimit);

public sealed record ConfigChange(Dictionary<string, int>? Quotas, Dictionary<string, int>? Settings);

public sealed record ConfigView(IReadOnlyDictionary<string, int> Quotas, IReadOnlyDictionary<string, int> Settings)
{
    public static ConfigView Of(IClusterScopeAccessor scopes)
    {
        var generation = scopes.Current!.PinnedGeneration!;
        return new ConfigView(generation.Get<QuotaState>("quota").Limits, generation.Get<SettingState>("setting").Values);
    }
}
