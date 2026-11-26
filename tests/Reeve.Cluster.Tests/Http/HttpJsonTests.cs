using System.Text.Json;
using System.Text.Json.Serialization.Metadata;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;
using Reeve.Cluster;
using Reeve.Cluster.AspNetCore;
using JsonOptions = Microsoft.AspNetCore.Http.Json.JsonOptions;

namespace Unit.Http;

public sealed class HttpJsonTests
{
    public static TheoryData<string> Applications => ["default", "snake_case", "no_reflection"];

    [Theory]
    [MemberData(nameof(Applications))]
    public void The_skip_answers_are_written_byte_for_byte_as_the_anonymous_bodies_were(string application)
    {
        var context = Context(application);

        Assert.Equal(Reference(new { skipId = 41L }, application), Serialize(new SkipOpenedBody(41), context));
        Assert.Equal(Reference(new { untilSeq = 42L }, application), Serialize(new SkipClosedBody(42), context));
    }

    [Theory]
    [MemberData(nameof(Applications))]
    public void The_error_envelope_is_written_byte_for_byte_as_before(string application)
    {
        var problem = new ProblemDetails { Status = 409, Title = "skip_covers_acknowledged", Detail = "A skip must start above acked_seq 7." };
        problem.Extensions["errorCode"] = "skip_covers_acknowledged";
        problem.Extensions["ackedSeq"] = 7L;

        Assert.Equal(Reference(problem, application), Serialize(problem, Context(application)));
    }

    [Theory]
    [MemberData(nameof(Applications))]
    public void The_skip_requests_are_read_as_before(string application)
    {
        var options = Options(application).SerializerOptions;
        var open = JsonSerializer.Deserialize("""{"aggregateType":"devices","fromSeq":9,"reason":"bad row","openedBy":"op"}""", (JsonTypeInfo<SkipOpenBody>)options.GetTypeInfo(typeof(SkipOpenBody)));
        var close = JsonSerializer.Deserialize("{}", (JsonTypeInfo<SkipCloseBody>)options.GetTypeInfo(typeof(SkipCloseBody)));

        var reference = ReferenceOptions(application);

        Assert.Equal(JsonSerializer.Deserialize<SkipOpenBody>("""{"aggregateType":"devices","fromSeq":9,"reason":"bad row","openedBy":"op"}""", reference), open);
        Assert.Equal(JsonSerializer.Deserialize<SkipCloseBody>("{}", reference), close);
    }

    [Fact]
    public void The_package_adds_its_metadata_after_the_applications_resolvers()
    {
        using var provider = new ServiceCollection()
            .AddReeveClusterAspNetCore()
            .BuildServiceProvider();

        var chain = provider.GetRequiredService<IOptions<JsonOptions>>().Value.SerializerOptions.TypeInfoResolverChain;

        Assert.Equal(2, chain.Count);
        Assert.IsType<DefaultJsonTypeInfoResolver>(chain[0]);
        Assert.Same(ClusterHttpJsonContext.Default, chain[1]);
    }

    [Fact]
    public void The_status_body_is_written_byte_for_byte_as_the_anonymous_envelope_was()
    {
        using var values = JsonDocument.Parse("""{"cursorSeq":12,"failingType":null,"nested":{"a":[1,2.5,"x"]}}""");
        var read = new ClusterStatusRead
        {
            ReadAt = new DateTimeOffset(2026, 11, 9, 10, 0, 0, 5, TimeSpan.Zero).AddTicks(3),
            Members =
            [
                new ClusterMemberStatus
                {
                    ReplicaId = "replica-a", Epoch = 4, State = "Serving", LeaseExpiresAt = DateTimeOffset.UnixEpoch, LeaseValid = true,
                    AppliedSeq = 99, ServingSince = new DateTimeOffset(2026, 11, 9, 9, 0, 0, TimeSpan.FromHours(2)), Version = "3.4.0+abc", Protocol = 3,
                    Sections = [new ClusterStatusSection { Name = "applier", Values = values.RootElement.Clone(), Age = TimeSpan.FromMilliseconds(1234.5) }],
                    Fences = [new ClusterFenceRecord { Epoch = 3, Trigger = "lease_lost", FencedAt = DateTimeOffset.UnixEpoch, Span = TimeSpan.FromSeconds(0.1 + 0.2) }],
                },
                new ClusterMemberStatus
                {
                    ReplicaId = "replica-<b>", Epoch = 5, State = "CatchingUp", LeaseExpiresAt = DateTimeOffset.MaxValue, LeaseValid = false,
                    AppliedSeq = 0, ServingSince = null, Version = "3.3.0", Protocol = 2, Sections = [], Fences = [],
                },
            ],
        };

        var reference = JsonSerializer.Serialize(
            new
            {
                readAt = read.ReadAt,
                members = read.Members.Select(m => new
                {
                    replicaId = m.ReplicaId,
                    epoch = m.Epoch,
                    state = m.State,
                    leaseExpiresAt = m.LeaseExpiresAt,
                    leaseValid = m.LeaseValid,
                    appliedSeq = m.AppliedSeq,
                    servingSince = m.ServingSince,
                    version = m.Version,
                    protocol = m.Protocol,
                    sections = m.Sections.Select(s => new { name = s.Name, ageSeconds = s.Age.TotalSeconds, values = s.Values }),
                    fences = m.Fences.Select(f => new { epoch = f.Epoch, trigger = f.Trigger, fencedAt = f.FencedAt, spanSeconds = f.Span.TotalSeconds }),
                }),
            },
            new JsonSerializerOptions(JsonSerializerDefaults.Web));

        Assert.Equal(reference, JsonSerializer.Serialize(StatusBody.Of(read), ClusterStatusJsonContext.Default.StatusBody));
    }

    private static string Serialize<T>(T value, HttpContext context) => JsonSerializer.Serialize(value, ClusterHttpJson.Info<T>(context));

    private static string Reference<T>(T value, string application) => JsonSerializer.Serialize(value, ReferenceOptions(application));

    private static JsonSerializerOptions ReferenceOptions(string application) =>
        application == "snake_case" ? Options("snake_case").SerializerOptions : new JsonOptions().SerializerOptions;

    private static DefaultHttpContext Context(string application) => new()
    {
        RequestServices = new ServiceCollection()
            .AddSingleton(Microsoft.Extensions.Options.Options.Create(Options(application)))
            .BuildServiceProvider(),
    };

    private static JsonOptions Options(string application)
    {
        var options = new JsonOptions();
        if (application == "snake_case")
        {
            options.SerializerOptions.PropertyNamingPolicy = JsonNamingPolicy.SnakeCaseLower;
        }
        else if (application == "no_reflection")
        {
            options.SerializerOptions.TypeInfoResolver = JsonTypeInfoResolver.Combine();
        }

        new ClusterHttpJsonSetup().PostConfigure(Microsoft.Extensions.Options.Options.DefaultName, options);
        return options;
    }
}
