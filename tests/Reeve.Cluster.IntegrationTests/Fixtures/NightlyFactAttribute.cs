namespace Int.Fixtures;

/// <summary>
/// A test the nightly lane runs: it carries <c>Category=Nightly</c> (the lane's filter) and is skipped unless <c>REEVE_NIGHTLY=true</c>, so
/// the integration lane, which runs every test, leaves it out. A copy of the harness tests' attribute.
/// </summary>
[AttributeUsage(AttributeTargets.Method)]
public sealed class NightlyFactAttribute : FactAttribute
{
    public NightlyFactAttribute()
    {
        if (Environment.GetEnvironmentVariable("REEVE_NIGHTLY") != "true")
        {
            Skip = "Runs in the nightly lane (REEVE_NIGHTLY=true).";
        }
    }
}
