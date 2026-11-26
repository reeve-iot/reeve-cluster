namespace Harness.Linearizability;

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
