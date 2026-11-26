namespace Harness.Mixed;

[AttributeUsage(AttributeTargets.Method)]
public sealed class MixedVersionFactAttribute : FactAttribute
{
    public MixedVersionFactAttribute()
    {
        if (string.IsNullOrWhiteSpace(MixedRig.OldRef))
        {
            Skip = $"Runs in the mixed-version lane: {MixedRig.OldRefVariable} names the older release.";
        }
    }
}
