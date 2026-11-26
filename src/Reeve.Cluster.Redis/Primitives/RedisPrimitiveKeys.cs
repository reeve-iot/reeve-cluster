namespace Reeve.Cluster.Redis;

/// <summary>The Redis keys of the primitives: a primitive's keys for one application key share one hash tag.</summary>
public static class RedisPrimitiveKeys
{
    /// <summary>The hash tag of a claim, <c>{&lt;prefix&gt;claim:&lt;key&gt;}</c>: the claim key and the keys of the caller's effect share it.</summary>
    public static string ClaimHashTag(string prefix, string key) => "{" + prefix + "claim:" + key + "}";

    /// <summary>The hash tag of a single-flight, <c>{&lt;prefix&gt;flight:&lt;key&gt;}</c>: its claim key and result share it.</summary>
    public static string FlightHashTag(string prefix, string key) => "{" + prefix + "flight:" + key + "}";

    /// <summary>The hash tag of a budget, <c>{&lt;prefix&gt;budget:&lt;key&gt;}</c>: the sets of its windows share it.</summary>
    public static string BudgetHashTag(string prefix, string key) => "{" + prefix + "budget:" + key + "}";

    /// <summary>The hash tag of a counter, <c>{&lt;prefix&gt;counter:&lt;key&gt;}</c>.</summary>
    public static string CounterHashTag(string prefix, string key) => "{" + prefix + "counter:" + key + "}";

    /// <summary>The hash tag of a slot's key, <c>{&lt;prefix&gt;slot:&lt;key&gt;}</c>: its <c>:held</c> set, its <c>:id:&lt;id&gt;</c> keys and the keys of the caller's effect share it.</summary>
    public static string SlotHashTag(string prefix, string key) => "{" + prefix + "slot:" + key + "}";
}
