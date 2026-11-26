namespace Int;

/// <summary>
/// Waits on a state, never on a fixed sleep: a condition is polled until it holds, with a safety bound that only a hung test
/// reaches. A runner that stalls for seconds makes the poll longer, not the test wrong.
/// </summary>
internal static class Poll
{
    /// <summary>The bound a poll gives up at: far above any wait a slow runner needs.</summary>
    public static readonly TimeSpan Bound = TimeSpan.FromSeconds(30);

    /// <summary>Returns once <paramref name="condition"/> holds; fails the test, naming <paramref name="what"/>, when it does not within <paramref name="bound"/>.</summary>
    public static async Task UntilAsync(Func<bool> condition, string what, TimeSpan? bound = null)
    {
        var until = DateTime.UtcNow + (bound ?? Bound);
        while (!condition())
        {
            if (DateTime.UtcNow > until)
            {
                throw new TimeoutException($"Not reached: {what}.");
            }

            await Task.Delay(10);
        }
    }

    /// <summary>As <see cref="UntilAsync(Func{bool}, string, TimeSpan?)"/> for a condition that reads a database or a task.</summary>
    public static async Task UntilAsync(Func<Task<bool>> condition, string what, TimeSpan? bound = null)
    {
        var until = DateTime.UtcNow + (bound ?? Bound);
        while (!await condition())
        {
            if (DateTime.UtcNow > until)
            {
                throw new TimeoutException($"Not reached: {what}.");
            }

            await Task.Delay(10);
        }
    }
}
