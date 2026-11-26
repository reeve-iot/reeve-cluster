using Reeve.Cluster.Claims;

namespace Reeve.Cluster.Slots;

internal sealed class SlotScopes
{
    private readonly ClaimScopes byKey = new();
    private readonly ClaimScopes bySlot = new();

    public IDisposable Open(string key, string id)
    {
        var slot = bySlot.Open(SlotName(key, id));
        try
        {
            return new Both(byKey.Open(key), slot);
        }
        catch
        {
            slot.Dispose();
            throw;
        }
    }

    public bool IsKeyOpen(string key) => byKey.IsOpen(key);

    public bool IsOpen(string key, string id) => bySlot.IsOpen(SlotName(key, id));

    private static string SlotName(string key, string id) => $"{key.Length}:{key}{id}";

    private sealed class Both(IDisposable key, IDisposable slot) : IDisposable
    {
        public void Dispose()
        {
            try
            {
                slot.Dispose();
            }
            finally
            {
                key.Dispose();
            }
        }
    }
}
