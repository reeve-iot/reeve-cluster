namespace Reeve.Cluster.Core;

internal readonly record struct Cursor(long Seq, Guid? Token)
{
    public static Cursor Empty { get; } = new(0, null);
}

internal static class CursorValidity
{
    public static bool IsValid(Cursor cursor, Guid? tokenOfRowAtSeq, long horizonSeq) =>
        (tokenOfRowAtSeq is { } token && cursor.Token == token) || (cursor.Seq == 0 && horizonSeq == 0);

    public static bool HasApplied(Cursor cursor, Guid? tokenOfRowAtSeq, long horizonSeq, long seq) =>
        IsValid(cursor, tokenOfRowAtSeq, horizonSeq) && cursor.Seq >= seq;
}

internal static class ApplierPoll
{
    public static bool PassNeeded(Cursor local, Cursor head, Guid? tokenOfRowAtCursor, long horizonSeq) =>
        head != local || !CursorValidity.IsValid(local, tokenOfRowAtCursor, horizonSeq);
}
