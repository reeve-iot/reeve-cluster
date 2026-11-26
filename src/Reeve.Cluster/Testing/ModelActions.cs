namespace Reeve.Cluster;

internal static class MembershipActions
{
    public const string Join = nameof(Join);
    public const string RenewSend = nameof(RenewSend);
    public const string RenewExec = nameof(RenewExec);
    public const string RenewAnswer = nameof(RenewAnswer);
    public const string RenewZeroL = nameof(RenewZeroL);
    public const string ReachFS = nameof(ReachFS);
    public const string FStep = nameof(FStep);
    public const string FenceAtStart = nameof(FenceAtStart);
    public const string FenceStall = nameof(FenceStall);
    public const string FenceAny = nameof(FenceAny);
    public const string FenceStuck = nameof(FenceStuck);
    public const string TimelineFence = nameof(TimelineFence);
    public const string FaultFenced = nameof(FaultFenced);
    public const string StuckExit = nameof(StuckExit);
    public const string FencedExit = nameof(FencedExit);
    public const string Rejoin = nameof(Rejoin);

    public const string Activate = nameof(Activate);
    public const string EnterServing = nameof(EnterServing);

    public const string Drain1 = nameof(Drain1);
    public const string Drain23 = nameof(Drain23);
    public const string Drain4 = nameof(Drain4);
}

internal static class LogActions
{
    public const string WBegin = nameof(WBegin);
    public const string WAppend = nameof(WAppend);
    public const string WSaveRefuse = nameof(WSaveRefuse);
    public const string WCommit = nameof(WCommit);
    public const string WRollback = nameof(WRollback);

    public const string Publish = nameof(Publish);
    public const string TryActivate = nameof(TryActivate);
    public const string LoadDone = nameof(LoadDone);
    public const string PassBegin = nameof(PassBegin);
    public const string AttemptOK = nameof(AttemptOK);
    public const string AttemptFail = nameof(AttemptFail);
    public const string AttemptHang = nameof(AttemptHang);
    public const string LateReturn = nameof(LateReturn);
    public const string CursorWrite = nameof(CursorWrite);

    public const string QStart = nameof(QStart);
    public const string QPin = nameof(QPin);
    public const string QRead = nameof(QRead);
    public const string QTimeout = nameof(QTimeout);

    public const string AckRecord = nameof(AckRecord);
    public const string AckSkipRead = nameof(AckSkipRead);
    public const string OpenLock = nameof(OpenLock);
    public const string OpenCommit = nameof(OpenCommit);
    public const string OpenRefuse = nameof(OpenRefuse);
    public const string SkipClose = nameof(SkipClose);

    public const string AckBegin = nameof(AckBegin);
    public const string AckDone = nameof(AckDone);
    public const string AckTimeout = nameof(AckTimeout);
    public const string AckAtCommit = nameof(AckAtCommit);

    public const string Retention = nameof(Retention);
}

internal static class OwnedActions
{
}
