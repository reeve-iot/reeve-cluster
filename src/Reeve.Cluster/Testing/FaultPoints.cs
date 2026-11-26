namespace Reeve.Cluster;

internal static class MembershipFaultPoints
{
    public static readonly FaultPoint JoinBefore = FaultPoint.At("membership.join.before", MembershipActions.Join);
    public static readonly FaultPoint JoinAfter = FaultPoint.At("membership.join.after", MembershipActions.Join);
    public static readonly FaultPoint RenewSend = FaultPoint.At("membership.renew.send", MembershipActions.RenewSend);
    public static readonly FaultPoint RenewExec = FaultPoint.At("membership.renew.exec", MembershipActions.RenewExec);
    public static readonly FaultPoint RenewAnswer = FaultPoint.At("membership.renew.answer", MembershipActions.RenewAnswer);

    public static readonly FaultPoint FenceBeforeStep0 = FaultPoint.Unmodelled("membership.fence.before-step-0");
    public static readonly FaultPoint FenceBeforeStep1 = FaultPoint.At("membership.fence.before-step-1", MembershipActions.FStep);
    public static readonly FaultPoint FenceBeforeStep2 = FaultPoint.At("membership.fence.before-step-2", MembershipActions.FStep);
    public static readonly FaultPoint FenceBeforeStep3 = FaultPoint.At("membership.fence.before-step-3", MembershipActions.FStep);
    public static readonly FaultPoint FenceBeforeStep4 = FaultPoint.At("membership.fence.before-step-4", MembershipActions.FStep);
    public static readonly FaultPoint FenceBeforeStep5 = FaultPoint.At("membership.fence.before-step-5", MembershipActions.FStep);
    public static readonly FaultPoint FenceBeforeStep6 = FaultPoint.At("membership.fence.before-step-6", MembershipActions.FStep);
    public static readonly FaultPoint FenceBeforeLeaseRelease = FaultPoint.At("membership.fence.before-lease-release", MembershipActions.FStep);
    public static readonly FaultPoint FenceAfterLeaseRelease = FaultPoint.At("membership.fence.after-lease-release", MembershipActions.FStep);
    public static readonly FaultPoint FenceBeforeHistoryRecord = FaultPoint.At("membership.fence.before-history-record", MembershipActions.FStep);
    public static readonly FaultPoint FenceAfterHistoryRecord = FaultPoint.At("membership.fence.after-history-record", MembershipActions.FStep);

    public static readonly FaultPoint ActivateBeforeLock = FaultPoint.At("membership.activate.before-lock", MembershipActions.Activate);
    public static readonly FaultPoint ActivateAfterHeadRead = FaultPoint.At("membership.activate.after-head-read", MembershipActions.Activate);
    public static readonly FaultPoint ActivateBeforeUpdate = FaultPoint.At("membership.activate.before-update", MembershipActions.Activate);
    public static readonly FaultPoint ActivateAfterUpdate = FaultPoint.At("membership.activate.after-update", MembershipActions.Activate);
    public static readonly FaultPoint ActivateBeforeCommit = FaultPoint.At("membership.activate.before-commit", MembershipActions.Activate);
    public static readonly FaultPoint ActivateCommittedBeforeApplied = FaultPoint.At("membership.activate.committed-before-applied", MembershipActions.EnterServing);

    public static readonly FaultPoint FloorRaiseBeforeLock = FaultPoint.Unmodelled("membership.floor-raise.before-lock");
    public static readonly FaultPoint FloorRaiseAfterMembersRead = FaultPoint.Unmodelled("membership.floor-raise.after-members-read");
    public static readonly FaultPoint FloorRaiseBeforeCommit = FaultPoint.Unmodelled("membership.floor-raise.before-commit");
    public static readonly FaultPoint FloorLowerBeforeWrite = FaultPoint.Unmodelled("membership.floor-lower.before-write");

    public static readonly FaultPoint DrainAfterStep1 = FaultPoint.At("membership.drain.after-step-1", MembershipActions.Drain1);
    public static readonly FaultPoint DrainBeforeStep4 = FaultPoint.At("membership.drain.before-step-4", MembershipActions.Drain23);
    public static readonly FaultPoint DrainBeforeDrainingWrite = FaultPoint.At("membership.drain.before-draining-write", MembershipActions.Drain4);
}

internal static class LogFaultPoints
{
    public static readonly FaultPoint AppendBefore = FaultPoint.At("log.append.before", LogActions.WAppend);
    public static readonly FaultPoint AppendAfter = FaultPoint.At("log.append.after", LogActions.WAppend);
    public static readonly FaultPoint AfterCommitBeforeWakeup = FaultPoint.At("log.append.after-commit-before-wakeup", LogActions.WCommit);

    public static readonly FaultPoint PassBegin = FaultPoint.At("log.applier.pass-begin", LogActions.PassBegin);
    public static readonly FaultPoint AfterSnapshotRead = FaultPoint.At("log.applier.after-snapshot-read", LogActions.PassBegin);
    public static readonly FaultPoint InsideFirstLoad = FaultPoint.At("log.applier.inside-first-load", LogActions.LoadDone);
    public static readonly FaultPoint BeforeReconcilerReturns = FaultPoint.At("log.applier.before-reconciler-returns", LogActions.AttemptOK);
    public static readonly FaultPoint BeforeAttemptAccepted = FaultPoint.At("log.applier.before-attempt-accepted", LogActions.AttemptOK);
    public static readonly FaultPoint BetweenPublishAndCursor = FaultPoint.At("log.applier.between-publish-and-cursor", LogActions.Publish);
    public static readonly FaultPoint AfterCursorWrite = FaultPoint.At("log.applier.after-cursor-write", LogActions.CursorWrite);

    public static readonly FaultPoint BeforeBackoffRetry = FaultPoint.At("log.applier.before-backoff-retry", LogActions.PassBegin);
    public static readonly FaultPoint StallLimitDecision = FaultPoint.At("log.applier.stall-limit-decision", MembershipActions.FenceStall);
    public static readonly FaultPoint StuckAttemptDecision = FaultPoint.At("log.applier.stuck-attempt-decision", MembershipActions.FenceStuck);
    public static readonly FaultPoint BeforeFullReload = FaultPoint.At("log.applier.before-full-reload", LogActions.PassBegin);

    public static readonly FaultPoint BetweenWaitAndPin = FaultPoint.At("log.read-index.between-wait-and-pin", LogActions.QPin);
    public static readonly FaultPoint BetweenPinAndRead = FaultPoint.At("log.read-index.between-pin-and-read", LogActions.QRead);

    public static readonly FaultPoint AckBeforeWaitSetRead = FaultPoint.At("log.ack.before-wait-set-read", LogActions.AckBegin);
    public static readonly FaultPoint AckBetweenCountAndRecord = FaultPoint.At("log.ack.between-count-and-record", LogActions.AckRecord);

    public static readonly FaultPoint RecordBeforeUpsert = FaultPoint.At("log.record.before-upsert", LogActions.AckRecord);
    public static readonly FaultPoint RecordBetweenUpsertAndSkipRead = FaultPoint.At("log.record.between-upsert-and-skip-read", LogActions.AckSkipRead);
    public static readonly FaultPoint RecordBeforeCommit = FaultPoint.At("log.record.before-commit", LogActions.AckRecord);
    public static readonly FaultPoint RecordHold = FaultPoint.At("log.record.hold", LogActions.AckRecord);

    public static readonly FaultPoint OpenBeforeUpsert = FaultPoint.At("log.skip.open.before-upsert", LogActions.OpenLock);
    public static readonly FaultPoint OpenAfterUpsert = FaultPoint.At("log.skip.open.after-upsert", LogActions.OpenLock);
    public static readonly FaultPoint OpenAfterInsert = FaultPoint.At("log.skip.open.after-insert", LogActions.OpenCommit);
    public static readonly FaultPoint CloseBeforeRepairAppend = FaultPoint.At("log.skip.close.before-repair-append", LogActions.SkipClose);
    public static readonly FaultPoint CloseAfterRepairAppend = FaultPoint.At("log.skip.close.after-repair-append", LogActions.SkipClose);

    public static readonly FaultPoint RetentionBetweenMinimumAndDelete = FaultPoint.At("log.retention.between-minimum-and-delete", LogActions.Retention);
    public static readonly FaultPoint RetentionBeforeHorizonUpdate = FaultPoint.At("log.retention.before-horizon-update", LogActions.Retention);
}

internal static class CaptureFaultPoints
{
    public static readonly FaultPoint CommittingBeforeAppend = FaultPoint.At("capture.committing.before-append", LogActions.WAppend);
    public static readonly FaultPoint CommittingAfterAppend = FaultPoint.At("capture.committing.after-append", LogActions.WAppend);

    public static readonly FaultPoint TransactionStarted = FaultPoint.Unmodelled("capture.transaction-started.before-discard");
    public static readonly FaultPoint TransactionUsed = FaultPoint.Unmodelled("capture.transaction-used.before-discard");
}

internal static class HttpFaultPoints
{
    public static readonly FaultPoint BetweenServingCheckAndHandler = FaultPoint.Unmodelled("http.between-serving-check-and-handler");
    public static readonly FaultPoint BeforeAckAwait = FaultPoint.At("http.before-ack-await", LogActions.AckBegin);
    public static readonly FaultPoint BeforeResponseStarts = FaultPoint.Unmodelled("http.before-response-starts");
}

internal static class OwnedFaultPoints
{
    public static readonly FaultPoint EnsureOwnerBeforeEffect = FaultPoint.Unmodelled("owned.ensure-owner.before-effect");

    public static readonly FaultPoint FencedAfterAssert = FaultPoint.Unmodelled("owned.fenced.after-assert");

    public static readonly FaultPoint PollBeforeRead = FaultPoint.Unmodelled("owned.poll.before-read");
    public static readonly FaultPoint PlacerPassBeforeRead = FaultPoint.Unmodelled("owned.placer.pass.before-read");
    public static readonly FaultPoint PlacerAdoptBeforeRead = FaultPoint.Unmodelled("owned.placer.adopt.before-read");
    public static readonly FaultPoint PlacerBeforeAssign = FaultPoint.Unmodelled("owned.placer.before-assign");

    public static readonly FaultPoint RedisBeforeExec = FaultPoint.Unmodelled("owned.redis.before-exec");
}

internal static class ClaimsFaultPoints
{
    public static readonly FaultPoint GrantBeforeInsert = FaultPoint.Unmodelled("claims.grant.before-insert");
    public static readonly FaultPoint GrantBeforeRead = FaultPoint.Unmodelled("claims.grant.before-read");
    public static readonly FaultPoint GrantBeforeTakeover = FaultPoint.Unmodelled("claims.grant.before-takeover");
    public static readonly FaultPoint ReleaseBeforeStatement = FaultPoint.Unmodelled("claims.release.before-statement");

    public static readonly FaultPoint GrantAfterCommit = FaultPoint.Unmodelled("claims.grant.after-commit");

    public static readonly FaultPoint FencedAfterAssert = FaultPoint.Unmodelled("claims.fenced.after-assert");

    public static readonly FaultPoint OnceBeforeInsert = FaultPoint.Unmodelled("claims.once.before-insert");
    public static readonly FaultPoint OnceBeforeRead = FaultPoint.Unmodelled("claims.once.before-read");
    public static readonly FaultPoint OnceAfterGrant = FaultPoint.Unmodelled("claims.once.after-grant");
    public static readonly FaultPoint OnceBeforeComplete = FaultPoint.Unmodelled("claims.once.before-complete");

    public static readonly FaultPoint RetakeBeforeStatement = FaultPoint.Unmodelled("claims.retake.before-statement");
    public static readonly FaultPoint RetakeAfterCommit = FaultPoint.Unmodelled("claims.retake.after-commit");

    public static readonly FaultPoint FlightBeforeInsert = FaultPoint.Unmodelled("claims.flight.before-insert");
    public static readonly FaultPoint FlightBeforeRead = FaultPoint.Unmodelled("claims.flight.before-read");
    public static readonly FaultPoint FlightBeforeTakeover = FaultPoint.Unmodelled("claims.flight.before-takeover");
    public static readonly FaultPoint FlightAfterGrant = FaultPoint.Unmodelled("claims.flight.after-grant");
    public static readonly FaultPoint FlightBeforeResult = FaultPoint.Unmodelled("claims.flight.before-result");

    public static readonly FaultPoint BudgetBeforeConsume = FaultPoint.Unmodelled("claims.budget.before-consume");
}

internal static class SlotsFaultPoints
{
    public static readonly FaultPoint AcquireBeforeStatement = FaultPoint.Unmodelled("slots.acquire.before-statement");
    public static readonly FaultPoint RenewBeforeStatement = FaultPoint.Unmodelled("slots.renew.before-statement");
    public static readonly FaultPoint ReleaseBeforeStatement = FaultPoint.Unmodelled("slots.release.before-statement");

    public static readonly FaultPoint AcquireAfterDelete = FaultPoint.Unmodelled("slots.acquire.after-delete");

    public static readonly FaultPoint AcquireAfterCommit = FaultPoint.Unmodelled("slots.acquire.after-commit");

    public static readonly FaultPoint FencedAfterAssert = FaultPoint.Unmodelled("slots.fenced.after-assert");
}

internal static class CommandsFaultPoints
{
    public static readonly FaultPoint AfterTake = FaultPoint.Unmodelled("commands.after-take");

    public static readonly FaultPoint BeforeComplete = FaultPoint.Unmodelled("commands.before-complete");
}
