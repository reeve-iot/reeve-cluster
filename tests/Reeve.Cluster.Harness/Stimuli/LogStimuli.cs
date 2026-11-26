using static Reeve.Cluster.Harness.Stimuli.HarnessOperation;
using static Reeve.Cluster.Harness.Stimuli.StimulusRow;
using static Reeve.Cluster.Harness.Stimuli.StimulusSubject;

namespace Reeve.Cluster.Harness.Stimuli;

internal static class LogStimuli
{
    public static readonly StimulusRow WBegin = Do(
        LogActions.WBegin, Write, "Begin a client write on the replica: a transaction in a request's scope that is about to append.",
        StartWrite).Proven("A_write_that_commits_records_WBegin_WAppend_and_WCommit_under_one_xid");

    public static readonly StimulusRow WAppend = At(
        LogActions.WAppend, Write, "Hold the write before and after Append, and before and after the interceptor's append at commit.",
        Of(null, LogFaultPoints.AppendBefore, LogFaultPoints.AppendAfter, CaptureFaultPoints.CommittingBeforeAppend, CaptureFaultPoints.CommittingAfterAppend),
        enabling: "WBegin.");

    public static readonly StimulusRow WSaveRefuse = Do(
        LogActions.WSaveRefuse, Write, "Make a capturing save in a transaction the library did not begin (UseTransaction); it throws and writes nothing.",
        SaveInForeignTransaction, enabling: "WBegin, on a transaction handed to EF with UseTransaction.").Proven("A_capturing_save_in_a_transaction_handed_to_EF_records_WSaveRefuse_and_writes_nothing");

    public static readonly StimulusRow WCommit = At(
        LogActions.WCommit, Write, "Hold the committed write between its commit and the wake-up it hands off.",
        Of(null, LogFaultPoints.AfterCommitBeforeWakeup), enabling: "WAppend.");

    public static readonly StimulusRow WRollback = Do(
        LogActions.WRollback, Write, "Roll the write's transaction back after its append: its seqs become gaps.",
        RollbackWrite, enabling: "WAppend.").Proven("A_write_rolled_back_after_its_append_records_WRollback_and_keeps_its_seq_in_the_tracker");

    public static readonly StimulusRow LoadDone = At(
        LogActions.LoadDone, Replica, "Hold the replica inside its first full load.",
        Of("1.10", LogFaultPoints.InsideFirstLoad), enabling: "Join.");

    public static readonly StimulusRow PassBegin = At(
        LogActions.PassBegin, Replica, "Hold the applier at the start of a pass and after it read its snapshot; before the retry that ends a backoff; before a full reload that is not the first load.",
        [.. Of("1.10", LogFaultPoints.PassBegin, LogFaultPoints.AfterSnapshotRead), .. Of(null, LogFaultPoints.BeforeBackoffRetry, LogFaultPoints.BeforeFullReload)],
        enabling: "A dirty type: a committed write the applier has not applied; for the retry, a failed or stalled pass; for the full reload, an invalid cursor or a rejoin.");

    public static readonly StimulusRow AttemptOK = At(
        LogActions.AttemptOK, Replica, "Hold the reconciler attempt before it returns its state, and between its return and the applier's acceptance of it.",
        [.. Of("1.10", LogFaultPoints.BeforeReconcilerReturns), .. Of(null, LogFaultPoints.BeforeAttemptAccepted)],
        enabling: "PassBegin.");

    public static readonly StimulusRow AttemptFail = Do(
        LogActions.AttemptFail, Replica, "Script the next attempt of the aggregate type to throw (HarnessOperations.ReconcilerThrow).",
        ReconcilerThrow, enabling: "PassBegin.", placedBy: "1.10");

    public static readonly StimulusRow AttemptHang = At(
        LogActions.AttemptHang, Replica, "Hang the attempt past ReconcileTimeout (HarnessOperations.ReconcilerHang; the mode decides whether a LateReturn can follow), holding it where it would return.",
        Of("1.10", LogFaultPoints.BeforeReconcilerReturns), ReconcilerHang, enabling: "PassBegin.");

    public static readonly StimulusRow LateReturn = At(
        LogActions.LateReturn, Replica, "Release the hung attempt (HarnessOperations.ReconcilerRelease) after its timeout, and hold it before the applier would accept its result.",
        Of(null, LogFaultPoints.BeforeAttemptAccepted), ReconcilerRelease, enabling: "AttemptHang with a cooperative or released attempt.");

    public static readonly StimulusRow Publish = At(
        LogActions.Publish, Replica, "Hold the applier between the swap of the generation and the cursor write.",
        Of("1.10", LogFaultPoints.BetweenPublishAndCursor), enabling: "Every attempt of the pass has returned (AttemptOK).");

    public static readonly StimulusRow TryActivate = At(
        LogActions.TryActivate, Replica, "Hold deferred work as it tries to become observable against the current generation. 1.7 emits the event and placed no point for it; the first replay that must hold the step adds the catalogue entry here.",
        [], enabling: "Publish.", placedBy: "1.19");

    public static readonly StimulusRow CursorWrite = At(
        LogActions.CursorWrite, Replica, "Hold the applier after the cursor write.",
        Of("1.10", LogFaultPoints.AfterCursorWrite), enabling: "Publish.");

    public static readonly StimulusRow QStart = Do(
        LogActions.QStart, Read, "Start a client read on the replica: a request that will wait for the read index.",
        StartRead, placedBy: "1.12");

    public static readonly StimulusRow QPin = At(
        LogActions.QPin, Read, "Hold the request between the end of its wait for the head and the pin of the generation.",
        Of("1.12", LogFaultPoints.BetweenWaitAndPin), enabling: "QStart, and the cursor standing for the head read.");

    public static readonly StimulusRow QRead = At(
        LogActions.QRead, Read, "Hold the request between the pin and the read.",
        Of("1.12", LogFaultPoints.BetweenPinAndRead), enabling: "QPin.");

    public static readonly StimulusRow QTimeout = Over(
        LogActions.QTimeout, Read, TimeAdvance.ReplicaClock, "Move the serving replica's clock past ReadIndexTimeout while its applier is held.",
        enabling: "QStart, with the applier held short of the head (PassBegin or AttemptOK held).", placedBy: "1.12");

    public static readonly StimulusRow AckBegin = At(
        LogActions.AckBegin, Write, "Hold the writer before the wait set read, and before the HTTP filter's await.",
        [.. Of("1.14", LogFaultPoints.AckBeforeWaitSetRead), .. Of("1.16", HttpFaultPoints.BeforeAckAwait)],
        enabling: "WCommit.");

    public static readonly StimulusRow AckDone = At(
        LogActions.AckDone, Write, "Hold the wait, which has seen every member of its wait set at the awaited seq, before its record, so the acknowledgement does not complete until released; the wait's poll then completes it.",
        [.. Of("1.14", LogFaultPoints.AckBetweenCountAndRecord), .. Of(null, LogFaultPoints.RecordBeforeUpsert, LogFaultPoints.RecordBeforeCommit)],
        enabling: "AckBegin, and every member of the wait set applied the awaited seq (CursorWrite).");

    public static readonly StimulusRow AckSkipRead = At(
        LogActions.AckSkipRead, Write, "Hold the record between its upsert and its skip read.",
        Of(null, LogFaultPoints.RecordBetweenUpsertAndSkipRead), enabling: "AckBegin.");

    public static readonly StimulusRow AckRecord = At(
        LogActions.AckRecord, Write, "Hold the record before its upsert and before its commit, hold its transaction open, and hold the wait between its count and the record.",
        [.. Of("1.14", LogFaultPoints.AckBetweenCountAndRecord), .. Of(null, LogFaultPoints.RecordBeforeUpsert, LogFaultPoints.RecordBeforeCommit, LogFaultPoints.RecordHold)],
        enabling: "AckBegin.");

    public static readonly StimulusRow AckTimeout = Over(
        LogActions.AckTimeout, Write, TimeAdvance.ReplicaClock, "Move the writer's clock past AckTimeout.",
        enabling: "AckBegin, with a member of the wait set that has not applied the awaited seq (a held or hung applier on it).", placedBy: "1.14");

    public static readonly StimulusRow AckAtCommit = Do(
        LogActions.AckAtCommit, Write, "Call the application's AcknowledgeAtCommit() on the open write: the opt-out.",
        AcknowledgeAtCommit, enabling: "WBegin.", placedBy: "1.14");

    public static readonly StimulusRow SkipOpen = Next(
        "SkipOpen", Fleet, "Not driven: the library's open is OpenLock, then OpenCommit or OpenRefuse.");

    public static readonly StimulusRow OpenLock = At(
        LogActions.OpenLock, Fleet, "Hold the open before its no-op upsert, and after it has locked the change_ack row.",
        Of(null, LogFaultPoints.OpenBeforeUpsert, LogFaultPoints.OpenAfterUpsert));

    public static readonly StimulusRow OpenCommit = At(
        LogActions.OpenCommit, Fleet, "Hold the open after its insert, before it commits.",
        Of(null, LogFaultPoints.OpenAfterInsert), enabling: "OpenLock, with from_seq above acked_seq.");

    public static readonly StimulusRow OpenRefuse = Next(
        LogActions.OpenRefuse, Fleet, "Nothing is injected: the open is refused (409).",
        enabling: "OpenLock, with from_seq at or below acked_seq (an AckDone's record raised it).").Proven("Replay_M1_D7_an_open_after_the_record_of_an_acknowledged_write_is_refused_whatever_its_type");

    public static readonly StimulusRow SkipClose = At(
        LogActions.SkipClose, Fleet, "Hold the close before and after its repair append.",
        Of(null, LogFaultPoints.CloseBeforeRepairAppend, LogFaultPoints.CloseAfterRepairAppend), enabling: "OpenCommit.");

    public static IReadOnlyList<StimulusRow> All { get; } =
    [
        WBegin, WAppend, WSaveRefuse, WCommit, WRollback, LoadDone, PassBegin, AttemptOK, AttemptFail, AttemptHang, LateReturn,
        Publish, TryActivate, CursorWrite, QStart, QPin, QRead, QTimeout, AckBegin, AckDone, AckSkipRead, AckRecord, AckTimeout,
        AckAtCommit, SkipOpen, OpenLock, OpenCommit, OpenRefuse, SkipClose,
    ];
}
