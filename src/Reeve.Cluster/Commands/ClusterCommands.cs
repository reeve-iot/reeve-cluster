using System.Data.Common;
using System.Net.Sockets;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Reeve.Cluster.Claims;
using Reeve.Cluster.Core;
using Reeve.Cluster.Wakeup;

namespace Reeve.Cluster.Commands;

internal sealed class ClusterCommands(
    ICommandRows rows, IEnumerable<CommandHandlerRegistration> registrations, IWakeupChannel wakeups, IOptions<ClusterOptions> options, TimeProvider time, ILogger<ClusterCommands> logger) : IClusterCommands
{
    private readonly HashSet<string> kinds = [.. registrations.Select(r => r.Kind)];

    private const int Attempts = 5;

    public async Task<DurableSendResult> SendDurableAsync(string kind, string key, ReadOnlyMemory<byte> command, string idempotencyKey, CancellationToken cancellationToken = default)
    {
        ClusterCapabilities.Validate(kind);
        if (!kinds.Contains(kind))
        {
            throw new InvalidOperationException($"No durable command handler is registered for the kind '{kind}'; register one with AddClusterCommandHandler.");
        }

        ArgumentNullException.ThrowIfNull(key);
        ArgumentException.ThrowIfNullOrEmpty(idempotencyKey);

        CommandText.ThrowIfUnstorable(key, nameof(key));
        CommandText.ThrowIfUnstorable(idempotencyKey, nameof(idempotencyKey));
        var payload = command.ToArray();

        for (var attempt = 0; attempt < Attempts; attempt++)
        {
            if (await BoundedAsync(bound => rows.InsertAsync(kind, key, idempotencyKey, payload, bound), cancellationToken).ConfigureAwait(false) is { } inserted)
            {
                await NudgeAsync(kind, cancellationToken).ConfigureAwait(false);
                return new DurableSendResult(DurableSendOutcome.Sent, inserted.Id, DurableCommandState.Pending);
            }

            var read = await BoundedAsync(bound => rows.ReadAsync(kind, key, idempotencyKey, bound), cancellationToken).ConfigureAwait(false);
            switch (SendDecisions.AfterConflict(read, payload))
            {
                case SendAction.Conflict:
                    return new DurableSendResult(DurableSendOutcome.IdempotencyConflict, read!.Id, Public(read.State));
                case SendAction.Existing:
                    if (read!.State == CommandState.Pending)
                    {
                        await NudgeAsync(kind, cancellationToken).ConfigureAwait(false);
                    }

                    return new DurableSendResult(DurableSendOutcome.Existing, read.Id, Public(read.State));
                case SendAction.Resend:
                    var resent = await BoundedAsync(bound => rows.ResendAsync(read!.Id, bound), cancellationToken).ConfigureAwait(false);
                    if (SendDecisions.AfterResend(resent) == SendAction.Resent)
                    {
                        await NudgeAsync(kind, cancellationToken).ConfigureAwait(false);
                        return new DurableSendResult(DurableSendOutcome.Resent, read!.Id, DurableCommandState.Pending);
                    }

                    break;
            }
        }

        throw new InvalidOperationException($"The durable command {kind}/{key}/{idempotencyKey} kept changing under the send; send it again.");
    }

    public async Task<DurableCommandResult> WaitAsync(string kind, string key, string idempotencyKey, TimeSpan timeout, CancellationToken cancellationToken = default)
    {
        ClusterCapabilities.Validate(kind);
        ArgumentNullException.ThrowIfNull(key);
        ArgumentException.ThrowIfNullOrEmpty(idempotencyKey);
        CommandText.ThrowIfUnstorable(key, nameof(key));
        CommandText.ThrowIfUnstorable(idempotencyKey, nameof(idempotencyKey));
        if (timeout <= TimeSpan.Zero || timeout == Timeout.InfiniteTimeSpan || timeout > OptionsValidator.MaxTimerDuration)
        {
            throw new ArgumentOutOfRangeException(nameof(timeout), timeout, "The wait must be finite and positive.");
        }

        var woken = NewSignal();
        var target = CommandNudges.Completed(kind, key);
        using var subscription = wakeups.Subscribe(WakeupKind.CommandNudge, w =>
        {
            if (w.Target == target)
            {
                Volatile.Read(ref woken).TrySetResult();
            }
        });

        using var deadline = new CancellationTokenSource(timeout, time);
        using var bound = CancellationTokenSource.CreateLinkedTokenSource(deadline.Token, cancellationToken);
        DurableCommandState? last = null;
        long lastId = 0;
        try
        {
            while (true)
            {
                var signal = NewSignal();
                Volatile.Write(ref woken, signal);
                (CommandStatus? Value, bool Cancelled) read;
                try
                {
                    read = await StatementBound.RunAsync(
                        statement => rows.ReadStatusAsync(kind, key, idempotencyKey, statement), options, time, bound.Token).ConfigureAwait(false);
                }
                catch (Exception exception) when (exception is DbException or IOException or SocketException or TimeoutException && !CommandText.IsPermanent(exception))
                {
                    logger.LogDebug(exception, "The wait for {Kind}/{Key} could not read the command; it reads again.", kind, key);
                    read = (default, true);
                }

                if (!read.Cancelled)
                {
                    if (WaitDecisions.Finished(read.Value) is { } finished)
                    {
                        return finished;
                    }

                    last = Public(read.Value!.Value.State);
                    lastId = read.Value.Value.Id;
                }

                try
                {
                    await signal.Task.WaitAsync(options.Value.PlacementInterval, time, bound.Token).ConfigureAwait(false);
                }
                catch (TimeoutException)
                {
                }
            }
        }
        catch (OperationCanceledException) when (bound.IsCancellationRequested)
        {
            return new DurableCommandResult(DurableCommandOutcome.Unfinished, lastId, last, default, null);
        }
    }

    private static TaskCompletionSource NewSignal() => new(TaskCreationOptions.RunContinuationsAsynchronously);

    private async Task<T> BoundedAsync<T>(Func<CancellationToken, Task<T>> statement, CancellationToken cancellationToken)
    {
        var (value, cancelled) = await StatementBound.RunAsync(statement, options, time, cancellationToken).ConfigureAwait(false);
        return cancelled
            ? throw new OperationCanceledException("The durable command's send got no answer within the library's bound; its outcome is unknown. Send it again with the same idempotency key.")
            : value!;
    }

    internal static DurableCommandState Public(CommandState state) => state switch
    {
        CommandState.Pending => DurableCommandState.Pending,
        CommandState.Taken => DurableCommandState.Taken,
        CommandState.Done => DurableCommandState.Done,
        CommandState.InDoubt => DurableCommandState.InDoubt,
        _ => DurableCommandState.Expired,
    };

    private async ValueTask NudgeAsync(string kind, CancellationToken cancellationToken)
    {
        try
        {
            await wakeups.PublishAsync(Wakeup.Wakeup.Nudge(WakeupKind.CommandNudge, kind), cancellationToken).ConfigureAwait(false);
        }
        catch (Exception exception) when (exception is not OutOfMemoryException && !cancellationToken.IsCancellationRequested)
        {
            logger.LogDebug(exception, "The command nudge for {Kind} was not published; the owner's poll takes the command.", kind);
        }
    }
}
