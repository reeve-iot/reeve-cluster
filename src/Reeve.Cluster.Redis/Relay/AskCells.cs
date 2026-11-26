using Microsoft.Extensions.Logging;

namespace Reeve.Cluster.Redis.Relay;

internal sealed class AskCells(IAskWire wire, TimeSpan margin, ILogger logger)
{
    private const int Attempts = 4;

    public async Task<SubmitEnd> SubmitAsync(Guid id, TimeSpan timeToLive, string channel, byte[] message, CancellationToken cancellationToken)
    {
        try
        {
            await wire.WriteAsync(AskAddress.Record(id), AskCellCodec.Encode(AskCell.Pending), timeToLive, cancellationToken).ConfigureAwait(false);
        }
        catch (Exception exception) when (exception is not OutOfMemoryException)
        {
            logger.LogDebug(exception, "Writing the record of an Ask request failed; nothing was published.");
            return SubmitEnd.NeverSent;
        }

        try
        {
            await wire.PublishAsync(channel, message, cancellationToken).ConfigureAwait(false);
            return SubmitEnd.Submitted;
        }
        catch (Exception exception) when (exception is not OutOfMemoryException)
        {
            logger.LogDebug(exception, "Publishing an Ask request failed.");
            return AskFault.ClassifySubmit(exception);
        }
    }

    public async Task<(AskCell? Cell, bool Read)> ReadAsync(Guid id, CancellationToken cancellationToken)
    {
        try
        {
            var record = await wire.ReadAsync(AskAddress.Record(id), cancellationToken).ConfigureAwait(false);
            return (record.Value is null ? null : AskCellCodec.Decode(record.Value), true);
        }
        catch (Exception exception) when (exception is not OutOfMemoryException)
        {
            logger.LogDebug(exception, "Reading an Ask record failed.");
            return (null, false);
        }
    }

    public async Task<Withdrawal?> WithdrawAsync(Guid id, CancellationToken cancellationToken)
    {
        var name = AskAddress.Record(id);
        try
        {
            for (var attempt = 0; attempt < Attempts; attempt++)
            {
                var record = await wire.ReadAsync(name, cancellationToken).ConfigureAwait(false);
                var cell = record.Value is null ? null : AskCellCodec.Decode(record.Value);
                var withdrawal = AskCellRules.Withdraw(cell);
                if (cell is not { Kind: AskCellKind.Pending })
                {
                    return withdrawal;
                }

                if (await wire.ReplaceAsync(name, record.Value!, AskCellCodec.Encode(AskCell.Withdrawn), record.TimeToLive ?? margin, cancellationToken).ConfigureAwait(false))
                {
                    return withdrawal;
                }
            }
        }
        catch (Exception exception) when (exception is not OutOfMemoryException)
        {
            logger.LogDebug(exception, "Withdrawing an Ask request failed.");
        }

        return null;
    }

    public async Task<TakeEnd> TakeAsync(Guid id, CancellationToken cancellationToken)
    {
        var name = AskAddress.Record(id);
        try
        {
            for (var attempt = 0; attempt < Attempts; attempt++)
            {
                var record = await wire.ReadAsync(name, cancellationToken).ConfigureAwait(false);
                var cell = record.Value is null ? null : AskCellCodec.Decode(record.Value);
                switch (AskCellRules.Take(cell, record.TimeToLive, margin))
                {
                    case TakeVerdict.NotPending:
                        return TakeEnd.NotPending;
                    case TakeVerdict.TooLate:
                        return TakeEnd.TooLate;
                }

                if (await wire.ReplaceAsync(name, record.Value!, AskCellCodec.Encode(AskCell.Taken), record.TimeToLive ?? margin, cancellationToken).ConfigureAwait(false))
                {
                    return TakeEnd.Written;
                }
            }
        }
        catch (Exception exception) when (exception is not OutOfMemoryException)
        {
            logger.LogDebug(exception, "Writing the receipt of an Ask request failed.");
        }

        return TakeEnd.Fault;
    }

    public async Task<bool> DeclineAsync(Guid id, Reeve.Cluster.Ask.DeclineReason reason, CancellationToken cancellationToken)
    {
        var name = AskAddress.Record(id);
        try
        {
            for (var attempt = 0; attempt < Attempts; attempt++)
            {
                var record = await wire.ReadAsync(name, cancellationToken).ConfigureAwait(false);
                var cell = record.Value is null ? null : AskCellCodec.Decode(record.Value);
                if (!AskCellRules.CanDecline(cell))
                {
                    return false;
                }

                if (await wire.ReplaceAsync(name, record.Value!, AskCellCodec.Encode(AskCell.Declined(reason)), record.TimeToLive ?? margin, cancellationToken).ConfigureAwait(false))
                {
                    return true;
                }
            }
        }
        catch (Exception exception) when (exception is not OutOfMemoryException)
        {
            logger.LogDebug(exception, "Writing the decline of an Ask request failed.");
        }

        return false;
    }

    public async Task<AnswerEnd> AnswerAsync(Guid id, Reeve.Cluster.Ask.AskAnswer answer, CancellationToken cancellationToken)
    {
        var name = AskAddress.Record(id);
        try
        {
            for (var attempt = 0; attempt < Attempts; attempt++)
            {
                var record = await wire.ReadAsync(name, cancellationToken).ConfigureAwait(false);
                var cell = record.Value is null ? null : AskCellCodec.Decode(record.Value);
                if (!AskCellRules.CanAnswer(cell))
                {
                    return AnswerEnd.Refused;
                }

                if (await wire.ReplaceAsync(name, record.Value!, AskCellCodec.Encode(AskCell.Answered(answer)), record.TimeToLive ?? margin, cancellationToken).ConfigureAwait(false))
                {
                    return AnswerEnd.Written;
                }
            }
        }
        catch (Exception exception) when (exception is not OutOfMemoryException)
        {
            logger.LogDebug(exception, "Writing the answer of an Ask request failed.");
        }

        return AnswerEnd.Fault;
    }

    public void Forget(Guid id)
    {
        try
        {
            wire.Delete(AskAddress.Record(id));
        }
        catch (Exception exception) when (exception is not OutOfMemoryException)
        {
            logger.LogDebug(exception, "Deleting an Ask record failed.");
        }
    }
}
