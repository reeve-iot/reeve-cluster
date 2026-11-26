using System.Globalization;

namespace Reeve.Cluster.Core;

internal static class PhysicalConnectionCheck
{
    public const string ProbeSql = "SELECT pg_is_in_recovery(), current_setting('transaction_timeout', true), current_setting('statement_timeout')";

    public static ClusterConfigurationException? Evaluate(bool inRecovery, string? transactionTimeout, string statementTimeout)
    {
        if (inRecovery)
        {
            return new ClusterConfigurationException(
                ClusterConfigurationReason.NotPrimary,
                "The connection reached a standby (pg_is_in_recovery() is true); the library reads from the primary only. Point the connection string at the primary (with several hosts, also set Target Session Attributes=primary).");
        }

        if (transactionTimeout is null)
        {
            return new ClusterConfigurationException(
                ClusterConfigurationReason.PostgreSqlTooOld,
                "The server has no transaction_timeout setting; the library needs PostgreSQL 18 or later, the only version it is tested on.");
        }

        if (!IsZero(transactionTimeout))
        {
            return new ClusterConfigurationException(
                ClusterConfigurationReason.TransactionTimeoutNotZero,
                $"transaction_timeout is {transactionTimeout} on a connection opened by the library or checked before a fenced transaction, and it must be 0 for the role, the database and the session: a non-zero session value is armed at BEGIN and a SET LOCAL inside the transaction does not re-arm it, so it, not the library's bound, would end fenced transactions. Run ALTER ROLE … RESET transaction_timeout and ALTER DATABASE … RESET transaction_timeout, and do not set it on this connection.");
        }

        if (!IsZero(statementTimeout))
        {
            return new ClusterConfigurationException(
                ClusterConfigurationReason.StatementTimeoutNotZero,
                $"statement_timeout is {statementTimeout} on a connection opened by the library or checked before a fenced transaction, and it must be 0: it cancels COMMIT's wait for the synchronous standby while PostgreSQL keeps the commit locally, unreplicated, and reports success. Run ALTER ROLE … RESET statement_timeout and ALTER DATABASE … RESET statement_timeout, and do not set it on this connection.");
        }

        return null;
    }

    internal static bool IsZero(string setting)
    {
        var digits = setting.Trim().TrimEnd('m', 's', 'i', 'n', 'h', 'd', ' ');
        return long.TryParse(digits, NumberStyles.None, CultureInfo.InvariantCulture, out var value) && value == 0;
    }
}
