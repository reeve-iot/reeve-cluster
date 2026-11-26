using System.Globalization;

namespace Reeve.Cluster.Migrations;

internal static class PostgreSqlVersion
{
    public const int FloorNum = 180000;

    public static ClusterConfigurationException? Refusal(int serverVersionNum)
    {
        if (serverVersionNum >= FloorNum)
        {
            return null;
        }

        var found = string.Create(CultureInfo.InvariantCulture, $"{serverVersionNum / 10000}.{serverVersionNum % 10000}");
        return new ClusterConfigurationException(
            ClusterConfigurationReason.PostgreSqlTooOld,
            $"The server is PostgreSQL {found}; the library needs PostgreSQL 18 or later, the only version it is tested on. Nothing was written.");
    }
}
