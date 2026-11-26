using Reeve.Cluster;
using Reeve.Cluster.Core;

namespace Unit.Core;

public sealed class PhysicalConnectionCheckTests
{
    [Theory]
    [InlineData("0")]
    [InlineData("0ms")]
    [InlineData("0s")]
    public void A_zero_transaction_timeout_on_the_primary_passes(string setting)
    {
        Assert.Null(PhysicalConnectionCheck.Evaluate(inRecovery: false, setting, "0"));
    }

    [Theory]
    [InlineData("5s")]
    [InlineData("1min")]
    [InlineData("100ms")]
    [InlineData("1h")]
    [InlineData("1d")]
    public void A_non_zero_transaction_timeout_fails_with_its_named_reason(string setting)
    {
        var fault = PhysicalConnectionCheck.Evaluate(inRecovery: false, setting, "0");

        Assert.NotNull(fault);
        Assert.Equal(ClusterConfigurationReason.TransactionTimeoutNotZero, fault.Reason);
        Assert.Contains(setting, fault.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void A_server_without_the_setting_fails_as_too_old()
    {
        var fault = PhysicalConnectionCheck.Evaluate(inRecovery: false, transactionTimeout: null, statementTimeout: "0");

        Assert.Equal(ClusterConfigurationReason.PostgreSqlTooOld, fault?.Reason);
    }

    [Fact]
    public void I24_a_standby_fails_before_anything_else_is_read()
    {
        var fault = PhysicalConnectionCheck.Evaluate(inRecovery: true, "5s", "0");

        Assert.Equal(ClusterConfigurationReason.NotPrimary, fault?.Reason);
    }

    [Fact]
    public void A_non_zero_statement_timeout_fails_and_zero_passes()
    {
        Assert.Null(PhysicalConnectionCheck.Evaluate(false, "0", "0ms"));
        var fault = PhysicalConnectionCheck.Evaluate(false, "0", "5s");
        Assert.Equal(ClusterConfigurationReason.StatementTimeoutNotZero, fault?.Reason);
    }

    [Theory]
    [InlineData("")]
    [InlineData("abc")]
    [InlineData("-1")]
    public void A_setting_it_cannot_read_is_not_zero(string setting)
    {
        Assert.False(PhysicalConnectionCheck.IsZero(setting));
    }
}
