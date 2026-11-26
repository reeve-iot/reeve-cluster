using System.Text;
using Npgsql;
using Reeve.Cluster.Commands;
using Unit.Claims;

namespace Unit.Commands;

public sealed class CommandTextTests
{
    private static readonly UTF8Encoding Strict = new(false, true);

    private static void AssertStorable(string text)
    {
        Assert.DoesNotContain('\0', text);
        Assert.Equal(text, Strict.GetString(Strict.GetBytes(text)));
    }

    [Fact]
    public void A_message_the_table_can_hold_is_kept_as_it_is()
    {
        Assert.Equal("boom: bad value 'ü😀'", CommandText.Normalise("boom: bad value 'ü😀'", "fallback"));
    }

    [Theory]
    [InlineData(null)]
    [InlineData("")]
    [InlineData("  \t")]
    public void A_message_with_no_text_becomes_the_fallback(string? message)
    {
        Assert.Equal("fallback", CommandText.Normalise(message, "fallback"));
    }

    [Fact]
    public void U_0000_is_replaced()
    {
        Assert.Equal("a�b", CommandText.Normalise("a\0b", "fallback"));
    }

    [Fact]
    public void An_unpaired_surrogate_is_replaced_and_a_pair_is_kept()
    {
        (string Message, string Expected)[] cases =
        [
            ("a\uD800b", "a\uFFFDb"),
            ("a\uDC00b", "a\uFFFDb"),
            ("a\uD800", "a\uFFFD"),
            ("\uDC00\uD800", "\uFFFD\uFFFD"),
            ("\uD800\uD800\uDC00", "\uFFFD\uD800\uDC00"),
        ];
        foreach (var (message, expected) in cases)
        {
            var text = CommandText.Normalise(message, "fallback");

            Assert.Equal(expected, text);
            AssertStorable(text);
        }
    }

    [Fact]
    public void A_long_message_is_cut_to_the_cap_and_ends_in_an_ellipsis()
    {
        var text = CommandText.Normalise(new string('x', CommandText.MaxErrorLength + 500), "fallback");

        Assert.Equal(CommandText.MaxErrorLength, text.Length);
        Assert.EndsWith("…", text, StringComparison.Ordinal);
    }

    [Fact]
    public void A_message_of_exactly_the_cap_is_not_cut()
    {
        var message = new string('x', CommandText.MaxErrorLength);

        Assert.Equal(message, CommandText.Normalise(message, "fallback"));
    }

    [Theory]
    [InlineData(0)]
    [InlineData(1)]
    public void A_cut_never_ends_inside_a_surrogate_pair(int shift)
    {
        var message = new string('x', CommandText.MaxErrorLength - 2 + shift) + "😀😀";

        var text = CommandText.Normalise(message, "fallback");

        Assert.True(text.Length <= CommandText.MaxErrorLength);
        AssertStorable(text);
    }

    [Fact]
    public void Whatever_the_message_the_result_is_storable_non_empty_and_within_the_cap()
    {
        var random = new Random(4711);
        var alphabet = new[] { 'a', 'é', '\0', '\uD800', '\uDBFF', '\uDC00', '\uDFFF', ' ', '\n', '\uD83D', '\uDE00' };
        for (var round = 0; round < 500; round++)
        {
            var length = random.Next(0, round % 50 == 0 ? 9000 : 40);
            var message = new string([.. Enumerable.Range(0, length).Select(_ => alphabet[random.Next(alphabet.Length)])]);

            var text = CommandText.Normalise(message, "fallback");

            Assert.NotEmpty(text);
            Assert.True(text.Length <= CommandText.MaxErrorLength);
            AssertStorable(text);
        }
    }

    [Fact]
    public void The_error_of_an_exception_is_the_message_of_its_base_exception()
    {
        Assert.Equal("inner", CommandText.ErrorOf(new InvalidOperationException("outer", new TimeoutException("inner"))));
    }

    [Fact]
    public void The_error_of_an_exception_whose_message_holds_a_NUL_and_a_lone_surrogate_is_storable()
    {
        var text = CommandText.ErrorOf(new FormatException("bad value 'a\0b\uD800'"));

        Assert.Equal("bad value 'a�b�'", text);
    }

    [Fact]
    public void The_error_of_an_exception_with_a_null_message_names_its_type()
    {
        Assert.Equal("The handler threw NullMessage.", CommandText.ErrorOf(new NullMessage()));
    }

    [Fact]
    public void The_error_of_an_exception_whose_message_throws_names_its_type()
    {
        Assert.Equal("The handler threw ThrowingMessage.", CommandText.ErrorOf(new ThrowingMessage()));
    }

    [Fact]
    public void A_text_is_storable_when_it_has_no_NUL_and_no_unpaired_surrogate()
    {
        (string Value, bool Storable)[] cases =
        [
            ("a", true), ("", true), ("a😀", true), ("a\0", false), ("\uD800", false), ("\uDC00", false), ("\uD800x", false),
        ];
        foreach (var (value, storable) in cases)
        {
            Assert.Equal(storable, CommandText.IsStorable(value));
            if (storable)
            {
                CommandText.ThrowIfUnstorable(value, "key");
            }
            else
            {
                Assert.Throws<ArgumentException>("key", () => CommandText.ThrowIfUnstorable(value, "key"));
            }
        }
    }

    [Theory]
    [InlineData("22021", true)]
    [InlineData("22P02", true)]
    [InlineData("22008", true)]
    [InlineData("40001", false)]
    [InlineData("57P01", false)]
    [InlineData("08006", false)]
    [InlineData("23505", false)]
    public void A_data_exception_of_the_database_is_permanent_and_nothing_else_of_it_is(string sqlState, bool permanent)
    {
        Assert.Equal(permanent, CommandText.IsPermanent(new PostgresException("m", "ERROR", "ERROR", sqlState)));
    }

    [Fact]
    public void The_drivers_refusal_to_encode_is_permanent_also_when_it_is_wrapped()
    {
        var refusal = new EncoderFallbackException("lone surrogate");

        Assert.True(CommandText.IsPermanent(refusal));
        Assert.True(CommandText.IsPermanent(new NpgsqlException("wrapped", refusal)));
    }

    [Fact]
    public void A_lost_connection_or_a_timeout_is_not_permanent()
    {
        Assert.False(CommandText.IsPermanent(new DbFailure("no answer")));
        Assert.False(CommandText.IsPermanent(new NpgsqlException("connection reset", new IOException("reset"))));
        Assert.False(CommandText.IsPermanent(new TimeoutException()));
    }

    private sealed class NullMessage : Exception
    {
        public override string Message => null!;
    }

    private sealed class ThrowingMessage : Exception
    {
        public override string Message => throw new InvalidOperationException("no message");
    }
}
