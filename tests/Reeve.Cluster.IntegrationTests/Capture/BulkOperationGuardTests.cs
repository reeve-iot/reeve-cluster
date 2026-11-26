using Microsoft.EntityFrameworkCore;
using Reeve.Cluster.Harness.Containers;

namespace Int.Capture;

public sealed class BulkOperationGuardTests(PostgresFixture postgres) : IClassFixture<PostgresFixture>
{
    [Fact]
    public async Task Unmarked_update_of_a_mapped_type_is_refused_without_changing_the_row()
    {
        await using var rig = await CaptureRig.CreateAsync(postgres);
        await using (var connection = await rig.OpenAsync())
        await using (var command = new Npgsql.NpgsqlCommand("INSERT INTO public.setting (id, name, value) VALUES (1, 'one', 'before')", connection))
            await command.ExecuteNonQueryAsync();

        await using var db = rig.NewContext();
        var error = await Assert.ThrowsAsync<InvalidOperationException>(() => db.Settings.ExecuteUpdateAsync(s => s.SetProperty(e => e.Value, "after")));
        Assert.Contains(nameof(Setting), error.Message);
        Assert.Equal("before", await rig.Log.ScalarAsync<string>("SELECT value FROM public.setting WHERE id = 1"));
    }

    [Fact]
    public async Task Marked_update_of_a_mapped_type_changes_the_row()
    {
        await using var rig = await CaptureRig.CreateAsync(postgres);
        await using (var connection = await rig.OpenAsync())
        await using (var command = new Npgsql.NpgsqlCommand("INSERT INTO public.setting (id, name, value) VALUES (1, 'one', 'before')", connection))
            await command.ExecuteNonQueryAsync();

        await using var db = rig.NewContext();
        Assert.Equal(1, await db.Settings.AppendedToChangeLog().ExecuteUpdateAsync(s => s.SetProperty(e => e.Value, "after")));
        Assert.Equal("after", await rig.Log.ScalarAsync<string>("SELECT value FROM public.setting WHERE id = 1"));
    }

    [Fact]
    public async Task Delete_of_an_unmapped_type_is_allowed()
    {
        await using var rig = await CaptureRig.CreateAsync(postgres);
        await using (var connection = await rig.OpenAsync())
        await using (var command = new Npgsql.NpgsqlCommand("INSERT INTO public.audit (id, text) VALUES (1, 'entry')", connection))
            await command.ExecuteNonQueryAsync();

        await using var db = rig.NewContext();
        Assert.Equal(1, await db.Audits.ExecuteDeleteAsync());
        Assert.Equal(0, await rig.CountAsync("public.audit"));
    }
}
