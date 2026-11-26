using System.Diagnostics.Metrics;

namespace Unit.Diagnostics;

internal sealed class InstrumentRecorder : IDisposable
{
    private readonly Instrument target;
    private readonly MeterListener listener = new();
    private readonly object sync = new();
    private readonly List<(double Value, IReadOnlyDictionary<string, object?> Tags)> measurements = [];
    private readonly SemaphoreSlim recorded = new(0);

    public InstrumentRecorder(Instrument target)
    {
        this.target = target;
        listener.InstrumentPublished = (instrument, meterListener) =>
        {
            if (ReferenceEquals(instrument, target)) meterListener.EnableMeasurementEvents(instrument);
        };
        listener.SetMeasurementEventCallback<long>(Record);
        listener.SetMeasurementEventCallback<int>(Record);
        listener.SetMeasurementEventCallback<double>(Record);
        listener.Start();
    }

    public IReadOnlyList<(double Value, IReadOnlyDictionary<string, object?> Tags)> Measurements()
    {
        lock (sync) return [.. measurements];
    }

    public void RecordObservable() => listener.RecordObservableInstruments();

    public async Task WaitForMeasurementAsync(TimeSpan timeout)
    {
        if (!await recorded.WaitAsync(timeout))
            throw new TimeoutException("No measurement was recorded before the bound.");
    }

    private void Record<T>(Instrument instrument, T value, ReadOnlySpan<KeyValuePair<string, object?>> tags, object? state) where T : struct
    {
        if (!ReferenceEquals(instrument, target)) return;
        var copy = new Dictionary<string, object?>(tags.Length, StringComparer.Ordinal);
        foreach (var tag in tags) copy[tag.Key] = tag.Value;
        lock (sync) measurements.Add((Convert.ToDouble(value, System.Globalization.CultureInfo.InvariantCulture), copy));
        recorded.Release();
    }

    public void Dispose()
    {
        listener.Dispose();
        recorded.Dispose();
    }
}
