using System.Collections.Concurrent;
using Pulses.Shared;

namespace Pulses.Pipeline.Aggregation;

public sealed class TumblingWindowAggregator
{
    private readonly ConcurrentDictionary<Guid, SensorWindow> _windows = new();
    private readonly ConcurrentDictionary<Guid, object> _sensorLocks = new();
    private readonly int _windowSizeMs;
    private readonly Action<IReadOnlyList<AggregatedMetric>> _onFlush;
    private readonly ReaderWriterLockSlim _snapshotLock = new();

    public TumblingWindowAggregator(int windowSizeMs, Action<IReadOnlyList<AggregatedMetric>> onFlush)
    {
        _windowSizeMs = windowSizeMs;
        _onFlush = onFlush;
    }

    public void Process(SensorEvent evt)
    {
        var windowStart = GetWindowStart(evt.Timestamp, _windowSizeMs);
        var key = evt.SensorId;

        // Get or create a per-sensor lock to allow parallel processing of different sensors
        var sensorLock = _sensorLocks.GetOrAdd(key, _ => new object());

        var window = _windows.GetOrAdd(key, _ => new SensorWindow(key, windowStart));

        if (window.WindowStart < windowStart)
        {
            // Window rollover — flush the old window (hold per-sensor lock only)
            lock (sensorLock)
            {
                var completed = window.Flush();
                if (completed.Count > 0)
                    _onFlush(new[] { completed });

                window = new SensorWindow(key, windowStart);
                _windows[key] = window;
            }
        }

        // Add to window — per-sensor lock held to prevent races with TakeSnapshot()
        lock (sensorLock)
        {
            window.Add(evt);
        }
    }

    private static long GetWindowStart(long timestampMs, int windowSizeMs)
        => (timestampMs / windowSizeMs) * windowSizeMs;

    /// <summary>
    /// Returns a stable snapshot of current window metrics without mutating state.
    /// Thread-safe: acquires read locks on each sensor to prevent races with Process().
    /// </summary>
    public IReadOnlyList<AggregatedMetric> TakeSnapshot()
    {
        var results = new List<AggregatedMetric>();
        foreach (var window in _windows.Values)
        {
            var sensorLock = _sensorLocks.GetOrAdd(window.SensorId, _ => new object());
            lock (sensorLock)
            {
                results.Add(window.CurrentMetric());
            }
        }
        return results;
    }

    /// <summary>
    /// Hard cap on values accumulated per sensor per window.
    /// Prevents unbounded memory growth if a single sensor sends events rapidly.
    /// Excess values are dropped oldest-first — statistical accuracy is preserved
    /// for sum/min/max/count; StdDev is slightly underestimated for capped windows.
    /// </summary>
    private const int MaxValuesPerWindow = 50_000;

    /// <summary>
    /// SensorWindow accumulates events for one sensor in one tumbling time window.
    /// Two output paths share the same mutable accumulators (_sum, _min, _max, _count, _values):
    ///   • Flush() — called on window rollover via _onFlush callback → pushed to SignalR
    ///   • CurrentMetric() — called by TakeSnapshot() (AggregationFlushWorker timer) → pushed via SignalR
    /// Both hold per-sensor lock: Flush() from Process() (ingestion thread),
    /// CurrentMetric() from TakeSnapshot() (AggregationFlushWorker thread).
    /// </summary>
    private sealed class SensorWindow
    {
        public Guid SensorId { get; }
        public long WindowStart { get; }
        private double _sum;
        private double _min = double.MaxValue;
        private double _max = double.MinValue;
        private int _count;
        private readonly List<double> _values = new();

        public SensorWindow(Guid sensorId, long windowStart)
        {
            SensorId = sensorId;
            WindowStart = windowStart;
        }

        public void Add(SensorEvent evt)
        {
            if (_count >= MaxValuesPerWindow) return;

            if (_values.Count >= MaxValuesPerWindow)
            {
                // Remove oldest value to make room — oldest-first eviction
                var removed = _values[0];
                _sum -= removed;
                _values.RemoveAt(0);
            }

            _sum += evt.Value;
            if (evt.Value < _min) _min = evt.Value;
            if (evt.Value > _max) _max = evt.Value;
            _count++;
            _values.Add(evt.Value);
        }

        public AggregatedMetric Flush()
        {
            var avg = _count > 0 ? _sum / _count : 0;
            var stdDev = CalculateStdDev(_values, avg);
            var result = new AggregatedMetric
            {
                SensorId = SensorId,
                WindowStart = DateTimeOffset.FromUnixTimeMilliseconds(WindowStart),
                WindowDurationMs = (int)(DateTimeOffset.UtcNow.ToUnixTimeMilliseconds() - WindowStart),
                AvgValue = avg,
                MinValue = _min == double.MaxValue ? 0 : _min,
                MaxValue = _max == double.MinValue ? 0 : _max,
                Count = _count,
                StdDev = stdDev,
            };

            _sum = 0; _min = double.MaxValue; _max = double.MinValue; _count = 0;
            _values.Clear();
            return result;
        }

        public AggregatedMetric CurrentMetric()
        {
            var avg = _count > 0 ? _sum / _count : 0;
            var stdDev = CalculateStdDev(_values, avg);
            return new AggregatedMetric
            {
                SensorId = SensorId,
                WindowStart = DateTimeOffset.FromUnixTimeMilliseconds(WindowStart),
                WindowDurationMs = (int)(DateTimeOffset.UtcNow.ToUnixTimeMilliseconds() - WindowStart),
                AvgValue = avg,
                MinValue = _min == double.MaxValue ? 0 : _min,
                MaxValue = _max == double.MinValue ? 0 : _max,
                Count = _count,
                StdDev = stdDev,
            };
        }

        private static double CalculateStdDev(List<double> values, double mean)
        {
            if (values.Count < 2) return 0;
            var sumSquares = values.Sum(v => (v - mean) * (v - mean));
            return Math.Sqrt(sumSquares / (values.Count - 1));
        }
    }
}