using Pulses.Shared;

namespace Pulses.Pipeline.Aggregation;

public sealed class MetricBuffer
{
    private readonly AggregatedMetric[] _buffer;
    private int _head;
    private int _count;
    private readonly int _capacity;

    public MetricBuffer(int capacity = 1200)
    {
        _capacity = capacity;
        _buffer = new AggregatedMetric[capacity];
    }

    public void Add(AggregatedMetric metric)
    {
        _buffer[_head] = metric;
        _head = (_head + 1) % _capacity;
        if (_count < _capacity) _count++;
    }

    public AggregatedMetric[] GetAll()
    {
        if (_count == 0) return Array.Empty<AggregatedMetric>();
        var result = new AggregatedMetric[_count];
        for (var i = 0; i < _count; i++)
        {
            var idx = (_head - _count + i + _capacity) % _capacity;
            result[i] = _buffer[idx]!;
        }
        return result;
    }

    public AggregatedMetric? GetLatest()
        => _count > 0 ? _buffer[(_head - 1 + _capacity) % _capacity] : null;
}