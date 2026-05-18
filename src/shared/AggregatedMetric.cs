namespace Pulses.Shared;

public sealed record AggregatedMetric
{
    public required Guid SensorId { get; init; }
    public required DateTimeOffset WindowStart { get; init; } // ISO 8601 on wire; DateTimeOffset internally
    public required int WindowDurationMs { get; init; }
    public double AvgValue { get; init; }
    public double MinValue { get; init; }
    public double MaxValue { get; init; }
    public required int Count { get; init; }
    public double StdDev { get; init; }
}