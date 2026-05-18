namespace Pulses.Shared;

public sealed record SensorEvent
{
    public required Guid SensorId { get; init; }
    public required double Value { get; init; }
    public required long Timestamp { get; init; } // Unix milliseconds — internal pipeline format
    public string Quality { get; init; } = "good";
}