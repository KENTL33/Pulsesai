namespace Pulses.Shared;

public sealed record Alert
{
    public required Guid Id { get; init; }
    public required Guid SensorId { get; init; }
    public required Guid RuleId { get; init; }
    public required string Severity { get; init; } // 'info', 'warning', 'critical'
    public required string Message { get; init; }
    public required double ValueAtTrigger { get; init; }
    public required double ThresholdValue { get; init; }
    public required string Status { get; init; } // 'active', 'acknowledged', 'resolved'
    public required DateTimeOffset TriggeredAt { get; init; }
    public DateTimeOffset? AcknowledgedAt { get; init; }
    public DateTimeOffset? ResolvedAt { get; init; }
}