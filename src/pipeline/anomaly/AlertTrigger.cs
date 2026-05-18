using Pulses.Shared;

namespace Pulses.Pipeline.Anomaly;

public sealed class AlertTrigger
{
    public Guid RuleId { get; init; }
    public Guid SensorId { get; init; }
    public required string MetricName { get; init; }
    public required string Operator { get; init; }
    public double Threshold { get; init; }
    public double ActualValue { get; init; }
    public int Severity { get; init; }
    public DateTimeOffset TriggeredAt { get; init; }
    public required string Message { get; init; }

    public Alert ToAlert()
    {
        var severity = Severity switch
        {
            1 => "info",
            2 => "warning",
            >= 3 => "critical",
            _ => "info",
        };
        return new Alert
        {
            Id = Guid.NewGuid(),
            SensorId = SensorId,
            RuleId = RuleId,
            Severity = severity,
            Message = Message,
            ValueAtTrigger = ActualValue,
            ThresholdValue = Threshold,
            Status = "active",
            TriggeredAt = TriggeredAt,
        };
    }
}