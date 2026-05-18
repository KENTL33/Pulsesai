namespace Pulses.Pipeline.Anomaly;

public sealed class ThresholdRule
{
    public Guid Id { get; init; }
    public Guid SensorId { get; init; }
    public required string MetricName { get; init; }
    public required string Operator { get; init; }
    public double Threshold { get; init; }
    public int Severity { get; init; } = 1;
    public bool IsEnabled { get; init; } = true;

    public ThresholdOperator ToOperator()
        => ThresholdOperatorExtensions.FromString(Operator);
}