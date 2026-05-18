namespace Pulses.Pipeline.Anomaly;

public enum ThresholdOperator
{
    GreaterThan,
    LessThan,
    GreaterThanOrEq,
    LessThanOrEq,
    Equals,
}

public static class ThresholdOperatorExtensions
{
    public static bool Evaluate(this ThresholdOperator op, double value, double threshold)
        => op switch
        {
            ThresholdOperator.GreaterThan      => value > threshold,
            ThresholdOperator.LessThan         => value < threshold,
            ThresholdOperator.GreaterThanOrEq  => value >= threshold,
            ThresholdOperator.LessThanOrEq      => value <= threshold,
            ThresholdOperator.Equals            => Math.Abs(value - threshold) < 1e-9,
            _                                   => false,
        };

    public static ThresholdOperator FromString(string op)
        => op.ToLowerInvariant() switch
        {
            "gt"  => ThresholdOperator.GreaterThan,
            "lt"  => ThresholdOperator.LessThan,
            "gte" => ThresholdOperator.GreaterThanOrEq,
            "lte" => ThresholdOperator.LessThanOrEq,
            "eq"  => ThresholdOperator.Equals,
            _     => throw new ArgumentOutOfRangeException(nameof(op), op, "Unknown operator"),
        };

    public static string ToSymbol(this ThresholdOperator op)
        => op switch
        {
            ThresholdOperator.GreaterThan      => ">",
            ThresholdOperator.LessThan         => "<",
            ThresholdOperator.GreaterThanOrEq  => ">=",
            ThresholdOperator.LessThanOrEq      => "<=",
            ThresholdOperator.Equals           => "==",
            _                                   => "?",
        };
}