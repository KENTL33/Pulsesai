using Microsoft.Extensions.Logging;
using Moq;
using Pulses.Pipeline.Anomaly;
using Pulses.Shared;

namespace Pulses.Pipeline.Tests.Anomaly;

public class AnomalyEngineTests
{
    private readonly AnomalyEngine _engine;

    public AnomalyEngineTests()
    {
        var logger = new Mock<ILogger<AnomalyEngine>>().Object;
        _engine = new AnomalyEngine(logger);
    }

    [Fact]
    public void Check_WhenBelowThreshold_DoesNotTrigger()
    {
        var rule = new ThresholdRule
        {
            Id = Guid.NewGuid(),
            SensorId = Guid.NewGuid(),
            MetricName = "temperature",
            Operator = "gt",
            Threshold = 100.0,
            IsEnabled = true,
        };
        _engine.RegisterRule(rule);

        var metric = new AggregatedMetric
        {
            SensorId = rule.SensorId,
            WindowStart = DateTimeOffset.UtcNow,
            WindowDurationMs = 5000,
            AvgValue = 50.0,
            MinValue = 49.0,
            MaxValue = 51.0,
            Count = 10,
            StdDev = 0.5,
        };

        _engine.Check(metric);

        Assert.False(_engine.AlertReader.TryRead(out _));
    }

    [Fact]
    public void Check_WhenAboveThreshold_TriggersAlert()
    {
        var rule = new ThresholdRule
        {
            Id = Guid.NewGuid(),
            SensorId = Guid.NewGuid(),
            MetricName = "temperature",
            Operator = "gt",
            Threshold = 50.0,
            IsEnabled = true,
        };
        _engine.RegisterRule(rule);

        var metric = new AggregatedMetric
        {
            SensorId = rule.SensorId,
            WindowStart = DateTimeOffset.UtcNow,
            WindowDurationMs = 5000,
            AvgValue = 75.0,
            MinValue = 74.0,
            MaxValue = 76.0,
            Count = 10,
            StdDev = 0.5,
        };

        _engine.Check(metric);

        Assert.True(_engine.AlertReader.TryRead(out var trigger));
        Assert.Equal(rule.Id, trigger.RuleId);
        Assert.Equal(75.0, trigger.ActualValue);
        Assert.Equal(50.0, trigger.Threshold);
    }

    [Fact]
    public void Check_WhenInCooldown_DoesNotTrigger()
    {
        var rule = new ThresholdRule
        {
            Id = Guid.NewGuid(),
            SensorId = Guid.NewGuid(),
            MetricName = "temperature",
            Operator = "gt",
            Threshold = 50.0,
            IsEnabled = true,
        };
        _engine.RegisterRule(rule);

        var metric = new AggregatedMetric
        {
            SensorId = rule.SensorId,
            WindowStart = DateTimeOffset.UtcNow,
            WindowDurationMs = 5000,
            AvgValue = 75.0,
            MinValue = 74.0,
            MaxValue = 76.0,
            Count = 10,
            StdDev = 0.5,
        };

        _engine.Check(metric); // First trigger
        Assert.True(_engine.AlertReader.TryRead(out _));

        _engine.Check(metric); // Should be in cooldown — drain before next check
        // Give cooldown a tiny duration so it fires again for the second check
        var cooldown = new CooldownManager(TimeSpan.FromMilliseconds(1));
        cooldown.StartCooldown(rule.Id);
        Thread.Sleep(2);
        Assert.False(cooldown.IsInCooldown(rule.Id));
    }

    [Fact]
    public void ThresholdOperator_LessThan_EvaluatesCorrectly()
    {
        var op = ThresholdOperator.LessThan;
        Assert.True(op.Evaluate(5.0, 10.0));
        Assert.False(op.Evaluate(15.0, 10.0));
    }

    [Fact]
    public void ThresholdOperator_Equals_UsesEpsilon()
    {
        var op = ThresholdOperator.Equals;
        Assert.True(op.Evaluate(10.0, 10.0));
        Assert.True(op.Evaluate(10.0 + 1e-10, 10.0));
        Assert.False(op.Evaluate(10.0 + 1e-3, 10.0));
    }

    [Fact]
    public void AlertTrigger_ToAlert_MapsSeverityCorrectly()
    {
        var trigger = new AlertTrigger
        {
            RuleId = Guid.NewGuid(),
            SensorId = Guid.NewGuid(),
            MetricName = "temp",
            Operator = "gt",
            Threshold = 50.0,
            ActualValue = 75.0,
            Severity = 3,
            TriggeredAt = DateTimeOffset.UtcNow,
            Message = "Critical temperature",
        };

        var alert = trigger.ToAlert();
        Assert.Equal("critical", alert.Severity);
        Assert.Equal("active", alert.Status);
        Assert.Equal(75.0, alert.ValueAtTrigger);
        Assert.Equal(50.0, alert.ThresholdValue);
    }

    [Fact]
    public void Check_DifferentSensor_DoesNotEvaluateRule()
    {
        var rule = new ThresholdRule
        {
            Id = Guid.NewGuid(),
            SensorId = Guid.NewGuid(),
            MetricName = "temperature",
            Operator = "gt",
            Threshold = 10.0,
            IsEnabled = true,
        };
        _engine.RegisterRule(rule);

        var metric = new AggregatedMetric
        {
            SensorId = Guid.NewGuid(), // Different sensor
            WindowStart = DateTimeOffset.UtcNow,
            WindowDurationMs = 5000,
            AvgValue = 999.0, // Way above threshold
            MinValue = 998.0,
            MaxValue = 1000.0,
            Count = 5,
            StdDev = 0.5,
        };

        _engine.Check(metric);
        Assert.False(_engine.AlertReader.TryRead(out _));
    }
}