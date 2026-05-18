using System.Threading.Channels;
using Microsoft.Extensions.Logging;
using Pulses.Shared;

namespace Pulses.Pipeline.Anomaly;

public sealed class AnomalyEngine
{
    private readonly Channel<AlertTrigger> _alertStream;
    private readonly Dictionary<Guid, ThresholdRule> _rules = new();
    private readonly CooldownManager _cooldowns;
    private readonly ILogger<AnomalyEngine> _logger;
    private readonly ReaderWriterLockSlim _rulesLock = new();
    private ThresholdRule[] _cachedSnapshot = Array.Empty<ThresholdRule>();

    public ChannelReader<AlertTrigger> AlertReader => _alertStream.Reader;

    public AnomalyEngine(ILogger<AnomalyEngine> logger, TimeSpan? defaultCooldown = null)
    {
        _logger = logger;
        _cooldowns = new CooldownManager(defaultCooldown);
        _alertStream = Channel.CreateBounded<AlertTrigger>(new BoundedChannelOptions(1000)
        {
            SingleReader = true,
            SingleWriter = false,
            FullMode = BoundedChannelFullMode.DropOldest,
        });
        LoadDefaultRules();
    }

    public void LoadDefaultRules()
    {
        _rulesLock.EnterWriteLock();
        try
        {
            _rules.Clear();
            _cachedSnapshot = Array.Empty<ThresholdRule>();
        }
        finally
        {
            _rulesLock.ExitWriteLock();
        }
    }

    public void RegisterRule(ThresholdRule rule)
    {
        _rulesLock.EnterWriteLock();
        try
        {
            _rules[rule.Id] = rule;
            _cachedSnapshot = _rules.Values.ToArray();
        }
        finally
        {
            _rulesLock.ExitWriteLock();
        }
        _logger.LogDebug("Registered anomaly rule {RuleId} for sensor {SensorId}", rule.Id, rule.SensorId);
    }

    public void Check(AggregatedMetric metric)
    {
        _rulesLock.EnterReadLock();
        try
        {
            foreach (var rule in _cachedSnapshot)
            {
                if (!rule.IsEnabled) continue;
                if (rule.SensorId != metric.SensorId) continue;

                var op = rule.ToOperator();
                var value = metric.AvgValue;
                if (!op.Evaluate(value, rule.Threshold)) continue;

                if (_cooldowns.IsInCooldown(rule.Id))
                {
                    _logger.LogDebug("Rule {RuleId} in cooldown, skipping", rule.Id);
                    continue;
                }

                _cooldowns.StartCooldown(rule.Id);

                var triggered = new AlertTrigger
                {
                    RuleId = rule.Id,
                    SensorId = rule.SensorId,
                    MetricName = rule.MetricName,
                    Operator = rule.Operator,
                    Threshold = rule.Threshold,
                    ActualValue = value,
                    Severity = rule.Severity,
                    TriggeredAt = DateTimeOffset.UtcNow,
                    Message = $"[{rule.Severity}] {metric.SensorId}: {rule.MetricName} {ThresholdOperatorExtensions.ToSymbol(op)} {rule.Threshold} (actual: {value:F4})",
                };

                _alertStream.Writer.TryWrite(triggered);
                _logger.LogWarning("Anomaly detected: {Message}", triggered.Message);
            }
        }
        finally
        {
            _rulesLock.ExitReadLock();
        }
    }
}