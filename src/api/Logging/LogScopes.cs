using Serilog;
using Serilog.Context;

namespace Pulses.Api.Logging;

// ── Correlation and Batch scopes ──────────────────────────────────────────────

public sealed class CorrelationScope : IDisposable
{
    private readonly IDisposable _property;
    public CorrelationScope(string correlationId)
        => _property = LogContext.PushProperty("CorrelationId", correlationId);
    public void Dispose() => _property.Dispose();
}

public sealed class BatchScope : IDisposable
{
    private readonly IDisposable _property;
    public BatchScope(Guid batchId)
        => _property = LogContext.PushProperty("BatchId", batchId);
    public void Dispose() => _property.Dispose();
}

// ── Pipeline metrics logger (atomic counters, periodic snapshot) ───────────────

public sealed class PipelineMetricsLogger : IDisposable
{
    private readonly Serilog.ILogger _logger;
    private readonly int _reportIntervalMs;
    private CancellationTokenSource? _cts;
    private Task? _reportTask;

    private long _retryCount;
    private long _droppedCount;
    private long _backpressureCount;
    private long _circuitOpenCount;

    public PipelineMetricsLogger(Serilog.ILogger logger, int reportIntervalMs = 15000)
    {
        _logger = logger;
        _reportIntervalMs = reportIntervalMs;
    }

    public void IncrementRetry() => Interlocked.Increment(ref _retryCount);
    public void IncrementDropped() => Interlocked.Increment(ref _droppedCount);
    public void IncrementBackpressure() => Interlocked.Increment(ref _backpressureCount);
    public void IncrementCircuitOpen() => Interlocked.Increment(ref _circuitOpenCount);

    public long PersistenceDroppedTotal => Interlocked.Read(ref _persistenceDroppedTotal);
    private long _persistenceDroppedTotal;

    public void IncrementPersistenceDropped() => Interlocked.Increment(ref _persistenceDroppedTotal);

    public void StartReporting()
    {
        _cts = new CancellationTokenSource();
        _reportTask = Task.Run(async () =>
        {
            while (!_cts.Token.IsCancellationRequested)
            {
                await Task.Delay(_reportIntervalMs, _cts.Token);
                LogMetricsSnapshot();
            }
        });
    }

    public void StopReporting()
    {
        _cts?.Cancel();
        _reportTask?.Wait(TimeSpan.FromSeconds(2));
        LogMetricsSnapshot();
    }

    private void LogMetricsSnapshot()
    {
        var retries = Interlocked.Exchange(ref _retryCount, 0);
        var dropped = Interlocked.Exchange(ref _droppedCount, 0);
        var backpressure = Interlocked.Exchange(ref _backpressureCount, 0);
        var circuitOpen = Interlocked.Read(ref _circuitOpenCount);
        var persistenceDropped = Interlocked.Read(ref _persistenceDroppedTotal);

        Log.Information(
            "[Metrics] Retries={Retries} Drops={Drops} BackpressureEvents={BackpressureEvents} CircuitOpens={CircuitOpens} PersistenceDropped={PersistenceDropped}",
            retries, dropped, backpressure, circuitOpen, persistenceDropped);
    }

    public void Dispose()
    {
        StopReporting();
        _cts?.Dispose();
    }
}

// ── Log sampling for high-volume noisy areas ───────────────────────────────────

public sealed class SamplingPolicy
{
    private readonly System.Collections.Concurrent.ConcurrentDictionary<string, int> _counters = new();
    private readonly int _sampleRate;

    public SamplingPolicy(int sampleRate = 10) => _sampleRate = sampleRate;

    public bool ShouldEmit(string sourceContext, Serilog.Events.LogEventLevel level)
    {
        var key = $"{sourceContext}:{level}";
        var count = _counters.AddOrUpdate(key, 1, (_, v) => v + 1);
        return count % _sampleRate == 1;
    }
}

// ── Structured logging helpers ─────────────────────────────────────────────────

public static class StructuredLogging
{
    public static void LogIngestionCheckpoint(Guid batchId, int eventCount)
        => Log.Information("Batch {BatchId} received. Size: {EventCount}", batchId, eventCount);

    public static void LogAggregationCheckpoint(Guid batchId)
        => Log.Information("Batch {BatchId} aggregation complete.", batchId);

    public static void LogPersistenceCheckpoint(Guid batchId, int count)
        => Log.Information("Batch {BatchId} persisted to storage. Count: {Count}", batchId, count);

    public static void LogPersistenceFailure(Guid batchId, int count, Exception ex)
        => Log.Error(ex,
            "Persistence failure in Batch {BatchId}. Count: {Count}. Error: {ErrorMessage}. Timestamp: {Timestamp:O}",
            batchId, count, ex.Message, DateTimeOffset.UtcNow);

    public static void LogRetry(string operation, int attempt, int maxAttempts, Exception ex)
        => Log.Warning(ex,
            "Retry {Attempt}/{MaxAttempts} for {Operation}. Reason: {Reason}",
            attempt, maxAttempts, operation, ex.Message);

    public static void LogCircuitOpen(string circuitName, string reason)
        => Log.Warning("Circuit breaker [{CircuitName}] OPENED. Reason: {Reason}. Calling service will fail fast.",
            circuitName, reason);

    public static void LogCircuitHalfOpen(string circuitName)
        => Log.Information("Circuit breaker [{CircuitName}] HALF-OPEN. Testing with limited requests.", circuitName);

    public static void LogCircuitClosed(string circuitName)
        => Log.Information("Circuit breaker [{CircuitName}] CLOSED. Service recovered.", circuitName);

    public static void LogHealthCheck(string probeName, string status, string? detail = null)
    {
        if (status == "healthy")
            Log.Debug("Health check [{Probe}]: {Status}", probeName, status);
        else if (status == "degraded")
            Log.Warning("Health check [{Probe}]: {Status} — {Detail}", probeName, status, detail ?? "no detail");
        else
            Log.Error("Health check [{Probe}]: {Status} — {Detail}", probeName, status, detail ?? "unknown");
    }

    public static void LogSamplingDrop(string sourceContext, Serilog.Events.LogEventLevel level, int sampledRate)
        => Log.Verbose("Sampled log dropped: [{SourceContext}] {Level} (1-in-{Rate})", sourceContext, level, sampledRate);
}