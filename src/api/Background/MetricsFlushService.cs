using System.Collections.Concurrent;
using Pulses.Api.Data;
using Pulses.Api.Models;
using Pulses.Shared;
using Serilog;

namespace Pulses.Api.Background;

public sealed class MetricsFlushService : BackgroundService
{
    private readonly IServiceScopeFactory _scopeFactory;
    private readonly ILogger<MetricsFlushService> _logger;
    private readonly ConcurrentQueue<AggregatedMetricEntity> _flushQueue = new();
    private const int MaxQueueSize = 5000;
    private const int FlushSize = 200;
    private static readonly TimeSpan FlushInterval = TimeSpan.FromSeconds(5);

    /// <summary>
    /// How long to wait for queue space before accepting backpressure.
    /// Set to 50ms so callers experience controlled latency rather than silent data loss.
    /// </summary>
    private static readonly TimeSpan EnqueueWaitTimeout = TimeSpan.FromMilliseconds(50);

    private readonly SemaphoreSlim _queueSpace = new(1, 1);
    private long _persistenceDroppedTotal;
    private long _persistenceBackpressureWaitTotal;

    public MetricsFlushService(IServiceScopeFactory scopeFactory, ILogger<MetricsFlushService> logger)
        => (_scopeFactory, _logger) = (scopeFactory, logger);

    public long PersistenceDroppedTotal => Interlocked.Read(ref _persistenceDroppedTotal);
    public long PersistenceBackpressureWaitTotal => Interlocked.Read(ref _persistenceBackpressureWaitTotal);
    public int QueueDepth => _flushQueue.Count;

    public async Task EnqueueMetricAsync(AggregatedMetric metric, CancellationToken ct = default)
    {
        // Wait up to EnqueueWaitTimeout for queue space before accepting loss
        var waited = false;
        while (_flushQueue.Count >= MaxQueueSize)
        {
            if (!await _queueSpace.WaitAsync(EnqueueWaitTimeout, ct))
            {
                // Queue still full after timeout — record drop and return
                Interlocked.Increment(ref _persistenceDroppedTotal);
                _logger.LogWarning(
                    "Persistence queue full ({MaxSize}), dropping metric for sensor {SensorId}. Total dropped: {TotalDropped}",
                    MaxQueueSize, metric.SensorId, PersistenceDroppedTotal);
                return;
            }
            waited = true;
            _queueSpace.Release();
        }

        if (waited) Interlocked.Increment(ref _persistenceBackpressureWaitTotal);

        _flushQueue.Enqueue(new AggregatedMetricEntity
        {
            SensorId = metric.SensorId,
            WindowStart = metric.WindowStart,
            WindowDurationMs = metric.WindowDurationMs,
            AvgValue = metric.AvgValue,
            MinValue = metric.MinValue,
            MaxValue = metric.MaxValue,
            Count = metric.Count,
            StdDev = metric.StdDev,
        });

        // Trigger flush if we've accumulated enough for a batch
        if (_flushQueue.Count >= FlushSize)
            _ = FlushAsync().ConfigureAwait(false);
    }

    /// <summary>
    /// Legacy synchronous entry point. Transforms silent drop into
    /// backpressure with a brief wait before accepting loss.
    /// Prefer EnqueueMetricAsync in new call sites.
    /// </summary>
    public void EnqueueMetric(AggregatedMetric metric)
    {
        EnqueueMetricAsync(metric).GetAwaiter().GetResult();
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        while (!stoppingToken.IsCancellationRequested)
        {
            await Task.Delay(FlushInterval, stoppingToken);
            await FlushAsync(stoppingToken);
        }
    }

    private async Task FlushAsync(CancellationToken ct = default)
    {
        if (_flushQueue.IsEmpty) return;

        var toFlush = new List<AggregatedMetricEntity>();
        while (toFlush.Count < FlushSize && _flushQueue.TryDequeue(out var metric))
            toFlush.Add(metric);

        if (toFlush.Count == 0) return;

        try
        {
            using var scope = _scopeFactory.CreateScope();
            var db = scope.ServiceProvider.GetRequiredService<AppDbContext>();

            await db.AggregatedMetrics.AddRangeAsync(toFlush, ct);
            await db.SaveChangesAsync(ct);
            _logger.LogDebug("Flushed {Count} metrics to database", toFlush.Count);
        }
        catch (Exception ex)
        {
            // Dead-letter: drop batch, increment counter, log, do NOT re-enqueue
            Interlocked.Add(ref _persistenceDroppedTotal, toFlush.Count);
            _logger.LogError(ex,
                "Persistence failure. Dropped {Count} metrics. Total dropped: {TotalDropped}",
                toFlush.Count, PersistenceDroppedTotal);
        }
        finally
        {
            // Always release queue space permit so next EnqueueMetricAsync can proceed
            if (_queueSpace.CurrentCount == 0)
                _queueSpace.Release();
        }
    }
}