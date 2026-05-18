using Microsoft.EntityFrameworkCore;
using Pulses.Api.Data;
using Pulses.Api.Models;

namespace Pulses.Api.Services;

public sealed class MetricsService
{
    private readonly AppDbContext _db;

    public MetricsService(AppDbContext db) => _db = db;

    public async Task<IReadOnlyList<AggregatedMetricEntity>> GetMetricsAsync(
        Guid sensorId, DateTimeOffset from, DateTimeOffset to, int limit = 300)
        => await _db.AggregatedMetrics
            .Where(m => m.SensorId == sensorId && m.WindowStart >= from && m.WindowStart <= to)
            .OrderByDescending(m => m.WindowStart)
            .Take(limit)
            .ToListAsync();

    public async Task SaveBatchAsync(IReadOnlyList<AggregatedMetricEntity> metrics)
    {
        if (metrics.Count == 0) return;
        _db.AggregatedMetrics.AddRange(metrics);
        await _db.SaveChangesAsync();
    }
}