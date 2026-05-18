using Microsoft.AspNetCore.Mvc;
using Microsoft.EntityFrameworkCore;
using Pulses.Api.Data;
using Pulses.Shared;

namespace Pulses.Api.Controllers;

[ApiController]
[Route("api/[controller]")]
public sealed class MetricsController : ControllerBase
{
    private readonly AppDbContext _db;

    public MetricsController(AppDbContext db) => _db = db;

    [HttpGet("{sensorId:guid}")]
    public async Task<ActionResult<IReadOnlyList<AggregatedMetric>>> GetMetrics(
        Guid sensorId,
        [FromQuery] DateTimeOffset? from = null,
        [FromQuery] DateTimeOffset? to = null,
        [FromQuery] int limit = 300)
    {
        var query = _db.AggregatedMetrics.Where(m => m.SensorId == sensorId);

        if (from.HasValue)
            query = query.Where(m => m.WindowStart >= from.Value);
        if (to.HasValue)
            query = query.Where(m => m.WindowStart <= to.Value);

        var metrics = await query
            .OrderByDescending(m => m.WindowStart)
            .Take(limit)
            .Select(m => new AggregatedMetric
            {
                SensorId = m.SensorId,
                WindowStart = m.WindowStart,
                WindowDurationMs = m.WindowDurationMs,
                AvgValue = m.AvgValue ?? 0,
                MinValue = m.MinValue ?? 0,
                MaxValue = m.MaxValue ?? 0,
                Count = m.Count,
                StdDev = m.StdDev ?? 0,
            })
            .ToListAsync();

        return Ok(metrics);
    }

    [HttpGet("latest")]
    public async Task<ActionResult<IReadOnlyList<AggregatedMetric>>> GetLatestPerSensor()
    {
        var latest = await _db.AggregatedMetrics
            .GroupBy(m => m.SensorId)
            .Select(g => g.OrderByDescending(m => m.WindowStart).First())
            .ToListAsync();

        var result = latest.Select(m => new AggregatedMetric
        {
            SensorId = m.SensorId,
            WindowStart = m.WindowStart,
            WindowDurationMs = m.WindowDurationMs,
            AvgValue = m.AvgValue ?? 0,
            MinValue = m.MinValue ?? 0,
            MaxValue = m.MaxValue ?? 0,
            Count = m.Count,
            StdDev = m.StdDev ?? 0,
        }).ToList();

        return Ok(result);
    }
}