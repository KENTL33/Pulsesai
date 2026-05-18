using Microsoft.AspNetCore.Mvc;
using Microsoft.EntityFrameworkCore;
using Pulses.Api.Data;
using Pulses.Api.Models;
using Pulses.Shared;

namespace Pulses.Api.Controllers;

[ApiController]
[Route("api/[controller]")]
public sealed class AlertsController : ControllerBase
{
    private readonly AppDbContext _db;

    public AlertsController(AppDbContext db) => _db = db;

    [HttpGet]
    public async Task<ActionResult<IReadOnlyList<Alert>>> GetAll([FromQuery] string? status = null)
    {
        var query = _db.Alerts.AsQueryable();
        if (!string.IsNullOrEmpty(status))
            query = query.Where(a => a.Status == status);

        var alerts = await query
            .OrderByDescending(a => a.TriggeredAt)
            .Select(a => new Alert
            {
                Id = a.Id,
                SensorId = a.SensorId,
                RuleId = a.RuleId,
                Severity = a.Severity,
                Message = a.Message,
                ValueAtTrigger = a.ValueAtTrigger,
                ThresholdValue = a.ThresholdValue,
                Status = a.Status,
                TriggeredAt = a.TriggeredAt,
                AcknowledgedAt = a.AcknowledgedAt,
                ResolvedAt = a.ResolvedAt,
            })
            .ToListAsync();

        return Ok(alerts);
    }

    [HttpGet("{id:guid}")]
    public async Task<ActionResult<Alert>> GetById(Guid id)
    {
        var alert = await _db.Alerts.FindAsync(id);
        if (alert is null) return NotFound();

        return Ok(new Alert
        {
            Id = alert.Id,
            SensorId = alert.SensorId,
            RuleId = alert.RuleId,
            Severity = alert.Severity,
            Message = alert.Message,
            ValueAtTrigger = alert.ValueAtTrigger,
            ThresholdValue = alert.ThresholdValue,
            Status = alert.Status,
            TriggeredAt = alert.TriggeredAt,
            AcknowledgedAt = alert.AcknowledgedAt,
            ResolvedAt = alert.ResolvedAt,
        });
    }

    [HttpPatch("{id:guid}/ack")]
    public async Task<ActionResult> Acknowledge(Guid id)
    {
        var rows = await _db.Alerts.Where(a => a.Id == id)
            .ExecuteUpdateAsync(a => a
                .SetProperty(x => x.Status, "acknowledged")
                .SetProperty(x => x.AcknowledgedAt, DateTimeOffset.UtcNow));

        return rows == 0 ? NotFound() : NoContent();
    }

    [HttpPatch("{id:guid}/resolve")]
    public async Task<ActionResult> Resolve(Guid id)
    {
        var rows = await _db.Alerts.Where(a => a.Id == id)
            .ExecuteUpdateAsync(a => a
                .SetProperty(x => x.Status, "resolved")
                .SetProperty(x => x.ResolvedAt, DateTimeOffset.UtcNow));

        return rows == 0 ? NotFound() : NoContent();
    }

    [HttpPost("rules")]
    public async Task<ActionResult<AlertRule>> CreateRule([FromBody] CreateRuleRequest request)
    {
        if (request.SensorId == Guid.Empty)
            return BadRequest("SensorId is required");

        var validOperators = new[] { "gt", "lt", "gte", "lte" };
        if (!validOperators.Contains(request.Operator.ToLowerInvariant()))
            return BadRequest($"Operator must be one of: {string.Join(", ", validOperators)}");

        var rule = new AlertRule
        {
            Id = Guid.NewGuid(),
            SensorId = request.SensorId,
            Metric = request.Metric ?? "value",
            Operator = request.Operator.ToLowerInvariant(),
            ThresholdValue = request.ThresholdValue,
            Severity = request.Severity ?? "warning",
            CooldownSeconds = request.CooldownSeconds ?? 60,
            IsEnabled = request.IsEnabled ?? true,
        };

        _db.AlertRules.Add(rule);
        await _db.SaveChangesAsync();
        return Created($"/api/alerts/rules/{rule.Id}", rule);
    }

    [HttpGet("rules")]
    public async Task<ActionResult<IReadOnlyList<AlertRule>>> GetRules([FromQuery] Guid? sensorId = null)
    {
        var query = _db.AlertRules.AsQueryable();
        if (sensorId.HasValue)
            query = query.Where(r => r.SensorId == sensorId.Value);

        return Ok(await query.ToListAsync());
    }
}

public sealed record CreateRuleRequest(
    Guid SensorId,
    string? Metric,
    string Operator,
    double ThresholdValue,
    string? Severity,
    int? CooldownSeconds,
    bool? IsEnabled);