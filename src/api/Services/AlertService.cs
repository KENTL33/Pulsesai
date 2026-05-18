using Microsoft.EntityFrameworkCore;
using Pulses.Api.Data;
using Pulses.Api.Models;

namespace Pulses.Api.Services;

public sealed class AlertService
{
    private readonly AppDbContext _db;

    public AlertService(AppDbContext db) => _db = db;

    public async Task<IReadOnlyList<AlertEntity>> GetActiveAlertsAsync()
        => await _db.Alerts
            .Where(a => a.Status == "active")
            .OrderByDescending(a => a.TriggeredAt)
            .Include(a => a.Sensor)
            .ToListAsync();

    public async Task<int> AcknowledgeAsync(Guid alertId, string acknowledgedBy)
        => await _db.Alerts.Where(a => a.Id == alertId)
            .ExecuteUpdateAsync(a => a
                .SetProperty(x => x.Status, "acknowledged")
                .SetProperty(x => x.AcknowledgedAt, DateTimeOffset.UtcNow));

    public async Task<int> ResolveAsync(Guid alertId)
        => await _db.Alerts.Where(a => a.Id == alertId)
            .ExecuteUpdateAsync(a => a
                .SetProperty(x => x.Status, "resolved")
                .SetProperty(x => x.ResolvedAt, DateTimeOffset.UtcNow));

    public async Task<AlertEntity> CreateAlertAsync(AlertEntity alert)
    {
        alert.Id = Guid.NewGuid();
        alert.TriggeredAt = DateTimeOffset.UtcNow;
        _db.Alerts.Add(alert);
        await _db.SaveChangesAsync();
        return alert;
    }

    public async Task<IReadOnlyList<AlertRule>> GetRulesForSensorAsync(Guid sensorId)
        => await _db.AlertRules.Where(r => r.SensorId == sensorId && r.IsEnabled).ToListAsync();
}