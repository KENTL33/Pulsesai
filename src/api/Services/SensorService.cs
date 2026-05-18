using Microsoft.EntityFrameworkCore;
using Pulses.Api.Data;
using Pulses.Api.Models;

namespace Pulses.Api.Services;

public sealed class SensorService
{
    private readonly AppDbContext _db;

    public SensorService(AppDbContext db) => _db = db;

    public async Task<IReadOnlyList<Sensor>> GetActiveSensorsAsync()
        => await _db.Sensors.Where(s => s.IsActive).OrderBy(s => s.Name).ToListAsync();

    public async Task<Sensor?> GetByIdAsync(Guid id)
        => await _db.Sensors.FindAsync(id);

    public async Task<Sensor> CreateAsync(Sensor sensor)
    {
        sensor.Id = Guid.NewGuid();
        sensor.CreatedAt = DateTimeOffset.UtcNow;
        sensor.UpdatedAt = DateTimeOffset.UtcNow;
        _db.Sensors.Add(sensor);
        await _db.SaveChangesAsync();
        return sensor;
    }

    public async Task<bool> UpdateActiveStatusAsync(Guid id, bool isActive)
    {
        var rows = await _db.Sensors.Where(s => s.Id == id)
            .ExecuteUpdateAsync(s => s
                .SetProperty(x => x.IsActive, isActive)
                .SetProperty(x => x.UpdatedAt, DateTimeOffset.UtcNow));
        return rows > 0;
    }
}