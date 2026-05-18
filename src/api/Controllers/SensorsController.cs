using Microsoft.AspNetCore.Mvc;
using Microsoft.EntityFrameworkCore;
using Pulses.Api.Data;
using Pulses.Api.Models;

namespace Pulses.Api.Controllers;

[ApiController]
[Route("api/[controller]")]
public sealed class SensorsController : ControllerBase
{
    private readonly AppDbContext _db;

    public SensorsController(AppDbContext db) => _db = db;

    [HttpGet]
    public async Task<ActionResult<IReadOnlyList<Sensor>>> GetAll([FromQuery] bool? isActive = null)
    {
        var query = _db.Sensors.AsQueryable();
        if (isActive.HasValue)
            query = query.Where(s => s.IsActive == isActive.Value);

        var sensors = await query.OrderBy(s => s.Name).ToListAsync();
        return Ok(sensors);
    }

    [HttpGet("{id:guid}")]
    public async Task<ActionResult<Sensor>> GetById(Guid id)
    {
        var sensor = await _db.Sensors.FindAsync(id);
        return sensor is null ? NotFound() : Ok(sensor);
    }

    [HttpPost]
    public async Task<ActionResult<Sensor>> Create([FromBody] CreateSensorRequest request)
    {
        if (string.IsNullOrWhiteSpace(request.Name) || string.IsNullOrWhiteSpace(request.Type))
            return BadRequest("Name and Type are required");

        var sensor = new Sensor
        {
            Id = Guid.NewGuid(),
            Name = request.Name,
            Type = request.Type,
            Unit = request.Unit,
            Location = request.Location,
            IsActive = request.IsActive ?? true,
            CreatedAt = DateTimeOffset.UtcNow,
            UpdatedAt = DateTimeOffset.UtcNow,
        };

        _db.Sensors.Add(sensor);
        await _db.SaveChangesAsync();
        return Created($"/api/sensors/{sensor.Id}", sensor);
    }

    [HttpPatch("{id:guid}")]
    public async Task<ActionResult> Update(Guid id, [FromBody] UpdateSensorRequest request)
    {
        var sensor = await _db.Sensors.FindAsync(id);
        if (sensor is null)
            return NotFound();

        if (request.Name is not null)
            sensor.Name = request.Name;
        if (request.Unit is not null)
            sensor.Unit = request.Unit;
        if (request.Location is not null)
            sensor.Location = request.Location;
        if (request.IsActive.HasValue)
            sensor.IsActive = request.IsActive.Value;

        sensor.UpdatedAt = DateTimeOffset.UtcNow;

        await _db.SaveChangesAsync();
        return NoContent();
    }

    [HttpDelete("{id:guid}")]
    public async Task<ActionResult> Delete(Guid id)
    {
        var rows = await _db.Sensors.Where(s => s.Id == id).ExecuteDeleteAsync();
        return rows == 0 ? NotFound() : NoContent();
    }
}

public sealed record CreateSensorRequest(
    string Name,
    string Type,
    string? Unit = null,
    string? Location = null,
    bool? IsActive = true);

public sealed record UpdateSensorRequest(
    string? Name = null,
    string? Unit = null,
    string? Location = null,
    bool? IsActive = null);