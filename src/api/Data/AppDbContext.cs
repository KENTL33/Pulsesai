using Microsoft.EntityFrameworkCore;
using Pulses.Api.Models;

namespace Pulses.Api.Data;

public sealed class AppDbContext : DbContext
{
    public AppDbContext(DbContextOptions<AppDbContext> options) : base(options) { }

    public DbSet<Sensor> Sensors => Set<Sensor>();
    public DbSet<AlertEntity> Alerts => Set<AlertEntity>();
    public DbSet<AlertRule> AlertRules => Set<AlertRule>();
    public DbSet<AggregatedMetricEntity> AggregatedMetrics => Set<AggregatedMetricEntity>();

    protected override void OnModelCreating(ModelBuilder modelBuilder)
    {
        modelBuilder.Entity<Sensor>(entity =>
        {
            entity.HasIndex(e => e.Name);
            entity.HasIndex(e => e.Type);
            entity.HasIndex(e => e.IsActive);
        });

        modelBuilder.Entity<AlertEntity>(entity =>
        {
            entity.HasIndex(e => new { e.Status, e.TriggeredAt });
            entity.HasIndex(e => new { e.SensorId, e.TriggeredAt });
        });

        modelBuilder.Entity<AlertRule>(entity =>
        {
            entity.HasIndex(e => e.SensorId);
            entity.HasIndex(e => e.IsEnabled);
        });

        modelBuilder.Entity<AggregatedMetricEntity>(entity =>
        {
            entity.HasIndex(e => new { e.SensorId, e.WindowStart });
            entity.HasIndex(e => e.WindowStart);
            entity.HasIndex(e => e.CreatedAt);
        });
    }
}