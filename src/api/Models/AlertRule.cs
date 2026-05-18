using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;

namespace Pulses.Api.Models;

[Table("alert_rules")]
public sealed class AlertRule
{
    [Key]
    [Column("id")]
    public Guid Id { get; set; }

    [Column("sensor_id")]
    public Guid SensorId { get; set; }

    [ForeignKey(nameof(SensorId))]
    public Sensor? Sensor { get; set; }

    [Required, MaxLength(100)]
    [Column("metric")]
    public required string Metric { get; set; } // 'value', 'avg', 'min', 'max', 'std_dev'

    [Required, MaxLength(20)]
    [Column("operator")]
    public required string Operator { get; set; } // 'gt', 'lt', 'gte', 'lte', 'eq' — stored as string

    [Column("threshold_value")]
    public double ThresholdValue { get; set; }

    [MaxLength(20)]
    [Column("severity")]
    public string Severity { get; set; } = "warning";

    [Column("is_enabled")]
    public bool IsEnabled { get; set; } = true;

    [Column("cooldown_seconds")]
    public int CooldownSeconds { get; set; } = 60;

    [Column("created_at")]
    public DateTimeOffset CreatedAt { get; set; } = DateTimeOffset.UtcNow;
}