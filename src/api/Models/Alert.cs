using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;

namespace Pulses.Api.Models;

[Table("alerts")]
public sealed class AlertEntity
{
    [Key]
    [Column("id")]
    public Guid Id { get; set; }

    [Column("rule_id")]
    public Guid RuleId { get; set; }

    [Column("sensor_id")]
    public Guid SensorId { get; set; }

    [ForeignKey(nameof(SensorId))]
    public Sensor? Sensor { get; set; }

    [Required, MaxLength(20)]
    [Column("severity")]
    public required string Severity { get; set; }

    [Required]
    [Column("message")]
    public required string Message { get; set; }

    [Column("value_at_trigger")]
    public double ValueAtTrigger { get; set; }

    [Column("threshold_value")]
    public double ThresholdValue { get; set; }

    [Required, MaxLength(20)]
    [Column("status")]
    public string Status { get; set; } = "active";

    [Column("triggered_at")]
    public DateTimeOffset TriggeredAt { get; set; } = DateTimeOffset.UtcNow;

    [Column("acknowledged_at")]
    public DateTimeOffset? AcknowledgedAt { get; set; }

    [Column("resolved_at")]
    public DateTimeOffset? ResolvedAt { get; set; }
}