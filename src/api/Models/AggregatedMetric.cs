using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;

namespace Pulses.Api.Models;

[Table("aggregated_metrics")]
public sealed class AggregatedMetricEntity
{
    [Key]
    [Column("id")]
    public long Id { get; set; }

    [Column("sensor_id")]
    public Guid SensorId { get; set; }

    [ForeignKey(nameof(SensorId))]
    public Sensor? Sensor { get; set; }

    [Column("window_start")]
    public DateTimeOffset WindowStart { get; set; }

    [Column("window_duration_ms")]
    public int WindowDurationMs { get; set; }

    [Column("avg_value")]
    public double? AvgValue { get; set; }

    [Column("min_value")]
    public double? MinValue { get; set; }

    [Column("max_value")]
    public double? MaxValue { get; set; }

    [Column("count")]
    public int Count { get; set; }

    [Column("std_dev")]
    public double? StdDev { get; set; }

    [Column("created_at")]
    public DateTimeOffset CreatedAt { get; set; } = DateTimeOffset.UtcNow;
}