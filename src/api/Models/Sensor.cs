using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;

namespace Pulses.Api.Models;

[Table("sensors")]
public sealed class Sensor
{
    [Key]
    [Column("id")]
    public Guid Id { get; set; }

    [Required, MaxLength(255)]
    [Column("name")]
    public required string Name { get; set; }

    [Required, MaxLength(100)]
    [Column("type")]
    public required string Type { get; set; }

    [MaxLength(50)]
    [Column("unit")]
    public string? Unit { get; set; }

    [MaxLength(255)]
    [Column("location")]
    public string? Location { get; set; }

    [Column("is_active")]
    public bool IsActive { get; set; } = true;

    [Column("created_at")]
    public DateTimeOffset CreatedAt { get; set; } = DateTimeOffset.UtcNow;

    [Column("updated_at")]
    public DateTimeOffset UpdatedAt { get; set; } = DateTimeOffset.UtcNow;

    public ICollection<AlertRule> AlertRules { get; set; } = new List<AlertRule>();
    public ICollection<AlertEntity> Alerts { get; set; } = new List<AlertEntity>();
}