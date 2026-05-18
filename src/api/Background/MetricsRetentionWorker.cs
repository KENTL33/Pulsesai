using Microsoft.EntityFrameworkCore;
using Pulses.Api.Data;
using Serilog;

namespace Pulses.Api.Background;

/// <summary>
/// Periodically purges aggregated metric rows older than 24 hours from PostgreSQL.
/// Runs every 60 minutes to avoid frequent DELETE load on the database.
/// Uses a configurable retention period so operators can tune without code changes.
/// </summary>
public sealed class MetricsRetentionWorker : BackgroundService
{
    private readonly IServiceScopeFactory _scopeFactory;
    private readonly ILogger<MetricsRetentionWorker> _logger;

    /// <summary>
    /// How long metrics are retained before being purged. Default: 24 hours.
    /// Can be overridden via the MetricsRetention__Hours configuration key.
    /// </summary>
    private readonly int _retentionHours;

    /// <summary>
    /// How often the retention sweep runs. Fixed at 60 minutes to balance
    /// timeliness of cleanup against database load.
    /// </summary>
    private static readonly TimeSpan SweepInterval = TimeSpan.FromHours(1);

    public MetricsRetentionWorker(
        IServiceScopeFactory scopeFactory,
        ILogger<MetricsRetentionWorker> logger,
        IConfiguration configuration)
    {
        _scopeFactory = scopeFactory;
        _logger = logger;
        _retentionHours = configuration.GetValue("MetricsRetention__Hours", 24);
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        _logger.LogInformation(
            "MetricsRetentionWorker started. Retention period: {RetentionHours}h, sweep interval: {Interval}h",
            _retentionHours, SweepInterval.TotalHours);

        // Run first sweep immediately on startup, then repeat on interval
        while (!stoppingToken.IsCancellationRequested)
        {
            try
            {
                await SweepAsync(stoppingToken);
            }
            catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
            {
                break;
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "MetricsRetentionWorker sweep failed. Will retry at next interval.");
            }

            try
            {
                await Task.Delay(SweepInterval, stoppingToken);
            }
            catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
            {
                break;
            }
        }

        _logger.LogInformation("MetricsRetentionWorker stopped.");
    }

    private async Task SweepAsync(CancellationToken ct)
    {
        var cutoff = DateTimeOffset.UtcNow.AddHours(-_retentionHours);

        _logger.LogDebug("Retention sweep started. Purging metrics older than {Cutoff}", cutoff);

        using var scope = _scopeFactory.CreateScope();
        var db = scope.ServiceProvider.GetRequiredService<AppDbContext>();

        // Use raw SQL for bulk delete to avoid loading entities into memory
        var deleted = await db.Database.ExecuteSqlInterpolatedAsync(
            $@"DELETE FROM ""aggregated_metrics""
               WHERE ""window_start"" < {cutoff}",
            ct);

        if (deleted > 0)
        {
            _logger.LogInformation(
                "Retention sweep complete. Purged {Count:N0} metric rows older than {RetentionHours}h (cutoff: {Cutoff})",
                deleted, _retentionHours, cutoff);
        }
        else
        {
            _logger.LogDebug(
                "Retention sweep complete. No metrics older than {RetentionHours}h (cutoff: {Cutoff})",
                _retentionHours, cutoff);
        }
    }
}