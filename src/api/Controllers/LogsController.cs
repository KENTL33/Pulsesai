using Microsoft.AspNetCore.Mvc;

namespace Pulses.Api.Controllers;

[ApiController]
[Route("api/[controller]")]
public sealed class LogsController : ControllerBase
{
    public LogsController(ILogger<LogsController> logger) { }

    [HttpPost("ingest")]
    public Task<ActionResult> IngestBrowserLogs([FromBody] BrowserLogBatch batch)
    {
        var correlationId = HttpContext.Items["CorrelationId"]?.ToString() ?? "unknown";

        foreach (var entry in batch.Entries)
        {
            var ctxLogger = Serilog.Log.ForContext("BrowserCorrelationId", entry.CorrelationId)
                .ForContext("BrowserTimestamp", entry.Timestamp)
                .ForContext("BrowserComponent", entry.Component ?? "unknown")
                .ForContext("SourceContext", "Browser");

            switch (entry.Level.ToLowerInvariant())
            {
                case "debug": ctxLogger.Debug(entry.Message); break;
                case "info": ctxLogger.Information(entry.Message); break;
                case "warn": ctxLogger.Warning(entry.Message); break;
                case "error": ctxLogger.Error(entry.Message); break;
            }
        }

        return Task.FromResult<ActionResult>(Accepted());
    }
}

public sealed record BrowserLogBatch(List<BrowserLogEntry> Entries);
public sealed record BrowserLogEntry(
    string Level,
    string Message,
    string Timestamp,
    string CorrelationId,
    string? Component,
    Dictionary<string, object>? Metadata);