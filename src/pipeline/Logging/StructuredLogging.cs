using Serilog;

namespace Pulses.Pipeline.Logging;

public static class StructuredLogging
{
    public static void LogIngestionCheckpoint(Guid batchId, int eventCount)
        => Log.Information("Batch {BatchId} received. Size: {EventCount}", batchId, eventCount);

    public static void LogAggregationCheckpoint(Guid batchId)
        => Log.Information("Batch {BatchId} aggregation complete.", batchId);
}