using System.Net.WebSockets;
using System.Text;
using System.Threading.Channels;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Http;
using Pulses.Shared;

namespace Pulses.Pipeline.Ingestion;

public sealed class IngestionServer
{
    private readonly Channel<SensorEvent> _channel;
    private readonly ILogger<IngestionServer> _logger;
    private long _totalIngested;
    private long _droppedTotal;
    private long _windowCount;
    private long _lastValue;
    private DateTime _lastUpdate = DateTime.UtcNow;
    private readonly object _rateLock = new();

    public IngestionServer(Channel<SensorEvent> channel, ILogger<IngestionServer> logger)
    {
        _channel = channel;
        _logger = logger;
    }

    public async Task HandleBatchAsync(HttpContext context)
    {
        context.Response.ContentType = "application/json";

        SensorEvent[]? events;
        try
        {
            events = await context.Request.ReadFromJsonAsync<SensorEvent[]>();
        }
        catch
        {
            context.Response.StatusCode = 400;
            await context.Response.WriteAsJsonAsync(new { error = "Invalid JSON array" });
            return;
        }

        if (events is null || events.Length == 0)
        {
            context.Response.StatusCode = 400;
            await context.Response.WriteAsJsonAsync(new { error = "Empty array" });
            return;
        }

        var accepted = 0;
        foreach (var evt in events)
        {
            if (_channel.Writer.TryWrite(evt))
                accepted++;
            else
                Interlocked.Increment(ref _droppedTotal);
        }

        Interlocked.Add(ref _totalIngested, accepted);
        Interlocked.Increment(ref _windowCount);

        await context.Response.WriteAsJsonAsync(new IngestResponse(
            Accepted: true,
            Position: accepted,
            ServerTimestamp: DateTimeOffset.UtcNow.ToUnixTimeMilliseconds()));
    }

    public IngestionMetrics GetMetrics()
    {
        lock (_rateLock)
        {
            var now = DateTime.UtcNow;
            var elapsed = (now - _lastUpdate).TotalSeconds;
            if (elapsed < 0.5) return new IngestionMetrics(_totalIngested, _lastValue, _droppedTotal, 0);

            var current = Interlocked.Exchange(ref _windowCount, 0);
            var rate = elapsed >= 1 ? current / elapsed : 0;
            _lastValue = (long)rate;
            _lastUpdate = now;
            return new IngestionMetrics(Interlocked.Read(ref _totalIngested), rate, Interlocked.Read(ref _droppedTotal),
                (double)_channel.Reader.Count / 10000);
        }
    }

    private record IngestResponse(bool Accepted, int Position, long ServerTimestamp);
}

public sealed record IngestionMetrics(
    long TotalIngested,
    double RatePerSecond,
    long DroppedTotal,
    double ChannelFillPercent);