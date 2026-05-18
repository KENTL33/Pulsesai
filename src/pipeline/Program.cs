using Serilog;
using Serilog.Context;
using System.Net.Http.Json;
using System.Threading.Channels;
using Pulses.Shared;
using Pulses.Pipeline.Ingestion;
using Pulses.Pipeline.Aggregation;
using Pulses.Pipeline.Dispatcher;
using Pulses.Pipeline.Anomaly;
using Pulses.Pipeline.Logging;
using Microsoft.AspNetCore.Builder;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

var builder = WebApplication.CreateBuilder(args);

// Bootstrap logger
Log.Logger = new LoggerConfiguration()
    .ReadFrom.Configuration(builder.Configuration)
    .Enrich.FromLogContext()
    .Enrich.WithProperty("Application", "Pulses.Pipeline")
    .WriteTo.Async(a => a.Console(
        outputTemplate: "[{Timestamp:O}] [{Level:u3}] [{BatchId}] {SourceContext} {Message:lj}{NewLine}{Exception}"))
    .CreateBootstrapLogger();

try
{
    builder.Host.UseSerilog((ctx, _, cfg) => cfg
        .ReadFrom.Configuration(ctx.Configuration)
        .Enrich.FromLogContext()
        .Enrich.WithProperty("Application", "Pulses.Pipeline")
        .WriteTo.Async(a => a.Console(
            outputTemplate: "[{Timestamp:O}] [{Level:u3}] [{BatchId}] {SourceContext} {Message:lj}{NewLine}{Exception}")));

    var pipelinePort = int.Parse(builder.Configuration["Pipeline__Port"] ?? "5001");
    var hubUrl = builder.Configuration["SignalR__HubUrl"] ?? "http://api:5000/hubs/analytics";
    var bufferCapacity = int.Parse(builder.Configuration["EventBuffer__Capacity"] ?? "10000");
    var windowSizeMs = int.Parse(builder.Configuration["Aggregation__WindowMs"] ?? "1000");
    var flushIntervalMs = int.Parse(builder.Configuration["EventFlush__IntervalMs"] ?? "1000");

    // Core pipeline components
    var eventChannel = new EventChannel(bufferCapacity);
    var ingestionServer = new IngestionServer(eventChannel.Channel, Microsoft.Extensions.Logging.Abstractions.NullLogger<IngestionServer>.Instance);
    var loggerFactory = LoggerFactory.Create(logging => logging.AddSerilog(Log.Logger));
    var dispatcher = new SignalRDispatcher(hubUrl, loggerFactory.CreateLogger<SignalRDispatcher>());
    var anomalyEngine = new AnomalyEngine(loggerFactory.CreateLogger<AnomalyEngine>());

    // Wire up: aggregator → dispatcher + anomaly engine
    var wiredAggregator = new TumblingWindowAggregator(windowSizeMs, metrics =>
    {
        // Checkpoint: aggregation complete
        var batchId = Guid.NewGuid();
        using (LogContext.PushProperty("BatchId", batchId))
        {
            StructuredLogging.LogAggregationCheckpoint(batchId);

            _ = dispatcher.SendMetricBatchAsync(metrics);
            foreach (var metric in metrics)
                anomalyEngine.Check(metric);
        }
    });

    // Background workers
    builder.Services.AddHostedService(sp => new IngestionWorker(eventChannel.Channel, wiredAggregator));
    builder.Services.AddHostedService(sp => new AggregationFlushWorker(wiredAggregator, dispatcher, flushIntervalMs));
    builder.Services.AddHostedService(sp => new AnomalyAlertWorker(anomalyEngine, dispatcher));

    if (builder.Configuration.GetValue("DemoData:Enabled", false))
    {
        builder.Services.AddHostedService(sp => new DemoDataSeeder(
            anomalyEngine,
            sp.GetRequiredService<ILogger<DemoDataSeeder>>(),
            builder.Configuration["DemoData:ApiBaseUrl"] ?? "http://api:5000",
            builder.Configuration["DemoData:IngestUrl"] ?? "http://localhost:5001/ingest"));
    }

    var app = builder.Build();

    app.MapPost("/ingest", async context =>
    {
        SensorEvent[]? events;
        try { events = await context.Request.ReadFromJsonAsync<SensorEvent[]>(); }
        catch { context.Response.StatusCode = 400; await context.Response.WriteAsJsonAsync(new { error = "Invalid JSON" }); return; }
        if (events is null || events.Length == 0) { context.Response.StatusCode = 400; return; }

        var batchId = Guid.NewGuid();
        using (LogContext.PushProperty("BatchId", batchId))
        {
            var accepted = 0;
            foreach (var evt in events)
                if (eventChannel.TryWrite(evt)) accepted++;
                else break; // channel full — backpressure signal

            StructuredLogging.LogIngestionCheckpoint(batchId, events.Length);
            await context.Response.WriteAsJsonAsync(new { accepted, batchId, serverTime = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds() });
        }
    });

    app.MapGet("/ingest/metrics", () => Results.Ok(ingestionServer.GetMetrics()));
    app.MapGet("/health", () => Results.Ok(new { status = "ok", timestamp = DateTimeOffset.UtcNow }));

    // Start SignalR dispatcher BEFORE host runs — otherwise all push calls are no-ops
    await dispatcher.StartAsync();

    Log.Information("Starting Pulses.Pipeline. Buffer: {Capacity}, Window: {WindowMs}ms, Flush: {FlushMs}ms",
        bufferCapacity, windowSizeMs, flushIntervalMs);
    await app.RunAsync();
}
catch (Exception ex)
{
    Log.Fatal(ex, "Pipeline terminated unexpectedly");
    throw;
}
finally
{
    Log.CloseAndFlush();
}

// ── Background Workers ─────────────────────────────────────────────────────────

public sealed class IngestionWorker : BackgroundService
{
    private readonly Channel<SensorEvent> _channel;
    private readonly TumblingWindowAggregator _aggregator;

    public IngestionWorker(Channel<SensorEvent> channel, TumblingWindowAggregator aggregator)
        => (_channel, _aggregator) = (channel, aggregator);

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        await foreach (var evt in _channel.Reader.ReadAllAsync(stoppingToken))
            _aggregator.Process(evt);
    }
}

public sealed class AggregationFlushWorker : BackgroundService
{
    private readonly TumblingWindowAggregator _aggregator;
    private readonly SignalRDispatcher _dispatcher;
    private readonly int _flushIntervalMs;

    public AggregationFlushWorker(TumblingWindowAggregator aggregator, SignalRDispatcher dispatcher, int flushIntervalMs)
        => (_aggregator, _dispatcher, _flushIntervalMs) = (aggregator, dispatcher, flushIntervalMs);

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        while (!stoppingToken.IsCancellationRequested)
        {
            await Task.Delay(_flushIntervalMs, stoppingToken);
            var snapshot = _aggregator.TakeSnapshot();
            if (snapshot.Count == 0) continue;

            // Skip empty-window metrics (Count == 0) so charts do not receive synthetic zero values.
            var currentMetrics = snapshot.Where(m => m.Count > 0).ToList();
            if (currentMetrics.Count > 0)
                await _dispatcher.SendMetricBatchAsync(currentMetrics);
        }
    }
}

public sealed class AnomalyAlertWorker : BackgroundService
{
    private readonly AnomalyEngine _engine;
    private readonly SignalRDispatcher _dispatcher;

    public AnomalyAlertWorker(AnomalyEngine engine, SignalRDispatcher dispatcher)
        => (_engine, _dispatcher) = (engine, dispatcher);

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        await foreach (var alert in _engine.AlertReader.ReadAllAsync(stoppingToken))
            await _dispatcher.SendAlertAsync(alert.ToAlert());
    }
}

public sealed class DemoDataSeeder : BackgroundService
{
    private const int FeedRatePerSecond = 1000;
    private const int FeedIntervalMs = 100;
    private const int FeedBatchSize = FeedRatePerSecond * FeedIntervalMs / 1000;

    private readonly AnomalyEngine _anomalyEngine;
    private readonly ILogger<DemoDataSeeder> _logger;
    private readonly string _apiBaseUrl;
    private readonly string _ingestUrl;
    private readonly Random _random = new();
    private readonly Dictionary<Guid, string> _sensorTypes = new();
    private readonly List<Guid> _sensorOrder = new();
    private readonly Dictionary<Guid, double> _baselineBySensor = new();
    private readonly Dictionary<Guid, double> _amplitudeBySensor = new();
    private readonly HashSet<Guid> _seededRules = new();
    private int _nextSensorIndex;

    public DemoDataSeeder(
        AnomalyEngine anomalyEngine,
        ILogger<DemoDataSeeder> logger,
        string apiBaseUrl,
        string ingestUrl)
    {
        _anomalyEngine = anomalyEngine;
        _logger = logger;
        _apiBaseUrl = apiBaseUrl.TrimEnd('/');
        _ingestUrl = ingestUrl;
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        using var httpClient = new HttpClient();
        httpClient.Timeout = TimeSpan.FromSeconds(5);

        while (!stoppingToken.IsCancellationRequested)
        {
            try
            {
                await EnsureSensorsLoadedAsync(httpClient, stoppingToken);

                if (_sensorOrder.Count == 0)
                {
                    _logger.LogWarning("Demo data seeder is idle because no sensors are available");
                    await Task.Delay(TimeSpan.FromSeconds(1), stoppingToken);
                    continue;
                }

                RegisterDemoRules();

                using var timer = new PeriodicTimer(TimeSpan.FromMilliseconds(FeedIntervalMs));
                while (await timer.WaitForNextTickAsync(stoppingToken))
                {
                    try
                    {
                        await PublishDemoBatchAsync(httpClient, stoppingToken);
                    }
                    catch (Exception ex)
                    {
                        _logger.LogWarning(ex, "Demo data seeder cycle failed");
                    }
                }
            }
            catch (Exception ex)
            {
                _logger.LogWarning(ex, "Demo data seeder cycle failed");
            }
        }
    }

    private async Task EnsureSensorsLoadedAsync(HttpClient httpClient, CancellationToken stoppingToken)
    {
        if (_sensorTypes.Count > 0) return;

        var sensors = await httpClient.GetFromJsonAsync<List<SensorDto>>($"{_apiBaseUrl}/api/sensors", cancellationToken: stoppingToken)
            ?? [];

        foreach (var sensor in sensors)
        {
            _sensorTypes[sensor.Id] = sensor.Type?.ToLowerInvariant() ?? "generic";
            _sensorOrder.Add(sensor.Id);
            _baselineBySensor[sensor.Id] = GetBaseline(sensor.Type);
            _amplitudeBySensor[sensor.Id] = GetAmplitude(sensor.Type);
        }

        _logger.LogInformation("Loaded {Count} demo sensors", _sensorTypes.Count);
    }

    private void RegisterDemoRules()
    {
        foreach (var (sensorId, sensorType) in _sensorTypes)
        {
            if (!_seededRules.Add(sensorId)) continue;

            var warningThreshold = _baselineBySensor[sensorId] + _amplitudeBySensor[sensorId] * 1.25;
            var criticalThreshold = _baselineBySensor[sensorId] + _amplitudeBySensor[sensorId] * 1.6;

            _anomalyEngine.RegisterRule(new ThresholdRule
            {
                Id = Guid.NewGuid(),
                SensorId = sensorId,
                MetricName = "avg",
                Operator = "gt",
                Threshold = warningThreshold,
                Severity = 2,
                IsEnabled = true,
            });

            _anomalyEngine.RegisterRule(new ThresholdRule
            {
                Id = Guid.NewGuid(),
                SensorId = sensorId,
                MetricName = "avg",
                Operator = "gt",
                Threshold = criticalThreshold,
                Severity = 3,
                IsEnabled = true,
            });

            _logger.LogInformation("Registered demo alert rules for {SensorId} ({SensorType})", sensorId, sensorType);
        }
    }

    private async Task PublishDemoBatchAsync(HttpClient httpClient, CancellationToken stoppingToken)
    {
        if (_sensorOrder.Count == 0) return;

        var now = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
        var batch = BuildFixedRateBatch(now);

        if (batch.Length == 0) return;

        var response = await httpClient.PostAsJsonAsync(_ingestUrl, batch, stoppingToken);
        response.EnsureSuccessStatusCode();
        _logger.LogDebug("Posted {Count} demo events at target rate {Rate} ev/s", batch.Length, FeedRatePerSecond);
    }

    private SensorEvent[] BuildFixedRateBatch(long now)
    {
        if (_sensorOrder.Count == 0) return [];

        var batch = new SensorEvent[FeedBatchSize];
        for (var i = 0; i < batch.Length; i++)
        {
            var sensorId = _sensorOrder[_nextSensorIndex];
            _nextSensorIndex = (_nextSensorIndex + 1) % _sensorOrder.Count;

            batch[i] = CreateEvent(sensorId, now + i);
        }

        return batch;
    }

    private SensorEvent CreateEvent(Guid sensorId, long timestampMs)
    {
        var baseline = _baselineBySensor[sensorId];
        var amplitude = _amplitudeBySensor[sensorId];
        var shouldSpike = _random.NextDouble() < 0.02;
        var spikeOffset = shouldSpike ? amplitude * (1.8 + _random.NextDouble()) : 0;
        var phase = (timestampMs / 1000.0) + _random.NextDouble();
        var drift = Math.Sin(phase) * amplitude;
        var noise = (_random.NextDouble() - 0.5) * amplitude * 0.2;
        var value = baseline + drift + noise + spikeOffset;

        return new SensorEvent
        {
            SensorId = sensorId,
            Value = Math.Round(value, 2),
            Timestamp = timestampMs,
            Quality = shouldSpike ? "degraded" : "good",
        };
    }

    private static double GetBaseline(string? sensorType)
        => sensorType?.ToLowerInvariant() switch
        {
            "temperature" => 24,
            "humidity" => 48,
            "pressure" => 1008,
            _ => 50,
        };

    private static double GetAmplitude(string? sensorType)
        => sensorType?.ToLowerInvariant() switch
        {
            "temperature" => 7,
            "humidity" => 18,
            "pressure" => 12,
            _ => 10,
        };

    private sealed record SensorDto(Guid Id, string Name, string Type, string? Unit, string? Location, bool IsActive, DateTimeOffset CreatedAt, DateTimeOffset UpdatedAt);
}