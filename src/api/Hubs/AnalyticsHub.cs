using Microsoft.AspNetCore.SignalR;
using Pulses.Shared;

namespace Pulses.Api.Hubs;

public sealed class AnalyticsHub : Hub
{
    private readonly ILogger<AnalyticsHub> _logger;

    public AnalyticsHub(ILogger<AnalyticsHub> logger) => _logger = logger;

    [HubMethodName("subscribeSensor")]
    public async Task SubscribeSensor(Guid sensorId)
    {
        await Groups.AddToGroupAsync(Context.ConnectionId, $"sensor:{sensorId}");
        _logger.LogInformation("Connection {ConnectionId} subscribed to sensor {SensorId}", Context.ConnectionId, sensorId);
    }

    [HubMethodName("unsubscribeSensor")]
    public async Task UnsubscribeSensor(Guid sensorId)
    {
        await Groups.RemoveFromGroupAsync(Context.ConnectionId, $"sensor:{sensorId}");
    }

    [HubMethodName("subscribeAll")]
    public async Task SubscribeAll()
    {
        await Groups.AddToGroupAsync(Context.ConnectionId, "all_metrics");
    }

    public override async Task OnConnectedAsync()
    {
        _logger.LogInformation("Client connected: {ConnectionId}", Context.ConnectionId);
        await Groups.AddToGroupAsync(Context.ConnectionId, "all_metrics");
        await base.OnConnectedAsync();
    }

    public override async Task OnDisconnectedAsync(Exception? exception)
    {
        if (exception is not null)
            _logger.LogWarning(exception, "Client disconnected with error: {ConnectionId}", Context.ConnectionId);
        else
            _logger.LogInformation("Client disconnected: {ConnectionId}", Context.ConnectionId);
        await base.OnDisconnectedAsync(exception);
    }

    // Called by pipeline worker to broadcast metrics
    [HubMethodName("broadcastMetric")]
    public async Task BroadcastMetric(AggregatedMetric metric)
    {
        _logger.LogInformation("Broadcasting metric for sensor {SensorId}", metric.SensorId);
        await Clients.Group($"sensor:{metric.SensorId}").SendAsync("MetricReceived", metric);
        await Clients.Group("all_metrics").SendAsync("MetricReceived", metric);
    }

    [HubMethodName("broadcastMetricBatch")]
    public async Task BroadcastMetricBatch(IReadOnlyList<AggregatedMetric> metrics)
    {
        _logger.LogInformation("Broadcasting metric batch with {Count} metrics", metrics.Count);
        foreach (var metric in metrics)
            await Clients.Group($"sensor:{metric.SensorId}").SendAsync("MetricReceived", metric);
        await Clients.Group("all_metrics").SendAsync("MetricBatchReceived", metrics);
    }

    [HubMethodName("broadcastAlert")]
    public async Task BroadcastAlert(Alert alert)
    {
        _logger.LogInformation("Broadcasting alert for sensor {SensorId}", alert.SensorId);
        await Clients.Group($"sensor:{alert.SensorId}").SendAsync("AlertTriggered", alert);
        await Clients.Group("all_alerts").SendAsync("AlertTriggered", alert);
    }
}