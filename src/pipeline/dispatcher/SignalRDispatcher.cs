using Microsoft.AspNetCore.SignalR.Client;
using Pulses.Shared;

namespace Pulses.Pipeline.Dispatcher;

public sealed class SignalRDispatcher : IAsyncDisposable
{
    private HubConnection? _hubConnection;
    private readonly string _hubUrl;
    private readonly ILogger<SignalRDispatcher> _logger;
    private static readonly TimeSpan SendTimeout = TimeSpan.FromSeconds(3);

    public SignalRDispatcher(string hubUrl, ILogger<SignalRDispatcher> logger)
    {
        _hubUrl = hubUrl;
        _logger = logger;
    }

    public async Task StartAsync(CancellationToken ct = default)
    {
        _hubConnection = new HubConnectionBuilder()
            .WithUrl(_hubUrl)
            .AddMessagePackProtocol()
            .WithAutomaticReconnect(new[] { TimeSpan.FromSeconds(1), TimeSpan.FromSeconds(3), TimeSpan.FromSeconds(5) })
            .Build();

        _hubConnection.Reconnecting += _ =>
        {
            _logger.LogWarning("SignalR reconnecting...");
            return Task.CompletedTask;
        };

        _hubConnection.Reconnected += _ =>
        {
            _logger.LogInformation("SignalR reconnected");
            return Task.CompletedTask;
        };

        await _hubConnection.StartAsync(ct);
        _logger.LogInformation("SignalR dispatcher connected to {HubUrl}", _hubUrl);
    }

    private bool IsHubConnected()
        => _hubConnection is not null && _hubConnection.State == HubConnectionState.Connected;

    public async Task SendMetricAsync(AggregatedMetric metric)
    {
        if (!IsHubConnected()) return;
        try
        {
            using var cts = new CancellationTokenSource(SendTimeout);
            await _hubConnection!.SendAsync("broadcastMetric", metric, cancellationToken: cts.Token);
        }
        catch (OperationCanceledException)
        {
            _logger.LogWarning("SignalR send timed out after {Timeout}s for metric", SendTimeout.TotalSeconds);
        }
        catch (Exception ex)
        {
            _logger.LogWarning(ex, "Failed to send metric via SignalR");
        }
    }

    public async Task SendMetricBatchAsync(IReadOnlyList<AggregatedMetric> metrics)
    {
        if (!IsHubConnected()) return;
        try
        {
            using var cts = new CancellationTokenSource(SendTimeout);
            await _hubConnection!.SendAsync("broadcastMetricBatch", metrics, cancellationToken: cts.Token);
        }
        catch (OperationCanceledException)
        {
            _logger.LogWarning("SignalR batch send timed out after {Timeout}s ({Count} metrics)",
                SendTimeout.TotalSeconds, metrics.Count);
        }
        catch (Exception ex)
        {
            _logger.LogWarning(ex, "Failed to send metric batch via SignalR");
        }
    }

    public async Task SendAlertAsync(Alert alert)
    {
        if (!IsHubConnected()) return;
        try
        {
            using var cts = new CancellationTokenSource(SendTimeout);
            await _hubConnection!.SendAsync("broadcastAlert", alert, cancellationToken: cts.Token);
        }
        catch (OperationCanceledException)
        {
            _logger.LogWarning("SignalR alert send timed out after {Timeout}s", SendTimeout.TotalSeconds);
        }
        catch (Exception ex)
        {
            _logger.LogWarning(ex, "Failed to send alert via SignalR");
        }
    }

    public bool IsConnected => IsHubConnected();

    public async ValueTask DisposeAsync()
    {
        if (_hubConnection is not null)
            await _hubConnection.DisposeAsync();
    }
}