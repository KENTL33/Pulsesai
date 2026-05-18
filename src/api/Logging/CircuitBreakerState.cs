using Serilog;

namespace Pulses.Api.Logging;

public sealed class CircuitBreakerState
{
    private volatile string _state = "closed";
    private DateTimeOffset _openedAt = DateTimeOffset.MinValue;
    private int _successCount;

    public string State => _state;
    public bool IsOpen => _state == "open";
    public bool IsHalfOpen => _state == "half_open";

    public void TransitionTo(string newState)
    {
        var oldState = _state;
        _state = newState;

        if (newState == "open")
        {
            _openedAt = DateTimeOffset.UtcNow;
            StructuredLogging.LogCircuitOpen("SignalR", $"State changed {oldState}→{newState}");
        }
        else if (newState == "half_open")
        {
            _successCount = 0;
            StructuredLogging.LogCircuitHalfOpen("SignalR");
        }
        else
        {
            StructuredLogging.LogCircuitClosed("SignalR");
        }
    }

    public void RecordSuccess()
    {
        if (_state == "half_open")
        {
            _successCount++;
            if (_successCount >= 3) TransitionTo("closed");
        }
    }

    public void RecordFailure()
    {
        if (_state == "half_open") TransitionTo("open");
        else if (_state == "closed") TransitionTo("half_open");
    }

    public TimeSpan TimeSinceOpen => _state == "open" ? DateTimeOffset.UtcNow - _openedAt : TimeSpan.Zero;
}