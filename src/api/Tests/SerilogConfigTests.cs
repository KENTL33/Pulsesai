using Serilog;
using Serilog.Core;
using Serilog.Events;
using Xunit;

namespace Pulses.Api.Tests;

public class SerilogConfigTests
{
    [Fact]
    public void AsyncSink_IsPresent_InLoggerConfiguration()
    {
        var logger = new LoggerConfiguration()
            .WriteTo.Async(a => a.Sink(new CollectingSink()))
            .CreateLogger();

        Assert.IsType<Serilog.AsyncAsyncSink>(logger as ILogger);
    }

    [Fact]
    public void LevelSwitch_Default_IsInformation()
    {
        var levelSwitch = new LoggingLevelSwitch(LogEventLevel.Information);
        Assert.Equal(LogEventLevel.Information, levelSwitch.MinimumLevel);
    }

    [Fact]
    public void StructuredTemplate_ContainsAllRequiredFields()
    {
        var events = new List<LogEvent>();
        var sink = new DelegatingSink(e => events.Add(e));

        var logger = new LoggerConfiguration()
            .WriteTo.Sink(sink)
            .Enrich.FromLogContext()
            .CreateLogger();

        using (LogContext.PushProperty("CorrelationId", "abc123"))
        using (LogContext.PushProperty("BatchId", "def456"))
        {
            logger.Information("Test message");
        }

        Assert.Single(events);
        var evt = events[0];
        Assert.True(evt.Properties.ContainsKey("CorrelationId"));
        Assert.True(evt.Properties.ContainsKey("BatchId"));
        Assert.Equal("abc123", evt.Properties["CorrelationId"].AsScalar());
        Assert.Equal("def456", evt.Properties["BatchId"].AsScalar());
    }

    [Fact]
    public void ExceptionLog_IncludesExceptionInStructuredField()
    {
        var events = new List<LogEvent>();
        var sink = new DelegatingSink(e => events.Add(e));
        var logger = new LoggerConfiguration().WriteTo.Sink(sink).CreateLogger();

        var ex = new InvalidOperationException("db unavailable");
        logger.Error(ex,
            "Persistence failure. BatchId: {BatchId}. Count: {Count}. Error: {ErrorMessage}",
            "batch-1", 42, ex.Message);

        var evt = events[0];
        Assert.Same(ex, evt.Exception);
        Assert.Equal("batch-1", evt.Properties["BatchId"].AsScalar());
        Assert.Equal(42L, evt.Properties["Count"].AsScalar());
        Assert.Equal("db unavailable", evt.Properties["ErrorMessage"].AsScalar());
    }

    [Fact]
    public void SamplingPolicy_EmitsOnlyOneInN()
    {
        var policy = new SamplingPolicy(sampleRate: 10);
        var emitted = 0;
        for (var i = 0; i < 100; i++)
            if (policy.ShouldEmit("TestSource", LogEventLevel.Information))
                emitted++;
        Assert.Equal(10, emitted);
    }

    [Fact]
    public void CircuitBreakerState_TransitionsLoggedAtCorrectLevel()
    {
        var events = new List<LogEvent>();
        var sink = new DelegatingSink(e => events.Add(e));
        var logger = new LoggerConfiguration().WriteTo.Sink(sink).Enrich.FromLogContext().CreateLogger();
        Log.Logger = logger;

        var cb = new CircuitBreakerState();
        cb.TransitionTo("open");
        Assert.Contains(events, e => e.MessageTemplate.Text.Contains("OPENED"));
        Assert.Equal(LogEventLevel.Warning, events.Last().Level);

        events.Clear();
        cb.TransitionTo("closed");
        Assert.Contains(events, e => e.MessageTemplate.Text.Contains("CLOSED"));
        Assert.Equal(LogEventLevel.Information, events.Last().Level);
    }

    private sealed class CollectingSink : ILogEventSink
    {
        public void Emit(LogEvent logEvent) { }
    }

    private sealed class DelegatingSink : ILogEventSink
    {
        private readonly Action<LogEvent> _emit;
        public DelegatingSink(Action<LogEvent> emit) => _emit = emit;
        public void Emit(LogEvent logEvent) => _emit(logEvent);
    }
}