using System.Diagnostics;
using System.Text;

namespace Pulses.Api.Logging;

public sealed class RequestResponseLoggingMiddleware
{
    private readonly RequestDelegate _next;
    private readonly ILogger<RequestResponseLoggingMiddleware> _logger;

    private static readonly HashSet<string> ExcludedPaths = new(StringComparer.OrdinalIgnoreCase)
    {
        "/health/live", "/health/ready", "/ingest/metrics", "/swagger", "/favicon.ico"
    };

    public RequestResponseLoggingMiddleware(RequestDelegate next, ILogger<RequestResponseLoggingMiddleware> logger)
        => (_next, _logger) = (next, logger);

    public async Task InvokeAsync(HttpContext context)
    {
        if (ExcludedPaths.Contains(context.Request.Path))
        {
            await _next(context);
            return;
        }

        var stopwatch = Stopwatch.StartNew();
        var correlationId = context.Items["CorrelationId"]?.ToString() ?? "unknown";

        await ReadAndLogRequestBodyAsync(context);

        Exception? caught = null;
        try
        {
            await _next(context);
        }
        catch (Exception ex)
        {
            caught = ex;
            throw;
        }
        finally
        {
            stopwatch.Stop();

            if (caught is not null)
            {
                _logger.LogError(caught,
                    "Response ERROR {Method} {Path} StatusCode={StatusCode} Duration={DurationMs}ms CorrelationId={CorrelationId} Error={Error}",
                    context.Request.Method, context.Request.Path, context.Response.StatusCode,
                    stopwatch.ElapsedMilliseconds, correlationId, caught.Message);
            }
            else if (context.Response.StatusCode >= 500)
            {
                _logger.LogWarning(
                    "Response {Method} {Path} StatusCode={StatusCode} Duration={DurationMs}ms CorrelationId={CorrelationId}",
                    context.Request.Method, context.Request.Path, context.Response.StatusCode,
                    stopwatch.ElapsedMilliseconds, correlationId);
            }
            else
            {
                _logger.LogInformation(
                    "Response {Method} {Path} StatusCode={StatusCode} Duration={DurationMs}ms CorrelationId={CorrelationId}",
                    context.Request.Method, context.Request.Path, context.Response.StatusCode,
                    stopwatch.ElapsedMilliseconds, correlationId);
            }
        }
    }

    private Task ReadAndLogRequestBodyAsync(HttpContext context)
    {
        // Only log metadata — body capture adds latency; enable if full body needed
        _logger.LogInformation(
            "Request {Method} {Path} CorrelationId={CorrelationId} ContentLength={ContentLength} QueryString={QueryString}",
            context.Request.Method,
            context.Request.Path,
            context.Items["CorrelationId"]?.ToString() ?? "unknown",
            context.Request.ContentLength ?? 0,
            context.Request.QueryString.ToString());

        return Task.CompletedTask;
    }

    private static async Task<string> ReadResponseBodyAsync(MemoryStream ms)
    {
        if (ms.Length == 0) return string.Empty;
        ms.Position = 0;
        using var reader = new StreamReader(ms, leaveOpen: true);
        var text = await reader.ReadToEndAsync();
        ms.Position = 0;
        return text.Length > 2000 ? text[..2000] + "...[truncated]" : text;
    }
}