using Serilog;
using Pulses.Api.Logging;
using Pulses.Api.Background;
using Pulses.Api.Data;
using Pulses.Api.Hubs;
using Pulses.Api.Services;
using Microsoft.EntityFrameworkCore;
using StackExchange.Redis;

var builder = WebApplication.CreateBuilder(args);

// Bootstrap logger (reads config before host is built)
Log.Logger = new LoggerConfiguration()
    .ReadFrom.Configuration(builder.Configuration)
    .Enrich.FromLogContext()
    .Enrich.WithProperty("Application", "Pulses.Api")
    .WriteTo.Async(a => a.Console(
        outputTemplate: "[{Timestamp:O}] [{Level:u3}] [{CorrelationId}] {SourceContext} {Message:lj}{NewLine}{Exception}"))
    .CreateBootstrapLogger();

try
{
    builder.Host.UseSerilog((ctx, _, cfg) => cfg
        .ReadFrom.Configuration(ctx.Configuration)
        .Enrich.FromLogContext()
        .Enrich.WithProperty("Application", "Pulses.Api")
        .WriteTo.Async(a => a.Console(
            outputTemplate: "[{Timestamp:O}] [{Level:u3}] [{CorrelationId}] {SourceContext} {Message:lj}{NewLine}{Exception}")));

    var useRedisBackplane = builder.Configuration.GetValue("Redis:UseBackplane", false);

    // PostgreSQL via EF Core
    builder.Services.AddDbContext<AppDbContext>(options =>
        options.UseNpgsql(builder.Configuration.GetConnectionString("PostgreSQL")));

    if (useRedisBackplane)
    {
        builder.Services.AddSingleton<IConnectionMultiplexer>(_ =>
        {
            var redisConn = builder.Configuration.GetConnectionString("Redis")!;
            var cfg = ConfigurationOptions.Parse(redisConn);
            cfg.AbortOnConnectFail = false;
            return ConnectionMultiplexer.Connect(cfg);
        });

        builder.Services.AddSignalR()
            .AddMessagePackProtocol()
            .AddStackExchangeRedis(builder.Configuration.GetConnectionString("Redis")!,
                options => options.Configuration.ChannelPrefix = RedisChannel.Literal("pulses"));
    }
    else
    {
        builder.Services.AddSignalR()
            .AddMessagePackProtocol();
    }

    builder.Services.AddControllers();
    builder.Services.AddEndpointsApiExplorer();
    builder.Services.AddSwaggerGen();
    builder.Services.AddCors(options => options.AddDefaultPolicy(policy => policy
        .SetIsOriginAllowed(_ => true)
        .AllowCredentials()
        .AllowAnyHeader()
        .AllowAnyMethod()));

    // Register services
    builder.Services.AddScoped<SensorService>();
    builder.Services.AddScoped<AlertService>();
    builder.Services.AddScoped<MetricsService>();
    builder.Services.AddHostedService<MetricsFlushService>();
    builder.Services.AddHostedService<MetricsRetentionWorker>();

    var app = builder.Build();

    // Startup log
    StartupShutdownLogger.LogStartup(
        Log.Logger,
        builder.Configuration,
        "Pulses.Api");

    // Per-request logging (duration, status, CorrelationId)
    app.UseSerilogRequestLogging(options =>
    {
        options.MessageTemplate = "HTTP {RequestMethod} {RequestPath} responded {StatusCode} in {Elapsed:0.0000}ms";
        options.EnrichDiagnosticContext = (diagnosticContext, httpContext) =>
        {
            diagnosticContext.Set("CorrelationId", httpContext.Items["CorrelationId"]?.ToString());
        };
    });

    // Correlation ID middleware (generate/pass X-Correlation-Id)
    app.UseMiddleware<CorrelationMiddleware>();

    // Full request/response logging for every API call
    app.UseMiddleware<RequestResponseLoggingMiddleware>();

    app.UseSwagger();
    app.UseSwaggerUI();
    app.UseCors();

    app.MapControllers();
    app.MapHub<AnalyticsHub>("/hubs/analytics");

    // Liveness — process alive; no external dependency check
    app.MapGet("/health/live", () => Results.Ok(new { status = "live", timestamp = DateTimeOffset.UtcNow }));

    // Readiness — fully operational; checks PostgreSQL + Redis
    app.MapGet("/health/ready", async (AppDbContext db, IServiceProvider services) =>
    {
        try
        {
            await db.Database.CanConnectAsync();
            StructuredLogging.LogHealthCheck("db", "healthy");

            var cacheStatus = "disabled";
            var redis = services.GetService<IConnectionMultiplexer>();
            if (redis is not null)
            {
                var redisDb = redis.GetDatabase();
                await redisDb.PingAsync();
                StructuredLogging.LogHealthCheck("redis", "healthy");
                cacheStatus = "connected";
            }
            else
            {
                StructuredLogging.LogHealthCheck("redis", "skipped");
            }

            return Results.Ok(new { status = "ready", timestamp = DateTimeOffset.UtcNow, database = "connected", cache = cacheStatus });
        }
        catch (Exception ex)
        {
            StructuredLogging.LogHealthCheck("db", "degraded", ex.Message);
            return Results.Json(new { status = "not_ready", timestamp = DateTimeOffset.UtcNow, error = ex.Message }, statusCode: 503);
        }
    });

    // Graceful shutdown hook
    app.Lifetime.ApplicationStopping.Register(() =>
    {
        StartupShutdownLogger.LogShutdown(
            Log.Logger,
            "Pulses.Api");
    });

    // Unobserved task exception handler
    TaskScheduler.UnobservedTaskException += StartupShutdownLogger.LogUnobservedException;

    Log.Information("Starting Pulses.Api on port {Port}", builder.Configuration["API:Port"]);
    app.Run();
}
catch (Exception ex)
{
    Log.Fatal(ex, "Application terminated unexpectedly");
    throw;
}
finally
{
    Log.CloseAndFlush();
}