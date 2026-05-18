using Serilog;

namespace Pulses.Api.Logging;

public static class StartupShutdownLogger
{
    public static void LogStartup(Serilog.ILogger logger, IConfiguration configuration, string appName)
    {
        logger.Information("═══════════════════════════════════════════════");
        logger.Information("  {AppName} starting up", appName);
        logger.Information("  Version: {Version}", typeof(StartupShutdownLogger).Assembly.GetName().Version?.ToString() ?? "unknown");
        logger.Information("  Environment: {Environment}", configuration["ASPNETCORE_ENVIRONMENT"] ?? "Production");
        logger.Information("  Host: {Host}", Environment.MachineName);
        logger.Information("  OS: {OS}", System.Runtime.InteropServices.RuntimeInformation.OSDescription);
        logger.Information("  CLR: {CLR}", System.Runtime.InteropServices.RuntimeInformation.FrameworkDescription);
        logger.Information("  PostgreSQL: {Postgres}", configuration.GetConnectionString("PostgreSQL")?.Split(';')[0] ?? "not configured");
        logger.Information("  Redis: {Redis}", configuration.GetConnectionString("Redis") ?? "not configured");
        logger.Information("  Serilog MinimumLevel: {MinLevel}",
            configuration["Serilog:MinimumLevel:Default"] ?? "Information (default)");
        logger.Information("═══════════════════════════════════════════════");
    }

    public static void LogShutdown(Serilog.ILogger logger, string appName)
    {
        logger.Information("═══════════════════════════════════════════════");
        logger.Information("  {AppName} shutting down gracefully", appName);
        logger.Information("  Timestamp: {Timestamp:O}", DateTimeOffset.UtcNow);
        logger.Information("═══════════════════════════════════════════════");
    }

    public static void LogUnobservedException(object? sender, UnobservedTaskExceptionEventArgs e)
    {
        Log.Fatal(e.Exception,
            "Unhandled exception in {AppName}. IsTerminating={IsTerminating}",
            "Pulses.Api", e.Observed == false);
        Log.CloseAndFlush();
        Environment.Exit(1);
    }
}