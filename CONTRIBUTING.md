# Contributing to Pulsesai

## Development Setup

### Prerequisites

- .NET 8 SDK
- Node.js 20+
- Docker Desktop (for local infrastructure: PostgreSQL + Redis)

### Quick Start

1. Clone the repo
2. Start infrastructure: `docker compose up -d postgres redis`
3. Run API: `cd src/api && dotnet run`
4. Run Pipeline: `cd src/pipeline && dotnet run`
5. Run Dashboard: `cd src/client && npm install && npm run dev`

### Architecture

```
src/api/        — ASP.NET Core Web API + SignalR hub
src/pipeline/   — .NET worker ingesting sensor events
src/client/     — React + TypeScript dashboard
src/shared/     — Shared models (SensorEvent, AggregatedMetric)
```

### Testing

```bash
# .NET tests
dotnet test src/api/Pulses.Api.csproj
dotnet test src/pipeline/Pulses.Pipeline.csproj

# Load test (requires API + Pipeline running)
cd load-test && python run_load_test.py --target-rate 1000
```

### Code Standards

- C#: `dotnet format` before commit, 120-char line limit
- TypeScript: ESLint + Prettier, strict mode enabled
- Commits: conventional commits (`feat:`, `fix:`, `docs:`, `refactor:`)
- Tests: xUnit for .NET

### Pull Request Checklist

- [ ] All tests pass (`dotnet test`, `npm test`)
- [ ] No new compiler warnings
- [ ] Secrets checked — no real credentials in diff
- [ ] Docs updated if behavior changed