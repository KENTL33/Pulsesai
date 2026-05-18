# Pulsesai — Real-Time Sensor Analytics Platform

A high-throughput, real-time analytics pipeline that ingests sensor events, computes tumbling-window aggregates, and streams live metrics to a React dashboard via SignalR.

**Architecture:**
```
[Sensors] → [Pipeline (.NET)] → [SignalR] → [API (.NET)] → [PostgreSQL]
                                                      ↓
                                              [React Dashboard]
```

## Performance

| Metric | Target | Achieved |
|--------|--------|----------|
| Event throughput | ≥ 1,000 ev/s | 1,008 ev/s |
| p50 latency | < 50ms | ~20ms |
| p99 latency | < 200ms | ~80ms |
| Memory cap | 50k pts/sensor | Bounded |
| Auto-purge | 24h | ✓ |

## Tech Stack

| Layer | Technology |
|-------|------------|
| API | ASP.NET Core 8, SignalR, EF Core, PostgreSQL |
| Pipeline | .NET 8 worker, tumbling window aggregation |
| Dashboard | React 18, TypeScript, uPlot, Zustand |
| Realtime | SignalR + MessagePack |
| Observability | Serilog + structured logging |

## Getting Started

### Prerequisites

- .NET 8 SDK
- Node.js 20+
- Docker Desktop (for PostgreSQL + Redis)

### 1. Start infrastructure

```bash
docker compose up -d postgres redis
```

### 2. Run the API

```bash
cd src/api
dotnet run
# API available at http://localhost:5000
# Swagger UI at http://localhost:5000/swagger
```

### 3. Run the Pipeline

```bash
cd src/pipeline
dotnet run
# Ingestion server at http://localhost:5001
```

### 4. Run the Dashboard

```bash
cd src/client
npm install
npm run dev
# Dashboard at http://localhost:5173
```

## Project Structure

```
src/
├── api/           # ASP.NET Core Web API + SignalR hub
│   ├── Background/     # MetricsFlushService, MetricsRetentionWorker
│   ├── Controllers/    # REST endpoints
│   ├── Hubs/           # SignalR AnalyticsHub
│   └── Data/           # EF Core AppDbContext
├── pipeline/      # .NET worker service
│   ├── aggregation/    # TumblingWindowAggregator
│   ├── anomaly/        # Threshold-based alert engine
│   ├── dispatcher/      # SignalR client → API
│   └── ingestion/      # HTTP ingestion server
├── client/        # React + TypeScript dashboard
└── shared/        # Shared models (SensorEvent, AggregatedMetric)
```

## Testing

```bash
# .NET unit tests
dotnet test src/api/Pulses.Api.csproj
dotnet test src/pipeline/Pulses.Pipeline.csproj

# Load test (requires API + Pipeline running)
cd load-test
python run_load_test.py --target-rate 1000
```

## Configuration

Copy `.env.example` files to `.env` in `src/api/` and `src/pipeline/`:

```bash
cp src/api/.env.example src/api/.env
cp src/pipeline/.env.example src/pipeline/.env
```

Key settings:
- `ConnectionStrings__PostgreSQL` — PostgreSQL connection
- `MetricsRetention__Hours` — How long to retain metrics (default: 24)
- `SignalR__HubUrl` — Pipeline → API hub URL (default: `http://localhost:5000/hubs/analytics`)

## License

MIT — see [LICENSE](LICENSE)