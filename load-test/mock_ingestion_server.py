#!/usr/bin/env python3
"""
Mock Ingestion Server — mimics Pulses.Pipeline /ingest endpoint.
Used when .NET 8.0.421 runtime is unavailable.
Simulates the full pipeline behavior: accepts events, tracks metrics,
and can be queried at /ingest/metrics.
"""
import asyncio
import json
import time
import uuid
from dataclasses import dataclass, asdict, field
from datetime import datetime, timezone
from typing import Optional
from aiohttp import web

PORT = 5001
STATS = {
    "total_received": 0,
    "total_accepted": 0,
    "channel_capacity": 10000,
    "channel_size": 0,
    "batches_received": 0,
    "errors": 0,
    "start_time": datetime.now(timezone.utc).isoformat(),
    "last_batch_time": None,
    "peak_rate_10s": 0.0,
    "recent_counts": [],  # [(timestamp, count), ...]
}


@dataclass
class SensorMetric:
    sensor_id: str
    window_start: str
    avg_value: float
    min_value: float
    max_value: float
    count: int


# In-memory "pipeline" state
sensor_metrics: dict[str, list] = {}
metric_lock = asyncio.Lock()


def aggregate_metrics(events: list) -> list[SensorMetric]:
    """Aggregate events into per-sensor 1s window metrics (same as TumblingWindowAggregator)."""
    by_sensor: dict[str, list] = {}
    for evt in events:
        sid = str(evt.get("sensorId", ""))
        if sid not in by_sensor:
            by_sensor[sid] = []
        by_sensor[sid].append(evt)

    results = []
    now = int(time.time() * 1000)
    window = now - (now % 1000)

    for sid, evts in by_sensor.items():
        values = [e["value"] for e in evts if "value" in e and isinstance(e["value"], (int, float))]
        if not values:
            continue
        m = SensorMetric(
            sensor_id=sid,
            window_start=datetime.fromtimestamp(window / 1000, tz=timezone.utc).isoformat(),
            avg_value=round(sum(values) / len(values), 4),
            min_value=round(min(values), 4),
            max_value=round(max(values), 4),
            count=len(values),
        )
        results.append(m)

        # Store in rolling buffer (300 points max)
        if sid not in sensor_metrics:
            sensor_metrics[sid] = []
        sensor_metrics[sid].append(m)
        if len(sensor_metrics[sid]) > 300:
            sensor_metrics[sid] = sensor_metrics[sid][-300:]

    return results


async def ingest_handler(request: web.Request) -> web.Response:
    try:
        events = await request.json()
    except Exception:
        return web.json_response({"error": "Invalid JSON"}, status=400)

    if not events or not isinstance(events, list):
        return web.json_response({"error": "Empty batch"}, status=400)

    global STATS
    STATS["total_received"] += len(events)
    STATS["batches_received"] += 1
    STATS["last_batch_time"] = datetime.now(timezone.utc).isoformat()

    now = time.time()
    STATS["recent_counts"].append((now, len(events)))
    STATS["recent_counts"] = [(t, c) for t, c in STATS["recent_counts"] if now - t < 10]
    window_count = sum(c for _, c in STATS["recent_counts"])
    STATS["peak_rate_10s"] = window_count / 10.0

    accepted = len(events)

    # Simulate processing delay (aggregation + anomaly check)
    # ~1-2ms per batch to mirror real pipeline
    await asyncio.sleep(0.002)

    # Aggregate and store
    aggregated = aggregate_metrics(events)
    STATS["total_accepted"] += accepted

    return web.json_response({
        "accepted": accepted,
        "batchId": str(uuid.uuid4()),
        "serverTime": int(time.time() * 1000),
        "metricsInBatch": len(aggregated),
    })


async def metrics_handler(request: web.Request) -> web.Response:
    global STATS
    return web.json_response({
        "totalReceived": STATS["total_received"],
        "totalAccepted": STATS["total_accepted"],
        "batchesReceived": STATS["batches_received"],
        "errors": STATS["errors"],
        "channelCapacity": STATS["channel_capacity"],
        "peakRate10s": round(STATS["peak_rate_10s"], 1),
        "startTime": STATS["start_time"],
        "lastBatchTime": STATS["last_batch_time"],
        "sensorCount": len(sensor_metrics),
    })


async def health_handler(request: web.Request) -> web.Response:
    return web.json_response({
        "status": "ok",
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "uptime_s": round(time.time() - time.mktime(
            datetime.fromisoformat(STATS["start_time"].replace("Z", "+00:00")).timetuple()
        ), 1),
    })


async def sensors_handler(request: web.Request) -> web.Response:
    """List all sensors with latest metrics."""
    result = []
    for sid, metrics in sensor_metrics.items():
        if metrics:
            latest = metrics[-1]
            result.append({
                "id": sid,
                "latest_avg": latest.avg_value,
                "latest_min": latest.min_value,
                "latest_max": latest.max_value,
                "count": len(metrics),
            })
    return web.json_response(result)


async def start_server():
    app = web.Application()
    app.router.add_post("/ingest", ingest_handler)
    app.router.add_get("/ingest/metrics", metrics_handler)
    app.router.add_get("/health", health_handler)
    app.router.add_get("/sensors", sensors_handler)

    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, "0.0.0.0", PORT)
    await site.start()
    print(f"Mock Ingestion Server running at http://localhost:{PORT}")
    print(f"  POST /ingest  — receive sensor events")
    print(f"  GET  /ingest/metrics — server metrics")
    print(f"  GET  /health  — health check")
    print(f"  GET  /sensors  — sensor list with latest values")
    print()
    return runner


async def main():
    runner = await start_server()
    try:
        await asyncio.Event().wait()
    finally:
        await runner.cleanup()


if __name__ == "__main__":
    print("=" * 60)
    print("  Pulses Mock Ingestion Server (Python)")
    print("  Simulates: POST /ingest, GET /ingest/metrics")
    print("  NOTE: Run the real pipeline for accurate results.")
    print("=" * 60)
    asyncio.run(main())