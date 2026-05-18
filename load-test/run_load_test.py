#!/usr/bin/env python3
"""
Pulses Platform — Load Test Runner
===================================
Reproducible load test script for the Pulses analytics platform.
Install: pip install aiohttp
Usage:
  python3 run_load_test.py baseline           # 1000 ev/s, 5-min steady
  python3 run_load_test.py burst               # 2000 ev/s burst test
  python3 run_load_test.py mixed              # 50 sensors, mixed distribution
  python3 run_load_test.py persistence         # DB persistence pressure test
"""

import argparse
import asyncio
import json
import math
import random
import sys
import time
import uuid
from dataclasses import dataclass, asdict, field
from datetime import datetime, timezone
from typing import Optional

try:
    import aiohttp
except ImportError:
    print("ERROR: aiohttp required. Install: pip install aiohttp")
    sys.exit(1)


# ─── Configuration ──────────────────────────────────────────────────────────────

SENSOR_TYPES = {
    "temperature": {"baseline": 24, "amplitude": 7,  "unit": "°C",  "weight": 0.70},
    "humidity":    {"baseline": 48, "amplitude": 18, "unit": "%",   "weight": 0.20},
    "pressure":    {"baseline": 1008, "amplitude": 12, "unit": "hPa", "weight": 0.10},
}

@dataclass
class TestConfig:
    name: str
    target_rate: int          # events/sec
    steady_duration: int      # seconds
    ramp_up: int = 30
    ramp_down: int = 30
    batch_size: int = 100
    num_sensors: int = 30
    burst_rate: int = 2000
    burst_duration: int = 30
    api_base: str = "http://localhost:5001"
    persist_pressure: bool = False


PROFILES = {
    "baseline": TestConfig(
        name="Baseline Ramp",
        target_rate=1000,
        steady_duration=300,
        ramp_up=30,
        ramp_down=30,
        batch_size=100,
    ),
    "burst": TestConfig(
        name="Burst Test",
        target_rate=1000,
        steady_duration=180,
        ramp_up=10,
        ramp_down=10,
        batch_size=100,
        burst_rate=2000,
        burst_duration=30,
    ),
    "mixed": TestConfig(
        name="Mixed Sensors",
        target_rate=1000,
        steady_duration=300,
        ramp_up=30,
        ramp_down=30,
        batch_size=100,
        num_sensors=50,
    ),
    "persistence": TestConfig(
        name="Persistence Pressure",
        target_rate=800,
        steady_duration=300,
        ramp_up=30,
        ramp_down=30,
        batch_size=100,
        persist_pressure=True,
    ),
}


# ─── Sensor Definitions ────────────────────────────────────────────────────────

def build_sensors(count: int) -> list[dict]:
    sensors = []
    type_list = []
    for stype, info in SENSOR_TYPES.items():
        type_list.extend([stype] * int(info["weight"] * 1000))

    for i in range(count):
        stype = type_list[i % len(type_list)] if type_list else "temperature"
        info = SENSOR_TYPES[stype]
        sensors.append({
            "id": str(uuid.uuid4()),
            "type": stype,
            "baseline": info["baseline"],
            "amplitude": info["amplitude"],
            "unit": info["unit"],
            "phase": random.uniform(0, 2 * math.pi),
        })
    return sensors


def build_event_batch(sensors: list[dict], batch_size: int) -> list[dict]:
    timestamp_ms = int(time.time() * 1000)
    batch = []
    for _ in range(batch_size):
        sensor = random.choice(sensors)
        batch.append(generate_value(sensor, timestamp_ms))
    return batch


def generate_value(sensor: dict, timestamp_ms: int) -> dict:
    phase = sensor["phase"] + (timestamp_ms / 1000.0 / 60.0)
    drift = math.sin(phase) * sensor["amplitude"]
    noise = (random.random() - 0.5) * sensor["amplitude"] * 0.2
    should_spike = random.random() < 0.02
    spike = (random.random() * sensor["amplitude"] * 1.5) if should_spike else 0
    value = sensor["baseline"] + drift + noise + spike
    return {
        "sensorId": sensor["id"],
        "value": round(value, 2),
        "timestamp": timestamp_ms - random.randint(0, 500),
        "quality": "degraded" if should_spike else "good",
    }


# ─── Metrics Collection ────────────────────────────────────────────────────────

@dataclass
class RequestResult:
    timestamp: float
    batch_id: str
    events_in_batch: int
    accepted: int
    errors: int
    latency_ms: float
    success: bool
    error_msg: Optional[str] = None


@dataclass
class PhaseResult:
    phase: str
    events_sent: int
    batches_sent: int
    duration_s: float
    avg_rate_ev_s: float


def summarize_throughput(phases: list[PhaseResult]) -> dict:
    total_phase_duration = sum(p.duration_s for p in phases)
    total_phase_events = sum(p.events_sent for p in phases)
    steady_phase = next((p for p in phases if p.phase == "steady"), None)
    overall_rate = round(total_phase_events / max(total_phase_duration, 0.001), 1)
    steady_rate = steady_phase.avg_rate_ev_s if steady_phase else overall_rate
    return {
        "overall_rate_ev_s": overall_rate,
        "steady_rate_ev_s": steady_rate,
    }


@dataclass
class LoadTestReport:
    name: str
    start_time: str
    end_time: str
    config: dict
    phases: list[dict]
    summary: dict
    raw_results: list[dict] = field(default_factory=list)
    environment: dict = field(default_factory=dict)


class MetricsCollector:
    def __init__(self):
        self.results: list[RequestResult] = []

    def record(self, result: RequestResult):
        self.results.append(result)

    def to_json(self) -> list[dict]:
        return [asdict(r) for r in self.results]

    def summary(self) -> dict:
        if not self.results:
            return {}
        latencies = sorted([r.latency_ms for r in self.results])
        total_events = sum(r.events_in_batch for r in self.results)
        total_accepted = sum(r.accepted for r in self.results)
        total_errors = sum(r.errors for r in self.results)
        success_count = sum(1 for r in self.results if r.success)

        p50 = latencies[int(len(latencies) * 0.50)]
        p90 = latencies[int(len(latencies) * 0.90)]
        p95 = latencies[int(len(latencies) * 0.95)]
        p99 = latencies[int(len(latencies) * 0.99)]
        avg_lat = sum(latencies) / len(latencies) if latencies else 0

        return {
            "total_requests": len(self.results),
            "success_count": success_count,
            "error_count": len(self.results) - success_count,
            "total_events_sent": total_events,
            "total_events_accepted": total_accepted,
            "total_events_dropped": total_events - total_accepted,
            "drop_rate_pct": round((total_events - total_accepted) / max(total_events, 1) * 100, 2),
            "avg_latency_ms": round(avg_lat, 2),
            "p50_latency_ms": round(p50, 2),
            "p90_latency_ms": round(p90, 2),
            "p95_latency_ms": round(p95, 2),
            "p99_latency_ms": round(p99, 2),
            "max_latency_ms": round(max(latencies), 2),
            "min_latency_ms": round(min(latencies), 2),
        }


# ─── Rate Controller ────────────────────────────────────────────────────────────

class TokenBucketRateController:
    def __init__(self, target_rate: int, batch_size: int):
        self.target_rate = target_rate
        self.batch_size = batch_size
        self.interval_sec = batch_size / target_rate
        self.last_tick = time.monotonic()
        self.lock = asyncio.Lock()

    async def wait_next(self):
        async with self.lock:
            now = time.monotonic()
            elapsed = now - self.last_tick
            sleep_time = max(0, self.interval_sec - elapsed)
            if sleep_time > 0:
                await asyncio.sleep(sleep_time)
            self.last_tick = time.monotonic()


# ─── Load Test Core ────────────────────────────────────────────────────────────

async def run_phase(
    session: aiohttp.ClientSession,
    url: str,
    sensors: list[dict],
    target_rate: int,
    duration: int,
    collector: MetricsCollector,
    phase_name: str,
    batch_size: int = 50,
) -> PhaseResult:
    print(f"\n  [{phase_name}] target={target_rate} ev/s, duration={duration}s, batch={batch_size}", flush=True)

    controller = TokenBucketRateController(target_rate, batch_size)
    start_time = time.monotonic()
    events_sent = 0
    batches_sent = 0

    while time.monotonic() - start_time < duration:
        batch = build_event_batch(sensors, batch_size)
        batch_id = str(uuid.uuid4())
        request_start = time.time()

        try:
            async with session.post(url, json=batch, timeout=aiohttp.ClientTimeout(total=5)) as resp:
                response = await resp.json()
                latency_ms = (time.time() - request_start) * 1000
                accepted = response.get("accepted", 0)
                result = RequestResult(
                    timestamp=request_start,
                    batch_id=batch_id,
                    events_in_batch=len(batch),
                    accepted=accepted,
                    errors=len(batch) - accepted,
                    latency_ms=latency_ms,
                    success=True,
                )
        except Exception as exc:
            latency_ms = (time.time() - request_start) * 1000
            result = RequestResult(
                timestamp=request_start,
                batch_id=batch_id,
                events_in_batch=len(batch),
                accepted=0,
                errors=len(batch),
                latency_ms=latency_ms,
                success=False,
                error_msg=str(exc),
            )

        collector.record(result)
        events_sent += len(batch)
        batches_sent += 1

        elapsed = time.monotonic() - start_time
        actual_rate = events_sent / max(elapsed, 1)
        ok_str = "OK" if result.success else "ERR"
        print(f"    t+{elapsed:.0f}s | sent={events_sent} | rate={actual_rate:.0f} ev/s | lat={result.latency_ms:.1f}ms | {ok_str}", end="\r", flush=True)
        await controller.wait_next()

    actual_elapsed = time.monotonic() - start_time
    actual_rate = events_sent / max(actual_elapsed, 1)
    print(f"\n  [{phase_name}] done: {events_sent} events, {batches_sent} batches, {actual_rate:.0f} ev/s")

    return PhaseResult(
        phase=phase_name,
        events_sent=events_sent,
        batches_sent=batches_sent,
        duration_s=round(actual_elapsed, 1),
        avg_rate_ev_s=round(actual_rate, 1),
    )


async def run_load_test(config: TestConfig, output_dir: str = ".", profile_name: str = "run") -> LoadTestReport:
    run_started_at = datetime.now(timezone.utc).isoformat()

    print(f"\n{'='*70}")
    print(f"  Load Test: {config.name}")
    print(f"  Target: {config.target_rate} ev/s | Steady: {config.steady_duration}s")
    print(f"  Sensors: {config.num_sensors} | Batch: {config.batch_size}")
    print(f"{'='*70}")

    sensors = build_sensors(config.num_sensors)
    url = f"{config.api_base}/ingest"
    collector = MetricsCollector()
    phases: list[PhaseResult] = []

    # Capture environment info
    import platform, os
    env_info = {
        "platform": platform.platform(),
        "python_version": platform.python_version(),
        "cpu_count": os.cpu_count(),
    }

    connector = aiohttp.TCPConnector(limit=100, limit_per_host=100)
    timeout = aiohttp.ClientTimeout(total=10)
    async with aiohttp.ClientSession(connector=connector, timeout=timeout) as session:
        try:
            async with session.get(f"{config.api_base}/health", timeout=aiohttp.ClientTimeout(total=3)) as resp:
                print(f"  Pipeline health: HTTP {resp.status}")
        except Exception as e:
            print(f"  WARNING: Pipeline not reachable at {config.api_base}: {e}")
            print("  Load test will continue — verify pipeline is running first.")

        # Phase 1: Ramp up
        if config.ramp_up > 0:
            phase = await run_phase(
                session, url, sensors,
                target_rate=config.target_rate,
                duration=config.ramp_up,
                collector=collector,
                phase_name="ramp_up",
                batch_size=config.batch_size,
            )
            phases.append(phase)

        # Phase 2: Steady state
        phase = await run_phase(
            session, url, sensors,
            target_rate=config.target_rate,
            duration=config.steady_duration,
            collector=collector,
            phase_name="steady",
            batch_size=config.batch_size,
        )
        phases.append(phase)

        # Phase 3: Burst
        if config.burst_rate > config.target_rate and config.burst_duration > 0:
            phase = await run_phase(
                session, url, sensors,
                target_rate=config.burst_rate,
                duration=config.burst_duration,
                collector=collector,
                phase_name="burst",
                batch_size=config.batch_size,
            )
            phases.append(phase)

        # Phase 4: Ramp down
        if config.ramp_down > 0:
            phase = await run_phase(
                session, url, sensors,
                target_rate=config.target_rate // 4,
                duration=config.ramp_down,
                collector=collector,
                phase_name="ramp_down",
                batch_size=config.batch_size,
            )
            phases.append(phase)

    summary = collector.summary()
    summary.update(summarize_throughput(phases))

    report = LoadTestReport(
        name=config.name,
        start_time=run_started_at,
        end_time=datetime.now(timezone.utc).isoformat(),
        config=asdict(config),
        phases=[asdict(p) for p in phases],
        summary=summary,
        raw_results=collector.to_json(),
        environment=env_info,
    )

    # Save results
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    base = f"{output_dir}/load_test_{profile_name}_{ts}"

    with open(f"{base}.json", "w") as f:
        json.dump(asdict(report), f, indent=2)

    with open(f"{base}.csv", "w") as f:
        if report.raw_results:
            headers = list(report.raw_results[0].keys())
            f.write(",".join(headers) + "\n")
            for r in report.raw_results:
                f.write(",".join(str(r[k]) for k in headers) + "\n")

    # Print summary
    print(f"\n{'='*70}")
    print(f"  RESULTS: {report.name}")
    print(f"{'='*70}")
    s = report.summary
    print(f"  Total requests   : {s.get('total_requests', 'N/A')}")
    print(f"  Success rate    : {s.get('success_count', 'N/A')}/{s.get('total_requests', 'N/A')}")
    print(f"  Events sent     : {s.get('total_events_sent', 'N/A')}")
    print(f"  Events accepted : {s.get('total_events_accepted', 'N/A')}")
    print(f"  Drop rate       : {s.get('drop_rate_pct', 'N/A')}%")
    print(f"  Avg latency     : {s.get('avg_latency_ms', 'N/A')}ms")
    print(f"  p50 latency     : {s.get('p50_latency_ms', 'N/A')}ms")
    print(f"  p90 latency     : {s.get('p90_latency_ms', 'N/A')}ms")
    print(f"  p95 latency     : {s.get('p95_latency_ms', 'N/A')}ms")
    print(f"  p99 latency     : {s.get('p99_latency_ms', 'N/A')}ms")
    print(f"  Overall rate    : {s.get('overall_rate_ev_s', 'N/A')} ev/s")
    print(f"  Steady rate     : {s.get('steady_rate_ev_s', 'N/A')} ev/s")
    print(f"\n  Files: {base}.json  {base}.csv")

    # Success criteria check
    print(f"\n  SUCCESS CRITERIA CHECK:")
    rate_ok = s.get('steady_rate_ev_s', 0) >= 1000
    drop_ok = s.get('drop_rate_pct', 100) <= 5
    lat_ok = s.get('p95_latency_ms', 9999) < 500
    print(f"    ≥1000 ev/s     : {'PASS' if rate_ok else 'FAIL'}")
    print(f"    ≤5% drop rate  : {'PASS' if drop_ok else 'FAIL'}")
    print(f"    p95 lat <500ms : {'PASS' if lat_ok else 'FAIL'}")

    return report


# ─── Main ──────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Pulses Load Test Runner")
    parser.add_argument("profile", choices=list(PROFILES.keys()), default="baseline", nargs="?")
    parser.add_argument("--batch-size", type=int, default=100)
    parser.add_argument("--steady-duration", type=int, default=None,
                        help="Override steady phase duration in seconds")
    parser.add_argument("--sensors", type=int, default=30)
    parser.add_argument("--api", default="http://localhost:5001")
    parser.add_argument("--output-dir", default=".")
    parser.add_argument("--target-rate", type=int, default=None,
                        help="Override target event rate (events/sec)")
    args = parser.parse_args()

    config = PROFILES[args.profile]
    config.api_base = args.api
    config.batch_size = args.batch_size
    config.num_sensors = args.sensors
    if args.steady_duration is not None:
        config.steady_duration = args.steady_duration
    if args.target_rate is not None:
        config.target_rate = args.target_rate

    asyncio.run(run_load_test(config, args.output_dir, args.profile))