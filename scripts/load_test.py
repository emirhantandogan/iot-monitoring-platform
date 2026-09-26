import argparse
import asyncio
import json
import math
import random
import time
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path

import httpx

from shared.config import load_settings


def percentile(values: list[float], percent: int) -> float:
    """Return a nearest-rank percentile from a non-empty list."""
    ordered = sorted(values)
    index = max(0, math.ceil(percent / 100 * len(ordered)) - 1)
    return ordered[index]


def save_result(result: dict, output_path: Path) -> None:
    """Write one load-test result to its own JSON file."""
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(
        json.dumps(result, indent=2) + "\n",
        encoding="utf-8",
    )


async def run_load_test(
    request_count: int,
    concurrency: int,
    device_count: int,
    timeout_seconds: float,
    run_id: str,
) -> dict:
    settings = load_settings()
    target_url = f"{settings.ingestion_url.rstrip('/')}/telemetry"
    device_prefix = f"load-{run_id}"
    queue: asyncio.Queue[int] = asyncio.Queue()
    latencies_ms: list[float] = []
    status_codes: Counter[str] = Counter()
    exception_types: Counter[str] = Counter()
    successful_requests = 0
    failed_requests = 0

    for request_number in range(request_count):
        queue.put_nowait(request_number)

    limits = httpx.Limits(
        max_connections=concurrency,
        max_keepalive_connections=concurrency,
    )

    async with httpx.AsyncClient(
        timeout=timeout_seconds,
        limits=limits,
    ) as client:

        async def worker() -> None:
            nonlocal successful_requests, failed_requests

            while True:
                request_number = await queue.get()
                device_number = request_number % device_count + 1
                payload = {
                    "device_id": f"{device_prefix}-{device_number:04d}",
                    "temperature": round(random.uniform(18, 30), 2),
                    "humidity": round(random.uniform(35, 70), 2),
                    "recorded_at": datetime.now(timezone.utc).isoformat(),
                }
                started = time.perf_counter()

                try:
                    response = await client.post(target_url, json=payload)
                    latency_ms = (time.perf_counter() - started) * 1000
                    latencies_ms.append(latency_ms)
                    status_codes[str(response.status_code)] += 1

                    if 200 <= response.status_code < 300:
                        successful_requests += 1
                    else:
                        failed_requests += 1
                except httpx.HTTPError as error:
                    latency_ms = (time.perf_counter() - started) * 1000
                    latencies_ms.append(latency_ms)
                    exception_types[type(error).__name__] += 1
                    failed_requests += 1
                finally:
                    queue.task_done()

        workers = [asyncio.create_task(worker()) for _ in range(concurrency)]
        started_at = datetime.now(timezone.utc)
        started = time.perf_counter()
        await queue.join()
        duration_seconds = time.perf_counter() - started
        finished_at = datetime.now(timezone.utc)

        for worker_task in workers:
            worker_task.cancel()
        await asyncio.gather(*workers, return_exceptions=True)

    return {
        "run_id": run_id,
        "device_prefix": device_prefix,
        "target_url": target_url,
        "started_at": started_at.isoformat(),
        "finished_at": finished_at.isoformat(),
        "request_count": request_count,
        "successful_requests": successful_requests,
        "failed_requests": failed_requests,
        "error_rate_percent": failed_requests / request_count * 100,
        "concurrency": concurrency,
        "device_count": device_count,
        "timeout_seconds": timeout_seconds,
        "duration_seconds": duration_seconds,
        "throughput_requests_per_second": request_count / duration_seconds,
        "successful_requests_per_second": successful_requests / duration_seconds,
        "latency_ms": {
            "min": min(latencies_ms),
            "p50": percentile(latencies_ms, 50),
            "p95": percentile(latencies_ms, 95),
            "p99": percentile(latencies_ms, 99),
            "max": max(latencies_ms),
        },
        "status_codes": dict(sorted(status_codes.items())),
        "exception_types": dict(sorted(exception_types.items())),
    }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Load test telemetry ingestion")
    parser.add_argument("--requests", type=int, default=1000)
    parser.add_argument("--concurrency", type=int, default=10)
    parser.add_argument("--devices", type=int, default=100)
    parser.add_argument("--timeout", type=float, default=10)
    parser.add_argument("--run-id")
    parser.add_argument("--output", type=Path)
    return parser.parse_args()


def main() -> None:
    arguments = parse_args()
    if arguments.requests < 1:
        raise SystemExit("--requests must be at least 1")
    if arguments.concurrency < 1:
        raise SystemExit("--concurrency must be at least 1")
    if arguments.devices < 1:
        raise SystemExit("--devices must be at least 1")
    if arguments.timeout <= 0:
        raise SystemExit("--timeout must be greater than 0")

    run_id = arguments.run_id or datetime.now(timezone.utc).strftime(
        "%Y%m%dT%H%M%S%fZ"
    )
    result = asyncio.run(
        run_load_test(
            request_count=arguments.requests,
            concurrency=arguments.concurrency,
            device_count=arguments.devices,
            timeout_seconds=arguments.timeout,
            run_id=run_id,
        )
    )
    rendered_result = json.dumps(result, indent=2)
    print(rendered_result)

    if arguments.output:
        save_result(result, arguments.output)
        print(f"result_file={arguments.output}")


if __name__ == "__main__":
    main()
