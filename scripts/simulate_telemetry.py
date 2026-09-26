import argparse
import asyncio
import random
from datetime import datetime, timezone

import httpx

from shared.config import load_settings


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Send simulated IoT telemetry")
    parser.add_argument("--devices", type=int, default=3)
    parser.add_argument("--interval", type=float, default=1.0)
    parser.add_argument(
        "--count",
        type=int,
        default=0,
        help="Messages to send; 0 runs until interrupted",
    )
    return parser.parse_args()


async def simulate(devices: int, interval: float, count: int) -> None:
    if devices < 1:
        raise ValueError("devices must be at least 1")
    if interval < 0:
        raise ValueError("interval cannot be negative")
    if count < 0:
        raise ValueError("count cannot be negative")

    ingestion_url = load_settings().ingestion_url.rstrip("/")
    sent = 0

    async with httpx.AsyncClient(timeout=10) as client:
        while count == 0 or sent < count:
            device_number = sent % devices + 1
            payload = {
                "device_id": f"device-{device_number:03d}",
                "temperature": round(random.uniform(18, 30), 2),
                "humidity": round(random.uniform(35, 70), 2),
                "recorded_at": datetime.now(timezone.utc).isoformat(),
            }
            response = await client.post(
                f"{ingestion_url}/telemetry",
                json=payload,
            )
            response.raise_for_status()
            sent += 1
            print(f"sent #{sent}: {payload}")
            await asyncio.sleep(interval)


if __name__ == "__main__":
    arguments = parse_args()
    try:
        asyncio.run(
            simulate(arguments.devices, arguments.interval, arguments.count)
        )
    except KeyboardInterrupt:
        print("simulation stopped")

