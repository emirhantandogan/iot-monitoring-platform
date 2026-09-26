# Simulate telemetry

## What it does

`scripts/simulate_telemetry.py` acts like a small group of IoT devices. It
creates plausible temperature and humidity readings and sends them to the
ingestion API at a chosen interval.

## Data flow

The simulator cycles through device IDs, creates a UTC `recorded_at` timestamp,
and sends each JSON payload to `POST /telemetry`. It uses one asynchronous HTTP
client but sends the requests one after another. A non-success HTTP response
stops the script with an error.

## Important functions

- `parse_args()` reads device count, message interval, and optional message
  count.
- `simulate()` validates the inputs, builds each reading, sends it, and waits
  for the configured interval.

## Dependencies

The script depends on HTTPX and `shared/config.py`. The ingestion endpoint is
read from `INGESTION_URL`.

## How to run it

Send five messages through Docker Compose:

```powershell
docker compose --profile tools run --rm simulator --devices 3 --interval 0.1 --count 5
```

For direct development with the Python dependencies installed:

```powershell
python scripts/simulate_telemetry.py --devices 3 --interval 0.1 --count 5
```

Use `--count 0`, or omit `--count`, to run until `Ctrl+C` is pressed.

## What to understand

This script checks the normal end-to-end data flow. It deliberately avoids
concurrency and latency calculations; use `scripts/load_test.py` for load and
performance measurements.
