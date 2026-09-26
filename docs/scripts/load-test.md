# Load test

## What it does

`scripts/load_test.py` sends a fixed number of telemetry requests to the
ingestion API with configurable asynchronous concurrency. It measures HTTP
throughput, status codes, errors, and request latency.

## Data flow

The script fills an asynchronous queue with request numbers and starts the
selected number of workers. Each worker builds telemetry for a device and sends
it to `POST /telemetry` with a shared HTTP client. A unique device prefix keeps
the rows from each run identifiable in PostgreSQL. The final measurements are
printed and can also be saved to JSON.

## Important functions

- `run_load_test()` creates the queue and workers, sends the requests, and
  returns all inputs and measurements as a dictionary.
- `percentile()` calculates nearest-rank p50, p95, and p99 latency.
- `save_result()` writes one result to a JSON file and creates its parent
  directory when needed.
- `main()` validates command-line inputs and runs the asynchronous test.

## Dependencies

The script depends on HTTPX and `shared/config.py`. The ingestion service must
be reachable through `INGESTION_URL`. The load-test dashboard imports
`run_load_test()` instead of creating a second generator.

## How to run it

Run it through Docker Compose:

```powershell
docker compose --profile tools run --rm load-generator --requests 1000 --concurrency 10 --devices 100 --output results/baseline.json
```

The optional `--run-id` sets the device prefix. If it is omitted, the script
creates a timestamped ID.

## What to understand

This script measures the HTTP boundary. It does not by itself show whether
Kafka and PostgreSQL have finished processing the accepted messages. A result
is one measurement under specific conditions, not proof of an improvement.
