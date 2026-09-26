# Compare benchmarks

## What it does

`scripts/compare_benchmarks.py` reads one or more load-test JSON files and
prints their inputs and HTTP results in one table. When several files are
provided, the first result is treated as the baseline.

## Data flow

The script loads each JSON file, reads its concurrency, request count,
throughput, latency percentiles, and error rate, and prints a comparison. It
also calculates the throughput and p95 latency change from the first result.
It does not contact the running platform or change any result file.

## Important functions

- `load_result()` reads and parses one JSON result.
- `percent_change()` calculates the percentage difference from the baseline.
- `main()` loads the requested files and prints the table and comparisons.

## Dependencies

It uses only the Python standard library. The input files must use the JSON
format produced by `scripts/load_test.py` or the load-test dashboard.

## How to run it

Run it through Docker Compose and pass at least one result file:

```powershell
docker compose --profile tools run --rm benchmark-analysis results/baseline.json results/concurrency-50.json
```

## What to understand

This script makes results easier to compare, but it does not prove why a value
changed. Throughput, latency, errors, Kafka lag, and test conditions must be
considered together.
