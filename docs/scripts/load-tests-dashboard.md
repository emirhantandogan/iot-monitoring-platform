# Load-test dashboard

## What it does

`dashboard/load_tests.py` is a Streamlit page for starting named load tests and
comparing saved results. The form accepts request count, concurrency, device
count, request timeout, and a readable test name.

## Data flow

The script converts the name into a safe run ID, adds a UTC timestamp, and
calls `run_load_test_with_pipeline_metrics()`. That function runs the HTTP test
while collecting pipeline measurements. The completed result is saved as a
separate JSON file in `LOAD_TEST_RESULTS_DIR`. The saved-results tab reloads
those files and renders summary tables, charts, detailed metrics, and raw JSON.

Older JSON files remain usable. Metrics that were not recorded in an older run
are shown as unavailable instead of causing the page to fail.

## Important functions

- `safe_test_name()` creates a short value that is safe for a run ID and file
  name.
- `load_saved_results()` reads valid JSON results and reports unreadable files.
- `summary_row()` converts one complete result into a comparison-table row.
- `show_result_metrics()` displays the HTTP summary.
- `show_pipeline_metrics()` displays Kafka, consumer, PostgreSQL, and pod CPU
  summaries and time-series charts.

## Dependencies

The page depends on Streamlit, `dashboard/pipeline_metrics.py`,
`scripts/load_test.py`, a writable results directory, and the configured Kafka,
PostgreSQL, ingestion, and storage-consumer endpoints. Kubernetes pod CPU also
requires Metrics Server and the included read-only permissions.

## How to run it

Run it through Docker Compose:

```powershell
docker compose up -d load-test-dashboard
```

Open `http://localhost:8502`. In Kubernetes, port-forward the Service:

```powershell
kubectl port-forward -n iot-monitoring service/load-test-dashboard 8502:8502
```

## What to understand

The dashboard keeps the load inputs and pipeline outputs together in one saved
result. This makes controlled comparisons easier. Per-pod CPU is available in
Kubernetes when Metrics Server works; it is unavailable in Docker Compose.
