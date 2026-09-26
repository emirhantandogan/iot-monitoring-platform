# Pipeline metrics

## What it does

`dashboard/pipeline_metrics.py` runs the existing HTTP load test while sampling
Kafka, the storage consumers, PostgreSQL, and optional Kubernetes pod CPU. It
continues sampling after the HTTP requests finish until Kafka lag reaches zero
or the configured drain timeout expires.

## Data flow

Each sample reads:

- Kafka end and committed offsets for input rate and consumer lag
- PostgreSQL table statistics for inserted rows per second
- every storage-consumer `/metrics` endpoint for record rate and timing
- the Kubernetes Metrics API for optional CPU usage from each pod

In Kubernetes, the collector finds all storage-consumer pods and reads each
metrics endpoint through the Kubernetes API proxy. It sums their counters
before calculating rates. In Docker Compose, it reads the single configured
`STORAGE_METRICS_URL`. The samples and final summary are added to the HTTP load
test result returned to the dashboard.

## Important functions and class

- `parse_prometheus_metrics()` and `merge_prometheus_metrics()` turn one or
  more storage metrics responses into combined counters and histograms.
- `rate()`, `histogram_average()`, and `histogram_quantile()` calculate values
  between samples.
- `cpu_to_millicores()` converts Kubernetes CPU quantities to millicores.
- `PipelineMetricsCollector` opens the external connections, collects samples,
  and builds the summary.
- `run_load_test_with_pipeline_metrics()` coordinates monitoring and the HTTP
  load test.

## Dependencies

The module uses HTTPX, Psycopg, aiokafka, the Prometheus text parser, the shared
settings, and `scripts/load_test.py`. It needs the configured Kafka and
PostgreSQL addresses. Kubernetes pod metrics additionally need Metrics Server,
the service-account token, and the permissions in
`kubernetes/load-test-dashboard-rbac.yaml`.

## How to run it

This module is not a separate service. The load-test dashboard calls it:

```powershell
docker compose up -d load-test-dashboard
```

Then open `http://localhost:8502` and start a test. The same collection runs
when the load-test dashboard is deployed in Kubernetes.

## What to understand

Compare Kafka input rate with consumer and PostgreSQL rates. If input stays
higher, the backlog grows. The separated batch, PostgreSQL transaction, and
Kafka commit timings help show where consumer time is spent. Histogram p95
values are bucket estimates, not exact individual timings. `1,000 mCPU` means
one full CPU core.
