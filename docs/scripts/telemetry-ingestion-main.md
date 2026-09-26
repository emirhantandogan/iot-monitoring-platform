# Telemetry ingestion main

## What it does

`services/telemetry_ingestion/main.py` provides the FastAPI service that
accepts telemetry and publishes it to Kafka.

## Data flow

`POST /telemetry` converts the request JSON into `TelemetryInput`. After
validation, `create_event()` adds the server's UTC ingestion timestamp. The
service serializes the event as JSON and publishes it to `telemetry.raw` with
`device_id` as the Kafka message key. Kafka can therefore keep messages from
the same device in order within one partition.

The endpoint returns HTTP `202 Accepted` only after `send_and_wait()` receives
Kafka's acknowledgement. It does not wait for the storage consumer or
PostgreSQL.

## Important functions

- `lifespan()` starts one reusable Kafka producer with the application and
  closes it during shutdown.
- `health()` provides `GET /health` for Docker and Kubernetes health checks.
- `ingest_telemetry()` validates, converts, serializes, and publishes one
  telemetry event.

## Dependencies

The service depends on FastAPI, Uvicorn, aiokafka, and the shared configuration
and telemetry modules. Kafka and the configured telemetry topic must be
available before messages can be accepted.

## How to run it

After Kafka and the topic are ready, run it through Docker Compose:

```powershell
docker compose up -d ingestion
```

For direct development with dependencies installed:

```powershell
uvicorn services.telemetry_ingestion.main:app --reload --port 8000
```

The health endpoint is available at `http://localhost:8000/health` in the
Docker Compose environment.

## What to understand

HTTP `202 Accepted` means Kafka acknowledged the event. It does not mean the
storage consumer has processed it or PostgreSQL has stored it.
