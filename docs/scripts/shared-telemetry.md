# Shared telemetry model

## What it does

`shared/telemetry.py` defines the telemetry data contract shared by the
ingestion service and storage consumer.

## Data flow

FastAPI converts incoming JSON into `TelemetryInput`. The model checks the
device ID, temperature, humidity, and device timestamp. The ingestion service
passes the validated model to `create_event()`, which adds a UTC
`ingested_at` timestamp and returns `TelemetryEvent`. Kafka carries that event,
and the storage consumer validates it again before writing it to PostgreSQL.

## Important parts

- `TelemetryInput` accepts temperatures from -100 to 200, humidity from 0 to
  100, a non-empty device ID, and a timezone-aware `recorded_at` value.
- `TelemetryEvent` extends the input with a timezone-aware `ingested_at` value.
- `create_event()` adds the current server time in UTC.

## Dependencies

The module depends on Pydantic and Python's datetime module.

## How to run it

It is a shared library, not a standalone program. It is used automatically by
the ingestion and storage-consumer services. Its validation is covered by the
containerized test suite:

```powershell
docker build --target test -t iot-monitoring-test:local .
docker run --rm iot-monitoring-test:local
```

## What to understand

Using the same small model on both sides of Kafka prevents the producer and
consumer from silently interpreting a telemetry message differently, without
adding a separate schema service.
