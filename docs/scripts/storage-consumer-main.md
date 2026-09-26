# Storage consumer main

## What it does

`services/storage_consumer/main.py` reads telemetry from Kafka and stores it in
PostgreSQL. It belongs to the `storage-consumer` consumer group and processes
records in configurable batches.

## Data flow

The consumer polls `telemetry.raw`, combines records returned from its assigned
partitions, and validates each value as `TelemetryEvent`. Valid rows are
inserted with one `executemany()` call and one PostgreSQL transaction. Only
after that transaction succeeds does the consumer commit the Kafka offsets.

Invalid events are logged and skipped. A database error stops the batch before
the Kafka commit, allowing Kafka to deliver those records again. If PostgreSQL
commits but the later Kafka commit fails, the records can be inserted again;
this is the small duplicate window in the simple at-least-once flow.

## Important parts

- `run_consumer()` owns the Kafka consumer, asynchronous PostgreSQL connection,
  polling loop, batch insert, and offset commit.
- `INSERT_TELEMETRY` is the explicit parameterized insert statement.
- `KAFKA_CONSUMER_BATCH_SIZE` and `KAFKA_CONSUMER_BATCH_WAIT_MS` control batch
  size and the short polling wait.
- Prometheus counters and histograms record processed records, batches, batch
  size, full batch time, PostgreSQL transaction time, and Kafka commit time.

## Dependencies

The service depends on aiokafka, Psycopg, Pydantic, prometheus-client, the
shared settings and telemetry model, a reachable Kafka topic, and the
PostgreSQL `telemetry` table.

## How to run it

After Kafka, PostgreSQL, and the topic are ready, run:

```powershell
docker compose up -d storage-consumer
```

For direct development with dependencies installed:

```powershell
python -m services.storage_consumer.main
```

The process exposes Prometheus-format metrics on port 9100. No Prometheus
server is required because the load-test dashboard reads this endpoint
directly. On Windows, the script selects the event loop required by Psycopg's
asynchronous connection.

## What to understand

Batching shares database and Kafka commit overhead across many records. The
separate latency measurements show whether database work or offset commits use
more time. Offsets are committed last so uncommitted work can be retried.
