import asyncio
import logging
import sys
import time

import psycopg
from aiokafka import AIOKafkaConsumer
from prometheus_client import Counter, Histogram, start_http_server
from pydantic import ValidationError

from shared.config import load_settings
from shared.telemetry import TelemetryEvent


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
)
logger = logging.getLogger(__name__)

RECORDS_PROCESSED = Counter(
    "iot_storage_records_processed",
    "Valid telemetry records committed to PostgreSQL",
)
BATCHES_PROCESSED = Counter(
    "iot_storage_batches_processed",
    "Storage-consumer batches committed successfully",
)
BATCH_SIZE = Histogram(
    "iot_storage_batch_size",
    "Valid telemetry records in a committed batch",
    buckets=(0, 1, 5, 10, 25, 50, 100, 250),
)
BATCH_PROCESSING_SECONDS = Histogram(
    "iot_storage_batch_processing_seconds",
    "Time from receiving a batch through committing Kafka offsets",
    buckets=(0.001, 0.0025, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5),
)
POSTGRES_TRANSACTION_SECONDS = Histogram(
    "iot_storage_postgres_transaction_seconds",
    "Time for the PostgreSQL batch insert and transaction commit",
    buckets=(0.001, 0.0025, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5),
)
KAFKA_COMMIT_SECONDS = Histogram(
    "iot_storage_kafka_commit_seconds",
    "Time for the Kafka consumer offset commit",
    buckets=(0.001, 0.0025, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5),
)

INSERT_TELEMETRY = """
    INSERT INTO telemetry (
        device_id,
        temperature,
        humidity,
        recorded_at,
        ingested_at
    )
    VALUES (%s, %s, %s, %s, %s)
"""


async def run_consumer() -> None:
    settings = load_settings()
    start_http_server(settings.storage_metrics_port)
    logger.info(
        "Storage-consumer metrics available on port %s",
        settings.storage_metrics_port,
    )
    consumer = AIOKafkaConsumer(
        settings.kafka_telemetry_topic,
        bootstrap_servers=settings.kafka_bootstrap_servers,
        group_id=settings.kafka_storage_group_id,
        enable_auto_commit=False,
        auto_offset_reset="earliest",
    )
    await consumer.start()
    logger.info("Storage consumer started")
    try:
        connection = await psycopg.AsyncConnection.connect(settings.postgres_dsn)
        try:
            while True:
                records = await consumer.getmany(
                    timeout_ms=settings.kafka_consumer_batch_wait_ms,
                    max_records=settings.kafka_consumer_batch_size,
                )
                messages = [
                    message
                    for partition_messages in records.values()
                    for message in partition_messages
                ]
                if not messages:
                    continue

                batch_started = time.perf_counter()
                rows = []
                for message in messages:
                    try:
                        event = TelemetryEvent.model_validate_json(message.value)
                    except ValidationError as error:
                        logger.error(
                            "Skipping invalid message at partition=%s offset=%s: %s",
                            message.partition,
                            message.offset,
                            error,
                        )
                        continue

                    rows.append(
                        (
                            event.device_id,
                            event.temperature,
                            event.humidity,
                            event.recorded_at,
                            event.ingested_at,
                        )
                    )

                if rows:
                    postgres_started = time.perf_counter()
                    async with connection.cursor() as cursor:
                        await cursor.executemany(INSERT_TELEMETRY, rows)
                    await connection.commit()
                    POSTGRES_TRANSACTION_SECONDS.observe(
                        time.perf_counter() - postgres_started
                    )

                kafka_commit_started = time.perf_counter()
                await consumer.commit()
                KAFKA_COMMIT_SECONDS.observe(
                    time.perf_counter() - kafka_commit_started
                )
                RECORDS_PROCESSED.inc(len(rows))
                BATCHES_PROCESSED.inc()
                BATCH_SIZE.observe(len(rows))
                BATCH_PROCESSING_SECONDS.observe(
                    time.perf_counter() - batch_started
                )
                logger.info("Stored telemetry batch with %s valid events", len(rows))
        finally:
            await connection.close()
    finally:
        await consumer.stop()


if __name__ == "__main__":
    if sys.platform == "win32":
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    asyncio.run(run_consumer())
