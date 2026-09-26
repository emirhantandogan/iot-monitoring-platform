import json
from contextlib import asynccontextmanager

from aiokafka import AIOKafkaProducer
from fastapi import FastAPI, Request, status

from shared.config import load_settings
from shared.telemetry import TelemetryInput, create_event


settings = load_settings()


@asynccontextmanager
async def lifespan(app: FastAPI):
    producer = AIOKafkaProducer(
        bootstrap_servers=settings.kafka_bootstrap_servers,
    )
    await producer.start()
    app.state.kafka_producer = producer
    try:
        yield
    finally:
        await producer.stop()


app = FastAPI(title="Telemetry Ingestion Service", lifespan=lifespan)


@app.get("/health")
async def health() -> dict[str, str]:
    return {"status": "ok"}


@app.post("/telemetry", status_code=status.HTTP_202_ACCEPTED)
async def ingest_telemetry(
    telemetry: TelemetryInput,
    request: Request,
) -> dict[str, str]:
    event = create_event(telemetry)
    message = json.dumps(event.model_dump(mode="json")).encode("utf-8")
    key = event.device_id.encode("utf-8")

    producer: AIOKafkaProducer = request.app.state.kafka_producer
    await producer.send_and_wait(
        settings.kafka_telemetry_topic,
        value=message,
        key=key,
    )

    return {"status": "accepted", "device_id": event.device_id}

