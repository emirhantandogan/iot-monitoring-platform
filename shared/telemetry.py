from datetime import datetime, timezone

from pydantic import BaseModel, Field, field_validator


class TelemetryInput(BaseModel):
    device_id: str = Field(min_length=1, max_length=100)
    temperature: float = Field(ge=-100, le=200)
    humidity: float = Field(ge=0, le=100)
    recorded_at: datetime

    @field_validator("recorded_at")
    @classmethod
    def recorded_at_must_have_timezone(cls, value: datetime) -> datetime:
        if value.tzinfo is None or value.utcoffset() is None:
            raise ValueError("recorded_at must include a timezone")
        return value


class TelemetryEvent(TelemetryInput):
    ingested_at: datetime

    @field_validator("ingested_at")
    @classmethod
    def ingested_at_must_have_timezone(cls, value: datetime) -> datetime:
        if value.tzinfo is None or value.utcoffset() is None:
            raise ValueError("ingested_at must include a timezone")
        return value


def create_event(telemetry: TelemetryInput) -> TelemetryEvent:
    """Add the ingestion timestamp before publishing to Kafka."""
    return TelemetryEvent(
        **telemetry.model_dump(),
        ingested_at=datetime.now(timezone.utc),
    )
