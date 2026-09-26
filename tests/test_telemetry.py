from datetime import datetime, timezone

import pytest
from pydantic import ValidationError

from shared.telemetry import TelemetryInput, create_event


def test_create_event_adds_ingestion_timestamp() -> None:
    telemetry = TelemetryInput(
        device_id="device-001",
        temperature=23.4,
        humidity=51.2,
        recorded_at=datetime.now(timezone.utc),
    )

    event = create_event(telemetry)

    assert event.device_id == telemetry.device_id
    assert event.ingested_at.tzinfo is not None


def test_humidity_must_be_in_valid_range() -> None:
    with pytest.raises(ValidationError):
        TelemetryInput(
            device_id="device-001",
            temperature=23.4,
            humidity=101,
            recorded_at=datetime.now(timezone.utc),
        )


def test_recorded_at_requires_timezone() -> None:
    with pytest.raises(ValidationError):
        TelemetryInput(
            device_id="device-001",
            temperature=23.4,
            humidity=51.2,
            recorded_at=datetime.now(),
        )
