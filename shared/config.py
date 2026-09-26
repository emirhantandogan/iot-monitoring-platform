import os
from dataclasses import dataclass


@dataclass(frozen=True)
class Settings:
    kafka_bootstrap_servers: str
    kafka_telemetry_topic: str
    kafka_storage_group_id: str
    kafka_consumer_batch_size: int
    kafka_consumer_batch_wait_ms: int
    postgres_dsn: str
    ingestion_url: str
    storage_metrics_url: str
    storage_metrics_port: int
    pipeline_sample_interval_seconds: float
    pipeline_drain_timeout_seconds: float
    kubernetes_api_url: str
    kubernetes_namespace: str
    kubernetes_metrics_url: str
    postgres_pod_name_prefix: str
    storage_pod_label_selector: str


def load_settings() -> Settings:
    """Read service configuration from environment variables."""
    return Settings(
        kafka_bootstrap_servers=os.getenv(
            "KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"
        ),
        kafka_telemetry_topic=os.getenv(
            "KAFKA_TELEMETRY_TOPIC", "telemetry.raw"
        ),
        kafka_storage_group_id=os.getenv(
            "KAFKA_STORAGE_GROUP_ID", "storage-consumer"
        ),
        kafka_consumer_batch_size=int(
            os.getenv("KAFKA_CONSUMER_BATCH_SIZE", "100")
        ),
        kafka_consumer_batch_wait_ms=int(
            os.getenv("KAFKA_CONSUMER_BATCH_WAIT_MS", "100")
        ),
        postgres_dsn=os.getenv(
            "POSTGRES_DSN",
            "postgresql://iot:iot@localhost:5432/iot_monitoring",
        ),
        ingestion_url=os.getenv("INGESTION_URL", "http://localhost:8000"),
        storage_metrics_url=os.getenv(
            "STORAGE_METRICS_URL", "http://localhost:9100/metrics"
        ),
        storage_metrics_port=int(os.getenv("STORAGE_METRICS_PORT", "9100")),
        pipeline_sample_interval_seconds=float(
            os.getenv("PIPELINE_SAMPLE_INTERVAL_SECONDS", "1")
        ),
        pipeline_drain_timeout_seconds=float(
            os.getenv("PIPELINE_DRAIN_TIMEOUT_SECONDS", "60")
        ),
        kubernetes_api_url=os.getenv("KUBERNETES_API_URL", ""),
        kubernetes_namespace=os.getenv("KUBERNETES_NAMESPACE", "iot-monitoring"),
        kubernetes_metrics_url=os.getenv("KUBERNETES_METRICS_URL", ""),
        postgres_pod_name_prefix=os.getenv(
            "POSTGRES_POD_NAME_PREFIX", "postgres-"
        ),
        storage_pod_label_selector=os.getenv(
            "STORAGE_POD_LABEL_SELECTOR", "app=storage-consumer"
        ),
    )
