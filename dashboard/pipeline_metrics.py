import asyncio
import time
from pathlib import Path

import httpx
import psycopg
from aiokafka import AIOKafkaConsumer, TopicPartition
from aiokafka.admin import AIOKafkaAdminClient
from prometheus_client.parser import text_string_to_metric_families

from scripts.load_test import run_load_test
from shared.config import Settings, load_settings


KUBERNETES_TOKEN = Path(
    "/var/run/secrets/kubernetes.io/serviceaccount/token"
)
KUBERNETES_CA = Path(
    "/var/run/secrets/kubernetes.io/serviceaccount/ca.crt"
)

LATENCY_HISTOGRAMS = {
    "batch_processing": "iot_storage_batch_processing_seconds",
    "postgres_transaction": "iot_storage_postgres_transaction_seconds",
    "kafka_commit": "iot_storage_kafka_commit_seconds",
}


def parse_prometheus_metrics(text: str) -> dict:
    """Return simple values and histograms from Prometheus text."""
    values: dict[str, float] = {}
    histograms: dict[str, dict] = {}

    for family in text_string_to_metric_families(text):
        for sample in family.samples:
            name = sample.name
            value = float(sample.value)

            if name.endswith("_bucket"):
                histogram_name = name.removesuffix("_bucket")
                histogram = histograms.setdefault(
                    histogram_name,
                    {"sum": 0.0, "count": 0.0, "buckets": {}},
                )
                boundary = sample.labels.get("le", "+Inf")
                histogram["buckets"][boundary] = value
            elif name.endswith("_sum"):
                histogram_name = name.removesuffix("_sum")
                histogram = histograms.setdefault(
                    histogram_name,
                    {"sum": 0.0, "count": 0.0, "buckets": {}},
                )
                histogram["sum"] = value
            elif name.endswith("_count"):
                histogram_name = name.removesuffix("_count")
                histogram = histograms.setdefault(
                    histogram_name,
                    {"sum": 0.0, "count": 0.0, "buckets": {}},
                )
                histogram["count"] = value
            elif not name.endswith("_created"):
                values[name] = value

    return {"values": values, "histograms": histograms}


def merge_prometheus_metrics(metrics: list[dict]) -> dict:
    """Sum the counters and histograms collected from multiple pods."""
    merged: dict = {"values": {}, "histograms": {}}
    for pod_metrics in metrics:
        for name, value in pod_metrics.get("values", {}).items():
            merged["values"][name] = merged["values"].get(name, 0.0) + value

        for name, histogram in pod_metrics.get("histograms", {}).items():
            merged_histogram = merged["histograms"].setdefault(
                name,
                {"sum": 0.0, "count": 0.0, "buckets": {}},
            )
            merged_histogram["sum"] += histogram.get("sum", 0.0)
            merged_histogram["count"] += histogram.get("count", 0.0)
            for boundary, value in histogram.get("buckets", {}).items():
                merged_histogram["buckets"][boundary] = (
                    merged_histogram["buckets"].get(boundary, 0.0) + value
                )
    return merged


def rate(current: float | None, previous: float | None, seconds: float) -> float | None:
    if current is None or previous is None or seconds <= 0:
        return None
    return max(0.0, (current - previous) / seconds)


def histogram_delta(start: dict, end: dict, name: str) -> dict:
    start_histogram = start.get("histograms", {}).get(name, {})
    end_histogram = end.get("histograms", {}).get(name, {})
    boundaries = set(start_histogram.get("buckets", {})) | set(
        end_histogram.get("buckets", {})
    )
    return {
        "sum": max(
            0.0,
            end_histogram.get("sum", 0.0)
            - start_histogram.get("sum", 0.0),
        ),
        "count": max(
            0.0,
            end_histogram.get("count", 0.0)
            - start_histogram.get("count", 0.0),
        ),
        "buckets": {
            boundary: max(
                0.0,
                end_histogram.get("buckets", {}).get(boundary, 0.0)
                - start_histogram.get("buckets", {}).get(boundary, 0.0),
            )
            for boundary in boundaries
        },
    }


def histogram_average(start: dict, end: dict, name: str) -> float | None:
    difference = histogram_delta(start, end, name)
    if difference["count"] <= 0:
        return None
    return difference["sum"] / difference["count"]


def histogram_quantile(
    start: dict,
    end: dict,
    name: str,
    quantile: float,
) -> float | None:
    """Estimate a quantile using the upper bound of a histogram bucket."""
    difference = histogram_delta(start, end, name)
    total = difference["count"]
    if total <= 0:
        return None

    target = total * quantile
    finite_boundaries = sorted(
        (float(boundary), boundary)
        for boundary in difference["buckets"]
        if boundary != "+Inf"
    )
    for numeric_boundary, original_boundary in finite_boundaries:
        if difference["buckets"][original_boundary] >= target:
            return numeric_boundary

    return finite_boundaries[-1][0] if finite_boundaries else None


def cpu_to_millicores(quantity: str) -> float:
    if quantity.endswith("n"):
        return float(quantity[:-1]) / 1_000_000
    if quantity.endswith("u"):
        return float(quantity[:-1]) / 1_000
    if quantity.endswith("m"):
        return float(quantity[:-1])
    return float(quantity) * 1_000


def average(values: list[float | None]) -> float | None:
    present = [value for value in values if value is not None]
    return sum(present) / len(present) if present else None


def summarize_pod_cpu(samples: list[dict]) -> list[dict]:
    """Return average and peak CPU for every pod found in the samples."""
    pod_names = sorted(
        {
            pod_name
            for sample in samples
            for pod_name in sample.get("pod_cpu_millicores", {})
        }
    )
    rows = []
    for pod_name in pod_names:
        values = [
            sample["pod_cpu_millicores"][pod_name]
            for sample in samples
            if pod_name in sample.get("pod_cpu_millicores", {})
        ]
        rows.append(
            {
                "pod": pod_name,
                "average_millicores": sum(values) / len(values),
                "peak_millicores": max(values),
            }
        )
    return rows


class PipelineMetricsCollector:
    """Collect a small time series from Kafka, PostgreSQL, and Kubernetes."""

    def __init__(self, settings: Settings):
        self.settings = settings
        self.started = time.perf_counter()
        self.kafka_consumer: AIOKafkaConsumer | None = None
        self.partitions: list[TopicPartition] = []
        self.postgres_connection: psycopg.AsyncConnection | None = None
        self.storage_client = httpx.AsyncClient(timeout=3)
        self.kubernetes_client: httpx.AsyncClient | None = None
        self.samples: list[dict] = []
        self.errors: set[str] = set()
        self.previous_state: dict | None = None
        self.first_storage_metrics: dict | None = None
        self.latest_storage_metrics: dict | None = None

    async def start(self) -> None:
        consumer: AIOKafkaConsumer | None = None
        try:
            admin = AIOKafkaAdminClient(
                bootstrap_servers=self.settings.kafka_bootstrap_servers
            )
            await admin.start()
            try:
                topics = await admin.describe_topics(
                    [self.settings.kafka_telemetry_topic]
                )
            finally:
                await admin.close()

            topic = next(
                (
                    item
                    for item in topics
                    if item["topic"] == self.settings.kafka_telemetry_topic
                ),
                None,
            )
            if topic is None or topic.get("error_code") != 0:
                raise RuntimeError("Kafka topic metadata was not found")
            self.partitions = [
                TopicPartition(
                    self.settings.kafka_telemetry_topic,
                    partition["partition"],
                )
                for partition in topic["partitions"]
            ]

            consumer = AIOKafkaConsumer(
                bootstrap_servers=self.settings.kafka_bootstrap_servers,
                group_id=self.settings.kafka_storage_group_id,
                enable_auto_commit=False,
            )
            await consumer.start()
            consumer.assign(self.partitions)
            self.kafka_consumer = consumer
        except Exception as error:
            self.errors.add(f"Kafka metrics unavailable: {error}")
            if consumer is not None:
                await consumer.stop()

        try:
            self.postgres_connection = await psycopg.AsyncConnection.connect(
                self.settings.postgres_dsn,
                autocommit=True,
            )
        except Exception as error:
            self.errors.add(f"PostgreSQL metrics unavailable: {error}")

        if (
            self.settings.kubernetes_metrics_url
            and KUBERNETES_TOKEN.exists()
            and KUBERNETES_CA.exists()
        ):
            token = KUBERNETES_TOKEN.read_text(encoding="utf-8").strip()
            self.kubernetes_client = httpx.AsyncClient(
                headers={"Authorization": f"Bearer {token}"},
                verify=str(KUBERNETES_CA),
                timeout=3,
            )

    async def close(self) -> None:
        if self.kafka_consumer is not None:
            await self.kafka_consumer.stop()
        if self.postgres_connection is not None:
            await self.postgres_connection.close()
        await self.storage_client.aclose()
        if self.kubernetes_client is not None:
            await self.kubernetes_client.aclose()

    async def read_kafka(self) -> tuple[int | None, int | None, int | None]:
        if self.kafka_consumer is None:
            return None, None, None
        try:
            end_offsets = await self.kafka_consumer.end_offsets(self.partitions)
            committed_offsets = await asyncio.gather(
                *(
                    self.kafka_consumer.committed(partition)
                    for partition in self.partitions
                )
            )
            log_end = sum(end_offsets.values())
            committed = sum(offset or 0 for offset in committed_offsets)
            return log_end, committed, max(0, log_end - committed)
        except Exception as error:
            self.errors.add(f"Kafka sample failed: {error}")
            return None, None, None

    async def read_postgres_rows(self) -> int | None:
        if self.postgres_connection is None:
            return None
        try:
            cursor = await self.postgres_connection.execute(
                """
                SELECT n_tup_ins
                FROM pg_stat_user_tables
                WHERE schemaname = 'public' AND relname = 'telemetry'
                """
            )
            row = await cursor.fetchone()
            return int(row[0]) if row else None
        except Exception as error:
            self.errors.add(f"PostgreSQL sample failed: {error}")
            return None

    async def read_storage_metrics(self) -> dict | None:
        if self.kubernetes_client and self.settings.kubernetes_api_url:
            return await self.read_kubernetes_storage_metrics()

        try:
            response = await self.storage_client.get(
                self.settings.storage_metrics_url
            )
            response.raise_for_status()
            return parse_prometheus_metrics(response.text)
        except Exception as error:
            self.errors.add(f"Storage-consumer metrics unavailable: {error}")
            return None

    async def read_kubernetes_storage_metrics(self) -> dict | None:
        """Read every storage pod directly so counters are not mixed."""
        try:
            base_url = self.settings.kubernetes_api_url.rstrip("/")
            namespace = self.settings.kubernetes_namespace
            response = await self.kubernetes_client.get(
                f"{base_url}/api/v1/namespaces/{namespace}/pods",
                params={
                    "labelSelector": self.settings.storage_pod_label_selector
                },
            )
            response.raise_for_status()
            pod_names = [
                item["metadata"]["name"]
                for item in response.json().get("items", [])
                if item.get("status", {}).get("phase") == "Running"
            ]
            if not pod_names:
                raise RuntimeError("no running storage-consumer pods found")

            requests = [
                self.kubernetes_client.get(
                    f"{base_url}/api/v1/namespaces/{namespace}/pods/"
                    f"{pod_name}:{self.settings.storage_metrics_port}/"
                    "proxy/metrics"
                )
                for pod_name in pod_names
            ]
            responses = await asyncio.gather(*requests)
            for pod_response in responses:
                pod_response.raise_for_status()
            return merge_prometheus_metrics(
                [
                    parse_prometheus_metrics(pod_response.text)
                    for pod_response in responses
                ]
            )
        except Exception as error:
            self.errors.add(
                f"Storage-consumer pod metrics unavailable: {error}"
            )
            return None

    async def read_pod_cpu(self) -> dict[str, float]:
        if self.kubernetes_client is None:
            return {}
        try:
            response = await self.kubernetes_client.get(
                self.settings.kubernetes_metrics_url
            )
            response.raise_for_status()
            pod_cpu = {}
            for item in response.json().get("items", []):
                pod_cpu[item["metadata"]["name"]] = sum(
                    cpu_to_millicores(container["usage"]["cpu"])
                    for container in item.get("containers", [])
                )
            return pod_cpu
        except Exception as error:
            self.errors.add(f"Kubernetes CPU metric unavailable: {error}")
        return {}

    async def sample(self) -> dict:
        sampled_at = time.perf_counter()
        elapsed = sampled_at - self.started
        kafka, postgres_rows, storage_metrics, pod_cpu = await asyncio.gather(
            self.read_kafka(),
            self.read_postgres_rows(),
            self.read_storage_metrics(),
            self.read_pod_cpu(),
        )
        log_end, committed, consumer_lag = kafka
        postgres_cpu_values = [
            value
            for pod_name, value in pod_cpu.items()
            if pod_name.startswith(self.settings.postgres_pod_name_prefix)
        ]
        postgres_cpu = (
            sum(postgres_cpu_values) if postgres_cpu_values else None
        )
        storage_values = (
            storage_metrics.get("values", {}) if storage_metrics else {}
        )
        state = {
            "sampled_at": sampled_at,
            "log_end": log_end,
            "committed": committed,
            "postgres_rows": postgres_rows,
            "records_processed": storage_values.get(
                "iot_storage_records_processed_total"
            ),
            "storage_metrics": storage_metrics,
        }

        previous = self.previous_state
        interval = (
            sampled_at - previous["sampled_at"] if previous else elapsed
        )
        sample = {
            "seconds": round(elapsed, 3),
            "consumer_lag": consumer_lag,
            "kafka_messages_per_second": rate(
                log_end,
                previous.get("log_end") if previous else None,
                interval,
            ),
            "consumer_records_per_second": rate(
                state["records_processed"],
                previous.get("records_processed") if previous else None,
                interval,
            ),
            "postgres_rows_per_second": rate(
                postgres_rows,
                previous.get("postgres_rows") if previous else None,
                interval,
            ),
            "batch_size_average": None,
            "batch_processing_average_ms": None,
            "postgres_transaction_average_ms": None,
            "kafka_commit_average_ms": None,
            "pod_cpu_millicores": pod_cpu,
            "postgres_cpu_millicores": postgres_cpu,
        }

        if storage_metrics and previous and previous.get("storage_metrics"):
            previous_metrics = previous["storage_metrics"]
            sample["batch_size_average"] = histogram_average(
                previous_metrics,
                storage_metrics,
                "iot_storage_batch_size",
            )
            for output_name, histogram_name in LATENCY_HISTOGRAMS.items():
                value = histogram_average(
                    previous_metrics,
                    storage_metrics,
                    histogram_name,
                )
                sample[f"{output_name}_average_ms"] = (
                    value * 1_000 if value is not None else None
                )

        if storage_metrics:
            if self.first_storage_metrics is None:
                self.first_storage_metrics = storage_metrics
            self.latest_storage_metrics = storage_metrics

        self.previous_state = state
        self.samples.append(sample)
        return sample

    def summary(
        self,
        load_finished_seconds: float,
        backlog_drain_seconds: float | None,
        drain_timed_out: bool,
    ) -> dict:
        summary = {
            "sample_interval_seconds": self.settings.pipeline_sample_interval_seconds,
            "monitoring_duration_seconds": round(
                self.samples[-1]["seconds"] if self.samples else 0.0,
                3,
            ),
            "peak_consumer_lag": max(
                (
                    sample["consumer_lag"]
                    for sample in self.samples
                    if sample["consumer_lag"] is not None
                ),
                default=None,
            ),
            "final_consumer_lag": (
                self.samples[-1]["consumer_lag"] if self.samples else None
            ),
            "backlog_drain_seconds": backlog_drain_seconds,
            "drain_timed_out": drain_timed_out,
            "average_kafka_messages_per_second": average(
                [
                    sample["kafka_messages_per_second"]
                    for sample in self.samples
                    if sample["seconds"] <= load_finished_seconds
                ]
            ),
            "average_consumer_records_per_second": average(
                [
                    sample["consumer_records_per_second"]
                    for sample in self.samples
                    if sample["seconds"] <= load_finished_seconds
                ]
            ),
            "average_postgres_rows_per_second": average(
                [
                    sample["postgres_rows_per_second"]
                    for sample in self.samples
                    if sample["seconds"] <= load_finished_seconds
                ]
            ),
            "average_batch_size": None,
            "batch_processing_p95_ms": None,
            "postgres_transaction_p95_ms": None,
            "kafka_commit_p95_ms": None,
            "postgres_peak_cpu_millicores": max(
                (
                    sample["postgres_cpu_millicores"]
                    for sample in self.samples
                    if sample["postgres_cpu_millicores"] is not None
                ),
                default=None,
            ),
            "pod_cpu_summary": summarize_pod_cpu(self.samples),
            "monitoring_errors": sorted(self.errors),
        }

        if self.first_storage_metrics and self.latest_storage_metrics:
            summary["average_batch_size"] = histogram_average(
                self.first_storage_metrics,
                self.latest_storage_metrics,
                "iot_storage_batch_size",
            )
            for output_name, histogram_name in LATENCY_HISTOGRAMS.items():
                value = histogram_quantile(
                    self.first_storage_metrics,
                    self.latest_storage_metrics,
                    histogram_name,
                    0.95,
                )
                summary[f"{output_name}_p95_ms"] = (
                    value * 1_000 if value is not None else None
                )

        return summary


async def run_load_test_with_pipeline_metrics(
    request_count: int,
    concurrency: int,
    device_count: int,
    timeout_seconds: float,
    run_id: str,
) -> dict:
    settings = load_settings()
    collector = PipelineMetricsCollector(settings)
    load_task: asyncio.Task | None = None

    await collector.start()
    try:
        await collector.sample()
        load_task = asyncio.create_task(
            run_load_test(
                request_count=request_count,
                concurrency=concurrency,
                device_count=device_count,
                timeout_seconds=timeout_seconds,
                run_id=run_id,
            )
        )

        while not load_task.done():
            await asyncio.wait(
                {load_task},
                timeout=settings.pipeline_sample_interval_seconds,
            )
            await collector.sample()

        result = await load_task
        load_finished_seconds = time.perf_counter() - collector.started
        latest_sample = await collector.sample()
        backlog_drain_seconds = 0.0 if latest_sample["consumer_lag"] == 0 else None
        drain_deadline = (
            time.perf_counter() + settings.pipeline_drain_timeout_seconds
        )

        while (
            latest_sample["consumer_lag"] not in (None, 0)
            and time.perf_counter() < drain_deadline
        ):
            await asyncio.sleep(settings.pipeline_sample_interval_seconds)
            latest_sample = await collector.sample()
            if latest_sample["consumer_lag"] == 0:
                backlog_drain_seconds = round(
                    latest_sample["seconds"] - load_finished_seconds,
                    3,
                )

        drain_timed_out = latest_sample["consumer_lag"] not in (None, 0)
        result["pipeline_summary"] = collector.summary(
            load_finished_seconds=load_finished_seconds,
            backlog_drain_seconds=backlog_drain_seconds,
            drain_timed_out=drain_timed_out,
        )
        result["pipeline_samples"] = collector.samples
        return result
    finally:
        if load_task is not None and not load_task.done():
            load_task.cancel()
            await asyncio.gather(load_task, return_exceptions=True)
        await collector.close()
