from dashboard.pipeline_metrics import (
    cpu_to_millicores,
    histogram_average,
    histogram_quantile,
    merge_prometheus_metrics,
    parse_prometheus_metrics,
    rate,
    summarize_pod_cpu,
)


START_METRICS = """
# TYPE iot_storage_records_processed counter
iot_storage_records_processed_total 100
# TYPE iot_storage_postgres_transaction_seconds histogram
iot_storage_postgres_transaction_seconds_bucket{le="0.01"} 2
iot_storage_postgres_transaction_seconds_bucket{le="0.1"} 4
iot_storage_postgres_transaction_seconds_bucket{le="1.0"} 5
iot_storage_postgres_transaction_seconds_bucket{le="+Inf"} 5
iot_storage_postgres_transaction_seconds_count 5
iot_storage_postgres_transaction_seconds_sum 0.5
"""

END_METRICS = """
# TYPE iot_storage_records_processed counter
iot_storage_records_processed_total 150
# TYPE iot_storage_postgres_transaction_seconds histogram
iot_storage_postgres_transaction_seconds_bucket{le="0.01"} 4
iot_storage_postgres_transaction_seconds_bucket{le="0.1"} 11
iot_storage_postgres_transaction_seconds_bucket{le="1.0"} 15
iot_storage_postgres_transaction_seconds_bucket{le="+Inf"} 15
iot_storage_postgres_transaction_seconds_count 15
iot_storage_postgres_transaction_seconds_sum 2.5
"""


def test_parse_prometheus_counter_and_histogram() -> None:
    parsed = parse_prometheus_metrics(END_METRICS)

    assert parsed["values"]["iot_storage_records_processed_total"] == 150
    histogram = parsed["histograms"][
        "iot_storage_postgres_transaction_seconds"
    ]
    assert histogram["count"] == 15
    assert histogram["sum"] == 2.5
    assert histogram["buckets"]["0.1"] == 11


def test_merge_prometheus_metrics_sums_pods() -> None:
    first = parse_prometheus_metrics(START_METRICS)
    second = parse_prometheus_metrics(END_METRICS)
    merged = merge_prometheus_metrics([first, second])

    assert merged["values"]["iot_storage_records_processed_total"] == 250
    histogram = merged["histograms"][
        "iot_storage_postgres_transaction_seconds"
    ]
    assert histogram["count"] == 20
    assert histogram["sum"] == 3.0
    assert histogram["buckets"]["0.1"] == 15


def test_rate_uses_counter_difference() -> None:
    assert rate(150, 100, 2) == 25
    assert rate(90, 100, 2) == 0
    assert rate(None, 100, 2) is None


def test_histogram_interval_average_and_p95_bucket() -> None:
    start = parse_prometheus_metrics(START_METRICS)
    end = parse_prometheus_metrics(END_METRICS)
    name = "iot_storage_postgres_transaction_seconds"

    assert histogram_average(start, end, name) == 0.2
    assert histogram_quantile(start, end, name, 0.95) == 1.0


def test_cpu_quantities_convert_to_millicores() -> None:
    assert cpu_to_millicores("2500000n") == 2.5
    assert cpu_to_millicores("2500u") == 2.5
    assert cpu_to_millicores("2.5m") == 2.5
    assert cpu_to_millicores("0.5") == 500


def test_pod_cpu_summary_contains_average_and_peak() -> None:
    samples = [
        {
            "pod_cpu_millicores": {
                "ingestion-abc": 10.0,
                "storage-consumer-def": 20.0,
            }
        },
        {
            "pod_cpu_millicores": {
                "ingestion-abc": 30.0,
                "storage-consumer-def": 40.0,
            }
        },
    ]

    assert summarize_pod_cpu(samples) == [
        {
            "pod": "ingestion-abc",
            "average_millicores": 20.0,
            "peak_millicores": 30.0,
        },
        {
            "pod": "storage-consumer-def",
            "average_millicores": 30.0,
            "peak_millicores": 40.0,
        },
    ]
