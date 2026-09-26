import asyncio
import json
import os
import re
from datetime import datetime, timezone
from pathlib import Path

import streamlit as st

from dashboard.pipeline_metrics import run_load_test_with_pipeline_metrics
from scripts.load_test import save_result


RESULTS_DIRECTORY = Path(os.getenv("LOAD_TEST_RESULTS_DIR", "results"))


def safe_test_name(test_name: str) -> str:
    """Create a short value that is safe in a run ID and filename."""
    name = re.sub(r"[^a-zA-Z0-9_-]+", "-", test_name.strip())
    return name.strip("-_")[:50].lower() or "load-test"


def load_saved_results() -> tuple[list[dict], list[str]]:
    results = []
    invalid_files = []

    for result_path in RESULTS_DIRECTORY.glob("*.json"):
        try:
            result = json.loads(result_path.read_text(encoding="utf-8"))
            result["result_file"] = result_path.name
            results.append(result)
        except (OSError, json.JSONDecodeError, TypeError):
            invalid_files.append(result_path.name)

    results.sort(key=lambda result: result.get("started_at", ""), reverse=True)
    return results, invalid_files


def summary_row(result: dict) -> dict:
    latency = result.get("latency_ms", {})
    pipeline = result.get("pipeline_summary", {})
    return {
        "Test name": result.get("test_name", "Not recorded"),
        "Run": result.get("run_id", "unknown"),
        "Started (UTC)": result.get("started_at", ""),
        "Requests": result.get("request_count", 0),
        "Concurrency": result.get("concurrency", 0),
        "Devices": result.get("device_count", 0),
        "Timeout (s)": result.get("timeout_seconds"),
        "Throughput (req/s)": round(
            result.get("throughput_requests_per_second", 0), 2
        ),
        "p50 (ms)": round(latency.get("p50", 0), 2),
        "p95 (ms)": round(latency.get("p95", 0), 2),
        "p99 (ms)": round(latency.get("p99", 0), 2),
        "Errors (%)": round(result.get("error_rate_percent", 0), 2),
        "Peak lag": pipeline.get("peak_consumer_lag"),
        "Drain (s)": pipeline.get("backlog_drain_seconds"),
        "DB p95 (ms)": pipeline.get("postgres_transaction_p95_ms"),
        "File": result.get("result_file", ""),
    }


def show_result_metrics(result: dict) -> None:
    latency = result["latency_ms"]
    throughput, errors, duration, success = st.columns(4)
    throughput.metric(
        "Throughput",
        f"{result['throughput_requests_per_second']:.2f} req/s",
    )
    errors.metric("Error rate", f"{result['error_rate_percent']:.2f}%")
    duration.metric("Duration", f"{result['duration_seconds']:.2f} s")
    success.metric(
        "Successful",
        f"{result['successful_requests']}/{result['request_count']}",
    )

    p50, p95, p99 = st.columns(3)
    p50.metric("HTTP p50", f"{latency['p50']:.2f} ms")
    p95.metric("HTTP p95", f"{latency['p95']:.2f} ms")
    p99.metric("HTTP p99", f"{latency['p99']:.2f} ms")


def format_number(value: float | None, suffix: str = "", digits: int = 2) -> str:
    if value is None:
        return "Not recorded"
    return f"{value:.{digits}f}{suffix}"


def show_pipeline_metrics(result: dict) -> None:
    summary = result.get("pipeline_summary")
    samples = result.get("pipeline_samples", [])
    if not summary:
        st.info("This older result does not contain pipeline metrics.")
        return

    st.subheader("Pipeline summary")
    peak_lag, final_lag, drain, db_cpu = st.columns(4)
    peak_lag.metric(
        "Peak Kafka lag",
        format_number(summary.get("peak_consumer_lag"), digits=0),
    )
    final_lag.metric(
        "Final Kafka lag",
        format_number(summary.get("final_consumer_lag"), digits=0),
    )
    drain.metric(
        "Backlog drain",
        format_number(summary.get("backlog_drain_seconds"), " s"),
    )
    db_cpu.metric(
        "Peak PostgreSQL CPU",
        format_number(summary.get("postgres_peak_cpu_millicores"), " mCPU"),
    )

    kafka_rate, consumer_rate, postgres_rate, batch_size = st.columns(4)
    kafka_rate.metric(
        "Kafka input rate",
        format_number(summary.get("average_kafka_messages_per_second"), " msg/s"),
    )
    consumer_rate.metric(
        "Consumer rate",
        format_number(summary.get("average_consumer_records_per_second"), " rec/s"),
    )
    postgres_rate.metric(
        "PostgreSQL insert rate",
        format_number(summary.get("average_postgres_rows_per_second"), " rows/s"),
    )
    batch_size.metric(
        "Average batch size",
        format_number(summary.get("average_batch_size"), digits=1),
    )

    batch_latency, database_latency, commit_latency = st.columns(3)
    batch_latency.metric(
        "Batch processing p95",
        format_number(summary.get("batch_processing_p95_ms"), " ms"),
    )
    database_latency.metric(
        "PostgreSQL transaction p95",
        format_number(summary.get("postgres_transaction_p95_ms"), " ms"),
    )
    commit_latency.metric(
        "Kafka offset commit p95",
        format_number(summary.get("kafka_commit_p95_ms"), " ms"),
    )

    if summary.get("drain_timed_out"):
        st.warning("Kafka lag did not reach zero before the monitoring timeout.")
    if summary.get("monitoring_errors"):
        st.warning(
            "Some metrics were unavailable: "
            + "; ".join(summary["monitoring_errors"])
        )

    if not samples:
        return

    st.subheader("Pipeline over time")
    st.caption(
        "Lag is the backlog. Rates and latency values are calculated "
        "between consecutive samples."
    )
    st.line_chart(samples, x="seconds", y=["consumer_lag"])
    st.line_chart(
        samples,
        x="seconds",
        y=[
            "kafka_messages_per_second",
            "consumer_records_per_second",
            "postgres_rows_per_second",
        ],
    )
    st.line_chart(
        samples,
        x="seconds",
        y=[
            "batch_processing_average_ms",
            "postgres_transaction_average_ms",
            "kafka_commit_average_ms",
        ],
    )
    if any(sample.get("postgres_cpu_millicores") is not None for sample in samples):
        st.line_chart(samples, x="seconds", y=["postgres_cpu_millicores"])

    pod_cpu_summary = summary.get("pod_cpu_summary", [])
    if pod_cpu_summary:
        st.subheader("CPU per pod")
        st.caption(
            "1,000 mCPU is one full CPU core. Pod names include a changing "
            "Kubernetes-generated suffix."
        )
        st.dataframe(
            [
                {
                    "Pod": row["pod"],
                    "Average CPU (mCPU)": round(row["average_millicores"], 2),
                    "Peak CPU (mCPU)": round(row["peak_millicores"], 2),
                }
                for row in pod_cpu_summary
            ],
            width="stretch",
            hide_index=True,
        )

        pod_names = [row["pod"] for row in pod_cpu_summary]
        cpu_samples = []
        for sample in samples:
            cpu_row = {"seconds": sample["seconds"]}
            cpu_row.update(sample.get("pod_cpu_millicores", {}))
            cpu_samples.append(cpu_row)
        st.line_chart(cpu_samples, x="seconds", y=pod_names)


st.set_page_config(page_title="IoT Load Tests", layout="wide")
st.title("IoT Load Test Dashboard")
st.caption(
    "Run the HTTP load generator and observe Kafka, storage-consumer, and "
    "PostgreSQL behavior in the same saved result."
)

run_tab, results_tab = st.tabs(["Start a test", "Saved results"])

with run_tab:
    st.subheader("Test inputs")
    st.info(
        "Change one input at a time when comparing runs. The dashboard measures "
        "HTTP latency, pipeline rates, Kafka lag, consumer timing, and "
        "database activity."
    )

    with st.form("load-test-form"):
        test_name = st.text_input(
            "Test name",
            value="baseline",
            max_chars=80,
            help="A timestamp is added automatically, so names can be reused.",
        )
        requests_column, concurrency_column = st.columns(2)
        request_count = requests_column.number_input(
            "Total requests",
            min_value=1,
            max_value=100_000,
            value=1_000,
            step=100,
        )
        concurrency = concurrency_column.number_input(
            "Concurrent workers",
            min_value=1,
            max_value=500,
            value=10,
        )

        devices_column, timeout_column = st.columns(2)
        device_count = devices_column.number_input(
            "Simulated devices",
            min_value=1,
            max_value=10_000,
            value=100,
            step=10,
        )
        timeout_seconds = timeout_column.number_input(
            "Request timeout (seconds)",
            min_value=0.1,
            max_value=120.0,
            value=10.0,
            step=0.5,
        )
        submitted = st.form_submit_button("Start load test", type="primary")

    if submitted and not test_name.strip():
        st.error("Enter a test name before starting the load test.")
    elif submitted:
        timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S%fZ")
        run_id = f"{safe_test_name(test_name)}-{timestamp}"
        output_path = RESULTS_DIRECTORY / f"{run_id}.json"

        with st.spinner(
            "Load test and pipeline monitoring are running. "
            "Keep this page open..."
        ):
            try:
                latest_result = asyncio.run(
                    run_load_test_with_pipeline_metrics(
                        request_count=int(request_count),
                        concurrency=int(concurrency),
                        device_count=int(device_count),
                        timeout_seconds=float(timeout_seconds),
                        run_id=run_id,
                    )
                )
                latest_result["test_name"] = test_name.strip()
                save_result(latest_result, output_path)
            except Exception as error:
                st.error(f"Load test failed: {error}")
            else:
                st.success(f"Saved result as {output_path.name}")
                show_result_metrics(latest_result)
                show_pipeline_metrics(latest_result)
                st.write("HTTP status codes", latest_result["status_codes"])

with results_tab:
    saved_results, invalid_result_files = load_saved_results()

    if invalid_result_files:
        st.warning(
            "Could not read these JSON files: " + ", ".join(invalid_result_files)
        )

    if not saved_results:
        st.info("No saved results yet. Start a load test first.")
    else:
        st.subheader(f"History ({len(saved_results)} runs)")
        summary = [summary_row(result) for result in saved_results]
        st.dataframe(summary, width="stretch", hide_index=True)

        st.subheader("Compare runs")
        st.bar_chart(summary, x="Run", y="Throughput (req/s)")
        st.bar_chart(summary, x="Run", y=["p50 (ms)", "p95 (ms)", "p99 (ms)"])

        selected_run_id = st.selectbox(
            "Inspect one run",
            options=[result["run_id"] for result in saved_results],
            format_func=lambda run_id: next(
                (
                    f"{result.get('test_name', 'Not recorded')} — {run_id}"
                    for result in saved_results
                    if result["run_id"] == run_id
                ),
                run_id,
            ),
        )
        selected_result = next(
            result
            for result in saved_results
            if result["run_id"] == selected_run_id
        )
        show_result_metrics(selected_result)
        show_pipeline_metrics(selected_result)
        st.write("HTTP status codes", selected_result.get("status_codes", {}))
        st.write("Exceptions", selected_result.get("exception_types", {}))
        with st.expander("Raw JSON"):
            st.json(selected_result)
