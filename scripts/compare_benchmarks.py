import argparse
import json
from pathlib import Path


def load_result(path: Path) -> dict:
    return json.loads(path.read_text(encoding="utf-8"))


def percent_change(baseline: float, current: float) -> float:
    if baseline == 0:
        return 0.0
    return (current - baseline) / baseline * 100


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Compare load-test JSON results")
    parser.add_argument("results", type=Path, nargs="+")
    return parser.parse_args()


def main() -> None:
    arguments = parse_args()
    loaded = [(path.name, load_result(path)) for path in arguments.results]
    header = (
        f"{'result':<28} {'conc':>6} {'requests':>8} {'rps':>10} "
        f"{'p50 ms':>10} {'p95 ms':>10} {'p99 ms':>10} {'error %':>9}"
    )
    print(header)
    print("-" * len(header))

    for name, result in loaded:
        latency = result["latency_ms"]
        print(
            f"{name:<28} {result['concurrency']:>6} "
            f"{result['request_count']:>8} "
            f"{result['throughput_requests_per_second']:>10.2f} "
            f"{latency['p50']:>10.2f} {latency['p95']:>10.2f} "
            f"{latency['p99']:>10.2f} "
            f"{result['error_rate_percent']:>9.2f}"
        )

    if len(loaded) > 1:
        baseline = loaded[0][1]
        print("\nChanges compared with the first result:")
        for name, result in loaded[1:]:
            throughput_change = percent_change(
                baseline["throughput_requests_per_second"],
                result["throughput_requests_per_second"],
            )
            p95_change = percent_change(
                baseline["latency_ms"]["p95"],
                result["latency_ms"]["p95"],
            )
            print(
                f"{name}: throughput {throughput_change:+.1f}%, "
                f"p95 latency {p95_change:+.1f}%"
            )


if __name__ == "__main__":
    main()

