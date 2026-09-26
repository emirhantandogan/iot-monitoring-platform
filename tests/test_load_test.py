import json

from scripts.load_test import percentile, save_result


def test_percentile_uses_nearest_rank() -> None:
    values = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]

    assert percentile(values, 50) == 5
    assert percentile(values, 95) == 10
    assert percentile(values, 99) == 10


def test_percentile_sorts_input() -> None:
    assert percentile([30, 10, 20], 50) == 20


def test_save_result_creates_parent_directory(tmp_path) -> None:
    output_path = tmp_path / "results" / "run.json"
    result = {"run_id": "test-run", "concurrency": 10}

    save_result(result, output_path)

    assert json.loads(output_path.read_text(encoding="utf-8")) == result
