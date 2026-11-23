from pathlib import Path
from types import SimpleNamespace

import pytest
import requests

from src import ingest, pipeline
from src.config import monthly_path, parse_month
from src.dbt_runner import _dbt_vars
from src.results import interpret


@pytest.mark.parametrize(
    ("month", "expected"),
    [("2025-01", (2025, 1)), ("2025-12", (2025, 12))],
)
def test_parse_month_and_builds_partition_path(month, expected, monkeypatch, tmp_path):
    assert parse_month(month) == expected
    monkeypatch.setenv("NYC_TAXI_DATA_DIR", str(tmp_path))
    assert monthly_path("raw", month, f"yellow_tripdata_{month}.parquet") == (
        tmp_path / "raw" / "yellow" / f"year={expected[0]}" / f"month={expected[1]:02d}" / f"yellow_tripdata_{month}.parquet"
    )


@pytest.mark.parametrize("month", ["2025", "2025-00", "2025-13", "bad-month"])
def test_parse_month_rejects_invalid_values(month):
    with pytest.raises(ValueError, match="Mes inválido"):
        parse_month(month)


def test_missing_official_month_has_clear_error(monkeypatch, tmp_path):
    monkeypatch.setenv("NYC_TAXI_DATA_DIR", str(tmp_path))

    def fake_ensure(url, path):
        if "trip-data" in url:
            error = requests.HTTPError("404")
            error.response = SimpleNamespace(status_code=404)
            raise error
        return path

    monkeypatch.setattr(ingest, "ensure_file", fake_ensure)
    with pytest.raises(FileNotFoundError, match="período 2099-01.*no está publicado"):
        ingest.run_ingest("2099-01")


def test_dbt_vars_are_derived_for_non_june_periods():
    assert _dbt_vars(["2024-08", "2025-08"]) == {
        "comparison_month": "2024-08",
        "target_month": "2025-08",
        "comparison_year": 2024,
        "comparison_month_num": "08",
        "target_year": 2025,
        "target_month_num": "08",
    }


def test_q6_interpretation_uses_selected_months_without_historical_claims():
    headers = [
        "comparison_month", "target_month", "comparison_trips", "target_trips",
        "absolute_trip_variation", "trip_variation_pct", "comparison_average_fare",
        "target_average_fare", "average_fare_variation",
    ]
    rows = [("2025-01", "2026-01", 10, 12, 2, 20.0, 10.0, 9.5, -0.5)]
    text = interpret("q6_month_variation.sql", headers, rows, ["2025-01", "2026-01"])
    assert "2025-01" in text and "2026-01" in text
    assert "2025-06" not in text


def test_pipeline_selects_comparison_month_for_december(monkeypatch):
    calls = []

    def stage(name):
        def run(month, *args):
            calls.append((name, month, args))
            return Path(name + month)
        return run

    for name in ["ingest", "schema", "quality"]:
        monkeypatch.setattr(pipeline, "run_" + name, stage(name))
    modules = {
        "src.transform": SimpleNamespace(run_transform=stage("transform")),
        "src.model": SimpleNamespace(run_model=lambda months: calls.append(("model", months))),
    }
    monkeypatch.setattr(pipeline.importlib, "import_module", modules.__getitem__)

    assert pipeline.main(["--mes", "2025-12"]) == 0
    assert [call[1] for call in calls if call[0] == "ingest"] == ["2024-12", "2025-12"]
    assert calls[-1] == ("model", ["2024-12", "2025-12"])
