import json

import pyarrow as pa
import pyarrow.parquet as pq
import pytest
import yaml

from src.config import ROOT
from src.schema import run_schema


def raw_fixture(path, **overrides):
    types = {"BIGINT": pa.int64(), "TIMESTAMP": pa.timestamp("us"), "DOUBLE": pa.float64(), "VARCHAR": pa.string()}
    columns = yaml.safe_load((ROOT / "contracts/yellow_schema.yaml").read_text())["columns"]
    values = {name: pa.array([None], type=types[spec["type"]]) for name, spec in columns.items() if name != "cbd_congestion_fee"}
    values.update(overrides)
    pq.write_table(pa.table(values), path)


def test_schema_adds_typed_null_and_maps_names(tmp_path, monkeypatch):
    monkeypatch.setenv("NYC_TAXI_DATA_DIR", str(tmp_path))
    raw = tmp_path / "raw.parquet"
    raw_fixture(raw, PULocationID=pa.array([1], type=pa.int32()))
    result = run_schema("2024-06", raw)
    table = pq.ParquetFile(result).read()
    assert table["pu_location_id"].to_pylist() == [1]
    assert table["cbd_congestion_fee"].to_pylist() == [None]
    assert table.schema.field("cbd_congestion_fee").type == pa.float64()
    assert table["source_file"].to_pylist() == ["raw.parquet"]
    report = json.loads((tmp_path / "reports/schema_2024-06.json").read_text())
    assert report["missing_columns"] == ["cbd_congestion_fee"]
    assert "PULocationID" in report["type_differences"]


@pytest.mark.parametrize("change,match", [
    ({"unexpected": pa.array([1])}, "Columnas nuevas"),
    ({"trip_distance": pa.array(["bad"])}, "Conversiones no autorizadas"),
])
def test_schema_rejects_drift(tmp_path, monkeypatch, change, match):
    monkeypatch.setenv("NYC_TAXI_DATA_DIR", str(tmp_path))
    raw = tmp_path / "raw.parquet"
    raw_fixture(raw, **change)
    with pytest.raises(ValueError, match=match):
        run_schema("2024-06", raw)
    assert not list(tmp_path.rglob("normalized.parquet"))


def test_schema_requires_cbd_in_2025(tmp_path, monkeypatch):
    monkeypatch.setenv("NYC_TAXI_DATA_DIR", str(tmp_path))
    raw = tmp_path / "raw.parquet"
    raw_fixture(raw)
    with pytest.raises(ValueError, match="faltantes"):
        run_schema("2025-06", raw)
