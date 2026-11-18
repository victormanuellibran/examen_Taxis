import csv
from unittest.mock import Mock

import pyarrow as pa
import pyarrow.parquet as pq
import pytest
import requests

from src import ingest


def test_ingest_idempotent(tmp_path, monkeypatch):
    monkeypatch.setenv("NYC_TAXI_DATA_DIR", str(tmp_path))
    source = tmp_path / "fixture.parquet"
    pq.write_table(pa.table({"value": [1, 2]}), source)
    payload = source.read_bytes()
    def fake_download(url, target):
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_bytes(payload if target.suffix == ".parquet" else b"LocationID,Borough,Zone\n1,Manhattan,Example\n")
    mock = Mock(side_effect=fake_download)
    monkeypatch.setattr(ingest, "download", mock)
    first = ingest.run_ingest("2025-06")
    original = first.read_bytes()
    assert ingest.run_ingest("2025-06") == first
    assert first.read_bytes() == original
    assert mock.call_count == 2  # zonas + un mensual, ninguna descarga en segunda ejecución
    assert pq.ParquetFile(first).metadata.num_rows == 2
    with (tmp_path / "control/ingestion_log.csv").open() as stream:
        rows = list(csv.DictReader(stream))
    assert [r["action"] for r in rows] == ["downloaded", "downloaded", "reused", "reused"]
    first.write_bytes(b"corrupt")
    with pytest.raises(ValueError, match="SHA-256"):
        ingest.run_ingest("2025-06")
    assert first.read_bytes() == b"corrupt"
    assert mock.call_count == 2


def test_network_failure_removes_partial(tmp_path, monkeypatch):
    monkeypatch.setenv("NYC_TAXI_DATA_DIR", str(tmp_path))
    get = Mock(side_effect=requests.ConnectionError("offline"))
    monkeypatch.setattr(ingest.requests, "get", get)
    sleep = Mock()
    monkeypatch.setattr(ingest.time, "sleep", sleep)
    with pytest.raises(requests.ConnectionError):
        ingest.run_ingest("2025-06")
    assert get.call_count == 4
    assert [call.args[0] for call in sleep.call_args_list] == [1, 2, 4]
    assert not list(tmp_path.rglob("*.part"))
    assert "error" in (tmp_path / "control/ingestion_log.csv").read_text()
