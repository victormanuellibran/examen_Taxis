import csv
import json
from datetime import datetime
from pathlib import Path
from unittest.mock import Mock

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq
import pytest
import yaml

from src import ingest, model, pipeline, quality, schema, transform
from src.config import ROOT, monthly_path


def _write_synthetic_raw(path: Path, month: str, include_cbd: bool):
    year, mon = int(month[:4]), int(month[5:])
    t1_pu = datetime(year, mon, 10, 8, 0, 0)
    t1_do = datetime(year, mon, 10, 8, 20, 0)
    t2_pu = datetime(year, mon, 15, 14, 0, 0)
    t2_do = datetime(year, mon, 15, 14, 30, 0)
    t_bad_pu = datetime(year, mon, 20, 10, 0, 0)
    t_bad_do = datetime(year, mon, 20, 9, 50, 0)  # R02: dropoff <= pickup

    data = {
        "VendorID": pa.array([1, 2, 1, 1], type=pa.int32()),
        "tpep_pickup_datetime": pa.array([t1_pu, t2_pu, t1_pu, t_bad_pu], type=pa.timestamp("us")),
        "tpep_dropoff_datetime": pa.array([t1_do, t2_do, t1_do, t_bad_do], type=pa.timestamp("us")),
        "passenger_count": pa.array([1, 0, 1, 2], type=pa.int64()),  # row 2 has R05 warning (0 passengers)
        "trip_distance": pa.array([5.0, 10.0, 5.0, 3.0], type=pa.float64()),  # row 3 is exact dup of row 1 (R07)
        "RatecodeID": pa.array([1, 1, 1, 1], type=pa.int64()),
        "store_and_fwd_flag": pa.array(["N", "N", "N", "N"], type=pa.string()),
        "PULocationID": pa.array([132, 138, 132, 161], type=pa.int32()),  # 132=JFK, 138=LaGuardia, 161=Midtown
        "DOLocationID": pa.array([161, 132, 161, 138], type=pa.int32()),
        "payment_type": pa.array([1, 0, 1, 2], type=pa.int64()),
        "fare_amount": pa.array([20.0, 40.0, 20.0, 15.0], type=pa.float64()),
        "extra": pa.array([1.0, 1.0, 1.0, 0.0], type=pa.float64()),
        "mta_tax": pa.array([0.5, 0.5, 0.5, 0.5], type=pa.float64()),
        "tip_amount": pa.array([4.0, -1.0, 4.0, 0.0], type=pa.float64()),  # row 2 has R08 warning (negative tip)
        "tolls_amount": pa.array([0.0, 0.0, 0.0, 0.0], type=pa.float64()),
        "improvement_surcharge": pa.array([1.0, 1.0, 1.0, 1.0], type=pa.float64()),
        "total_amount": pa.array([26.5, 41.5, 26.5, 16.5], type=pa.float64()),
        "congestion_surcharge": pa.array([2.5, 2.5, 2.5, 2.5], type=pa.float64()),
        "Airport_fee": pa.array([1.75, 1.75, 1.75, 0.0], type=pa.float64()),
    }
    if include_cbd:
        data["cbd_congestion_fee"] = pa.array([0.75, 0.0, 0.75, 0.75], type=pa.float64())

    path.parent.mkdir(parents=True, exist_ok=True)
    pq.write_table(pa.table(data), path)


ZONES_CSV = (
    "LocationID,Borough,Zone,service_zone\n"
    "132,Queens,JFK Airport,Airports\n"
    "138,Queens,LaGuardia Airport,Airports\n"
    "161,Manhattan,Midtown Center,Yellow Zone\n"
)


def _setup_mock_ingest(tmp_path: Path, monkeypatch):
    monkeypatch.setenv("NYC_TAXI_DATA_DIR", str(tmp_path))
    fixtures = tmp_path / "_fixtures"
    raw_2024 = fixtures / "yellow_tripdata_2024-06.parquet"
    raw_2025 = fixtures / "yellow_tripdata_2025-06.parquet"
    _write_synthetic_raw(raw_2024, "2024-06", include_cbd=False)
    _write_synthetic_raw(raw_2025, "2025-06", include_cbd=True)

    def fake_download(url: str, target: Path):
        target.parent.mkdir(parents=True, exist_ok=True)
        if target.name == "taxi_zone_lookup.csv":
            target.write_text(ZONES_CSV, encoding="utf-8")
        elif "2024-06" in target.name:
            target.write_bytes(raw_2024.read_bytes())
        elif "2025-06" in target.name:
            target.write_bytes(raw_2025.read_bytes())
        else:
            raise ValueError(f"URL no esperada: {url}")

    mock_dl = Mock(side_effect=fake_download)
    monkeypatch.setattr(ingest, "download", mock_dl)
    return mock_dl


def test_end_to_end_synthetic_pipeline_and_idempotency(tmp_path, monkeypatch):
    """Ejecuta las 5 etapas extremo a extremo con datos sintéticos dos veces y verifica idempotencia y reconciliación."""
    mock_dl = _setup_mock_ingest(tmp_path, monkeypatch)
    resultados_out = tmp_path / "RESULTADOS_test.md"
    monkeypatch.setattr(model, "SQL_DIR", ROOT / "sql")

    # Redirigir escritura de RESULTADOS.md dentro de model.run_model para no tocar la raíz del repo
    orig_write_text = Path.write_text

    def safe_write_text(self: Path, data: str, *args, **kwargs):
        if self.name == "RESULTADOS.md":
            return orig_write_text(resultados_out, data, *args, **kwargs)
        return orig_write_text(self, data, *args, **kwargs)

    monkeypatch.setattr(Path, "write_text", safe_write_text)

    # 1ra ejecución completa
    assert pipeline.main(["--mes", "2025-06"]) == 0
    assert mock_dl.call_count == 3  # 1 zone + 2 parquet

    # Verificar reconciliación exacta por mes (4 leídas = 2 aceptadas + 2 rechazadas por R07 y R02)
    for m in ["2024-06", "2025-06"]:
        rep = json.loads((tmp_path / "reports" / f"quality_{m}.json").read_text(encoding="utf-8"))
        assert rep["rows_read"] == 4
        assert rep["rows_accepted"] == 2
        assert rep["rows_quarantined"] == 2
        assert rep["reconciled"] is True
        acc_rows = pq.ParquetFile(monthly_path("validated", m, "accepted.parquet")).metadata.num_rows
        rej_rows = pq.ParquetFile(monthly_path("quarantine", m, "rejected.parquet")).metadata.num_rows
        cur_rows = pq.ParquetFile(monthly_path("curated", m, "part-000.parquet")).metadata.num_rows
        assert acc_rows == 2 and rej_rows == 2 and cur_rows == 2

    # Verificar Warehouse DuckDB
    wh_path = tmp_path / "warehouse" / "nyc_taxi.duckdb"
    assert wh_path.exists()
    with duckdb.connect(str(wh_path), read_only=True) as con:
        assert con.execute("SELECT count(*) FROM dim_zone").fetchone()[0] == 3
        assert con.execute("SELECT count(*) FROM dim_date").fetchone()[0] == 4
        assert con.execute("SELECT count(*) FROM fact_trips").fetchone()[0] == 4
        assert con.execute("SELECT sum(trips) FROM agg_zone_hour").fetchone()[0] == 4

    # 2da ejecución completa (idempotencia)
    assert pipeline.main(["--mes", "2025-06"]) == 0
    assert mock_dl.call_count == 3  # Sin nuevas descargas

    with duckdb.connect(str(wh_path), read_only=True) as con:
        assert con.execute("SELECT count(*) FROM fact_trips").fetchone()[0] == 4
        assert con.execute("SELECT sum(trips) FROM agg_zone_hour").fetchone()[0] == 4

    assert resultados_out.exists()
    content = resultados_out.read_text(encoding="utf-8")
    for q in ["q1_top_zones.sql", "q2_peak_revenue_hour.sql", "q3_cbd_fee.sql", "q4_tip_by_payment.sql", "q5_airports.sql", "q6_month_variation.sql"]:
        assert q in content


def test_partition_isolation_on_reprocess(tmp_path, monkeypatch):
    """Demuestra que reprocesar un mes en transform reemplaza sólo su partición sin alterar otros meses."""
    _setup_mock_ingest(tmp_path, monkeypatch)
    for m in ["2024-06", "2025-06"]:
        acc = quality.run_quality(m, schema.run_schema(m, ingest.run_ingest(m)))
        transform.run_transform(m, acc)

    p2024 = monthly_path("curated", "2024-06", "part-000.parquet")
    p2025 = monthly_path("curated", "2025-06", "part-000.parquet")
    bytes_2024_before = p2024.read_bytes()
    mtime_2024_before = p2024.stat().st_mtime_ns

    # Reprocesar únicamente 2025-06
    acc_2025 = monthly_path("validated", "2025-06", "accepted.parquet")
    transform.run_transform("2025-06", acc_2025)

    assert p2024.read_bytes() == bytes_2024_before
    assert p2024.stat().st_mtime_ns == mtime_2024_before
    assert pq.ParquetFile(p2025).metadata.num_rows == 2
