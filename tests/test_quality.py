import json
from datetime import datetime

import pyarrow as pa
import pyarrow.parquet as pq

from src.config import monthly_path
from src.quality import run_quality


def test_all_rules_and_multiple_violations(tmp_path, monkeypatch):
    monkeypatch.setenv("NYC_TAXI_DATA_DIR", str(tmp_path))
    zones = tmp_path / "reference/taxi_zone_lookup.csv"
    zones.parent.mkdir()
    zones.write_text("LocationID,Borough,Zone\n1,Manhattan,Example\n2,Queens,Another\n")
    base = dict(tpep_pickup_datetime=datetime(2025, 6, 1, 12),
                tpep_dropoff_datetime=datetime(2025, 6, 1, 12, 10),
                pu_location_id=1, do_location_id=2, passenger_count=1,
                trip_distance=2.0, total_amount=15.0, tip_amount=2.0,
                fare_amount=10.0, payment_type=1, cbd_congestion_fee=2.5, source_file="fixture.parquet")
    rows = [base.copy(), {**base, "passenger_count": 0, "tip_amount": -1.0},
            {**base, "trip_distance": 0.0, "total_amount": -5.0, "tip_amount": -2.0},
            {**base, "tpep_pickup_datetime": datetime(2025, 5, 31)},
            {**base, "tpep_dropoff_datetime": base["tpep_pickup_datetime"]},
            {**base, "pu_location_id": 999}, base.copy(),
            {**base, "trip_distance": 201.0},
            {**base, "passenger_count": None, "fare_amount": 11.0}]
    staged = tmp_path / "staged.parquet"
    pq.write_table(pa.Table.from_pylist(rows), staged)
    accepted = pq.ParquetFile(run_quality("2025-06", staged)).read().to_pylist()
    rejected = pq.ParquetFile(monthly_path("quarantine", "2025-06", "rejected.parquet")).read().to_pylist()
    assert len(accepted) == 3
    assert len(rejected) == 6
    assert any(row["warning_flags"] == ["R05", "R08"] for row in accepted)
    multi = next(row for row in rejected if row["total_amount"] == -5)
    assert multi["violated_rules"] == ["R03", "R04", "R08"]
    assert multi["rejection_flags"] == ["R03", "R04"]
    assert multi["warning_flags"] == ["R08"]
    assert multi["source_file"] == "fixture.parquet"
    assert multi["trip_distance"] == 0 and multi["tip_amount"] == -2
    report = json.loads((tmp_path / "reports/quality_2025-06.json").read_text())
    assert report["rule_violations"] == {"R01": 1, "R02": 1, "R03": 2, "R04": 1, "R05": 2, "R06": 1, "R07": 1, "R08": 2}
    assert report["rows_read"] == report["rows_accepted"] + report["rows_quarantined"] == 9
    run_quality("2025-06", staged)
    assert pq.ParquetFile(monthly_path("validated", "2025-06", "accepted.parquet")).metadata.num_rows == 3
