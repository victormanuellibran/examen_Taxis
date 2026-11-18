"""Construcción de la capa curada a partir de accepted.parquet."""
import logging
from pathlib import Path

from .config import connect, copy_parquet, data_dir, literal, monthly_path

LOG = logging.getLogger(__name__)


def run_transform(month: str, validated_path: Path) -> Path:
    """Enriquece un mes y reemplaza solamente su partición curada."""
    zones = data_dir() / "reference" / "taxi_zone_lookup.csv"
    target = monthly_path("curated", month, "part-000.parquet")
    with connect() as con:
        con.execute(f"CREATE OR REPLACE TEMP VIEW zones AS SELECT CAST(LocationID AS BIGINT) AS location_id, Borough AS borough, Zone AS zone_name, service_zone FROM read_csv_auto({literal(zones)})")
        con.execute(f"CREATE OR REPLACE TEMP VIEW accepted AS SELECT * FROM read_parquet({literal(validated_path)}, hive_partitioning=false)")
        query = """
        SELECT a.*,
               date_diff('second', a.tpep_pickup_datetime, a.tpep_dropoff_datetime) / 60.0 AS duration_minutes,
               CASE WHEN a.trip_distance > 0 AND date_diff('second', a.tpep_pickup_datetime, a.tpep_dropoff_datetime) > 0
                    THEN a.trip_distance / (date_diff('second', a.tpep_pickup_datetime, a.tpep_dropoff_datetime) / 3600.0)
               END AS average_speed_mph,
               CAST(a.tpep_pickup_datetime AS DATE) AS pickup_date,
               hour(a.tpep_pickup_datetime) AS pickup_hour,
               dayofweek(a.tpep_pickup_datetime) AS pickup_day_of_week,
               CASE WHEN a.fare_amount > 0 THEN 100.0 * a.tip_amount / a.fare_amount END AS tip_pct_fare,
               pu.borough AS pickup_borough,
               pu.zone_name AS pickup_zone,
               pu.service_zone AS pickup_service_zone,
               doo.borough AS dropoff_borough,
               doo.zone_name AS dropoff_zone,
               doo.service_zone AS dropoff_service_zone
        FROM accepted a
        LEFT JOIN zones pu ON pu.location_id = a.pu_location_id
        LEFT JOIN zones doo ON doo.location_id = a.do_location_id
        """
        copy_parquet(con, query, target)
        count = con.execute(f"SELECT count(*) FROM read_parquet({literal(target)}, hive_partitioning=false)").fetchone()[0]
    LOG.info("Curada %s: %s filas en %s", month, count, target)
    return target
