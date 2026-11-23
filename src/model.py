"""Modelo estrella, agregado horario y respuestas reproducibles del examen."""
import logging
from pathlib import Path

import duckdb

from .config import data_dir, literal, monthly_path
from .results import markdown_table, render_results

LOG = logging.getLogger(__name__)
SQL_DIR = Path(__file__).resolve().parents[1] / "sql"


_markdown = markdown_table


def run_model(months: list[str]) -> None:
    if len(months) != 2:
        raise ValueError("run_model requiere [mes_comparacion, mes_asignado]")
    comparison_month, target_month = months
    warehouse = data_dir() / "warehouse" / "nyc_taxi.duckdb"
    warehouse.parent.mkdir(parents=True, exist_ok=True)
    curated = [monthly_path("curated", month, "part-000.parquet") for month in months]
    con = duckdb.connect(str(warehouse), config={"memory_limit": "2GB", "threads": "2", "temp_directory": str(data_dir() / "tmp"), "preserve_insertion_order": "false"})
    try:
        paths = "[" + ", ".join(literal(p) for p in curated) + "]"
        zones = data_dir() / "reference" / "taxi_zone_lookup.csv"
        con.execute(f"CREATE OR REPLACE TABLE dim_zone AS SELECT CAST(LocationID AS BIGINT) AS zone_id, Borough AS borough, Zone AS zone_name, service_zone FROM read_csv_auto({literal(zones)})")
        con.execute("DROP TABLE IF EXISTS main._curated")
        con.execute(f"CREATE OR REPLACE TEMP VIEW _curated AS SELECT * FROM read_parquet({paths}, hive_partitioning=false)")
        con.execute("""CREATE OR REPLACE TABLE dim_date AS
            SELECT DISTINCT CAST(strftime(pickup_date, '%Y%m%d') AS INTEGER) AS date_key,
                   pickup_date AS date, year(pickup_date) AS year, month(pickup_date) AS month,
                   day(pickup_date) AS day, dayofweek(pickup_date) AS day_of_week
            FROM _curated WHERE pickup_date IS NOT NULL""")
        con.execute("""CREATE OR REPLACE TABLE fact_trips AS
            SELECT row_number() OVER () AS trip_id,
                   CAST(strftime(pickup_date, '%Y%m%d') AS INTEGER) AS date_key,
                   pickup_date, pickup_hour, pu_location_id AS pickup_zone_id, do_location_id AS dropoff_zone_id,
                   payment_type, passenger_count, trip_distance, duration_minutes, average_speed_mph,
                   fare_amount, tip_amount, tip_pct_fare, total_amount, cbd_congestion_fee,
                   pickup_borough, pickup_zone, dropoff_borough, dropoff_zone, source_file, warning_flags
            FROM _curated""")
        con.execute("""CREATE OR REPLACE TABLE agg_zone_hour AS
            SELECT pickup_zone_id, pickup_date, pickup_hour,
                   count(*) AS trips, sum(total_amount) AS total_revenue,
                   avg(duration_minutes) AS average_duration_minutes
            FROM fact_trips GROUP BY pickup_zone_id, pickup_date, pickup_hour""")
        con.execute("CREATE OR REPLACE TABLE analysis_parameters AS SELECT ? AS comparison_month, ? AS target_month", [comparison_month, target_month])
        results = []
        for query_file in sorted(SQL_DIR.glob("q*.sql")):
            query = query_file.read_text(encoding="utf-8")
            result = con.execute(query)
            rows = result.fetchall()
            results.append((query_file.name, query.strip(), result.description, rows))
    finally:
        con.close()
    render_results(Path(__file__).resolve().parents[1] / "RESULTADOS.md", months, "python", results)
    LOG.info("Warehouse y RESULTADOS.md generados para %s", months)
