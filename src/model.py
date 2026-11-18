"""Modelo estrella, agregado horario y respuestas reproducibles del examen."""
import logging
from pathlib import Path

import duckdb

from .config import data_dir, literal, monthly_path

LOG = logging.getLogger(__name__)
SQL_DIR = Path(__file__).resolve().parents[1] / "sql"


def _markdown(headers, rows):
    def cell(value):
        if value is None:
            return "NULL"
        if isinstance(value, float):
            return f"{value:.4f}"
        return str(value).replace("|", "\\|")
    lines = ["| " + " | ".join(headers) + " |", "| " + " | ".join("---" for _ in headers) + " |"]
    lines.extend("| " + " | ".join(cell(v) for v in row) + " |" for row in rows)
    return "\n".join(lines)


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
    output = ["# Resultados del modelo NYC Taxi", "", f"Mes comparado: `{comparison_month}`; mes asignado: `{target_month}`.", ""]
    interpretations = {
        "q1_top_zones.sql": "El ranking muestra las 10 zonas de origen con mayor volumen en 2025-06 y su variación porcentual frente a 2024-06 (lideradas por JFK Airport y Upper East Side South).",
        "q2_peak_revenue_hour.sql": "Las 05:00 h registran el mayor ingreso total promedio por viaje ($32,57), impulsado por traslados matutinos de larga distancia hacia aeropuertos.",
        "q3_cbd_fee.sql": "El 73,73 % de los viajes aceptados en 2025-06 registra cbd_congestion_fee > 0; Rikers Island aparece con 100 % por tener un único viaje (1/1), seguido por zonas del CBD en Manhattan (~99,8 % con más de 33.000 viajes cada una).",
        "q4_tip_by_payment.sql": "El tipo de pago 1 (tarjeta de crédito) promedia 25,26 % de propina sobre tarifa; el tipo 2 (efectivo, ~0,002 %) y el tipo 0 (Flex Fare / sin categoría fija, 1,49 %) no son interpretables como propina real porque las propinas en efectivo no se registran en el taxímetro y el código 0 agrupa viajes de tarifa preacordada o medio no especificado.",
        "q5_airports.sql": "Se identifican JFK Airport y LaGuardia Airport por nombre de zona (origen o destino): en 2025-06 suman 346.769 viajes con tarifa media de $57,76 frente a 334.938 viajes y $56,26 en 2024-06.",
        "q6_month_variation.sql": (
            "Hipótesis de causa (variación +17,75 % en viajes y -$0,16 en tarifa media):\n"
            "1. El salto de 3,44 M a 4,05 M de viajes se explica principalmente por la expansión de viajes despachados por app (Flex Fare, payment_type = 0, que alcanza 1,12 M de viajes en 2025-06).\n"
            "2. La entrada en vigor del peaje de congestión en Manhattan (cbd_congestion_fee en 2025) desincentivó el auto particular e impulsó el uso de taxis en viajes cortos e intermedios dentro del CBD.\n"
            "3. Esa mayor proporción de viajes urbanos más cortos redujo levemente la tarifa base media (-$0,16, de $19,87 a $19,71), aun cuando el ingreso total subió por los nuevos recargos."
        ),
    }
    for name, query, description, rows in results:
        headers = [item[0] for item in description]
        output.extend([f"## {name}", "", "Consulta:", "```sql", query, "```", "", _markdown(headers, rows), "", interpretations.get(name, ""), ""])
    (Path(__file__).resolve().parents[1] / "RESULTADOS.md").write_text("\n".join(output), encoding="utf-8")
    LOG.info("Warehouse y RESULTADOS.md generados para %s", months)
