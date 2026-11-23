"""Clasificación simultánea: cada fila se conserva una sola vez."""
import logging
from pathlib import Path

from .config import connect, copy_parquet, data_dir, identifier, literal, monthly_path, parse_month, write_json

LOG = logging.getLogger(__name__)
RULES = [
    {"id": "R01", "description": "Subida nula o fuera del mes", "severity": "reject", "threshold": "inicio <= pickup < próximo mes", "justification": "El período analítico lo determina la subida."},
    {"id": "R02", "description": "Bajada nula o no posterior a subida", "severity": "reject", "threshold": "dropoff > pickup", "justification": "Una duración válida debe ser positiva."},
    {"id": "R03", "description": "Distancia nula, no finita, <= 0 o > 200 millas", "severity": "reject", "threshold": "0 < trip_distance <= 200", "justification": "Límite operativo conservador para viajes urbanos/regionales; no es un límite legal."},
    {"id": "R04", "description": "Total negativo, nulo o no finito", "severity": "reject", "threshold": "total_amount >= 0 y finito", "justification": "Importes reversados o desconocidos no representan ingresos válidos para este análisis."},
    {"id": "R05", "description": "Pasajeros nulos o no positivos", "severity": "warning", "threshold": "passenger_count > 0", "justification": "Dato incompleto que no impide analizar tiempo, zona e importe."},
    {"id": "R06", "description": "Origen o destino nulo o ausente del catálogo", "severity": "reject", "threshold": "ambas zonas presentes", "justification": "Se requiere integridad referencial; 264/265 se conservan si existen en TLC."},
    {"id": "R07", "description": "Repetición exacta de todos los valores del viaje", "severity": "reject", "threshold": "una copia por conjunto de valores", "justification": "Conservar una ocurrencia y poner las restantes en cuarentena evita duplicar métricas."},
    {"id": "R08", "description": "Propina negativa", "severity": "warning", "threshold": "tip_amount < 0", "justification": "Puede representar un reverso; se señala sin excluir por sí sola."},
]


def reconcile(read: int, accepted: int, rejected: int):
    if min(read, accepted, rejected) < 0 or read != accepted + rejected:
        raise ValueError(f"Reconciliación incorrecta: leídas={read}, aceptadas={accepted}, cuarentena={rejected}")


def flag_list(rules) -> str:
    return "list_filter([" + ", ".join(f"CASE WHEN {rule['id']} THEN '{rule['id']}' END" for rule in rules) + "], x -> x IS NOT NULL)"


def run_quality(month: str, staged_path: Path) -> Path:
    year, number = parse_month(month)
    start = f"{year:04d}-{number:02d}-01"
    stop = f"{year + (number == 12):04d}-{number % 12 + 1:02d}-01"
    zones = data_dir() / "reference" / "taxi_zone_lookup.csv"
    accepted = monthly_path("validated", month, "accepted.parquet")
    rejected = monthly_path("quarantine", month, "rejected.parquet")
    with connect() as con:
        con.execute(f"CREATE VIEW staged AS SELECT * FROM read_parquet({literal(staged_path)}, hive_partitioning=false)")
        names = [row[0] for row in con.execute("DESCRIBE staged").fetchall()]
        originals = ", ".join(identifier(name) for name in names if name != "source_file")
        con.execute(f"CREATE TEMP TABLE zones AS SELECT CAST(LocationID AS BIGINT) AS id FROM read_csv_auto({literal(zones)})")
        con.execute(f"""CREATE TEMP TABLE classified AS
            WITH numbered AS (
                SELECT *, row_number() OVER (PARTITION BY {originals}) AS _occurrence FROM staged
            ), rules AS (
                SELECT *,
                (tpep_pickup_datetime IS NULL OR tpep_pickup_datetime < TIMESTAMP '{start}' OR tpep_pickup_datetime >= TIMESTAMP '{stop}') AS R01,
                (tpep_dropoff_datetime IS NULL OR tpep_pickup_datetime IS NULL OR tpep_dropoff_datetime <= tpep_pickup_datetime) AS R02,
                (trip_distance IS NULL OR NOT isfinite(trip_distance) OR trip_distance <= 0 OR trip_distance > 200) AS R03,
                (total_amount IS NULL OR NOT isfinite(total_amount) OR total_amount < 0) AS R04,
                (passenger_count IS NULL OR passenger_count <= 0) AS R05,
                (NOT EXISTS (SELECT 1 FROM zones WHERE id = pu_location_id) OR NOT EXISTS (SELECT 1 FROM zones WHERE id = do_location_id)) AS R06,
                (_occurrence > 1) AS R07,
                coalesce(tip_amount < 0, false) AS R08
                FROM numbered
            )
            SELECT * EXCLUDE (_occurrence),
                {flag_list(RULES)} AS violated_rules,
                {flag_list([r for r in RULES if r['severity'] == 'reject'])} AS rejection_flags,
                {flag_list([r for r in RULES if r['severity'] == 'warning'])} AS warning_flags
            FROM rules""")
        read, good, bad = con.execute("SELECT count(*), count(*) FILTER(WHERE len(rejection_flags)=0), count(*) FILTER(WHERE len(rejection_flags)>0) FROM classified").fetchone()
        reconcile(read, good, bad)
        counts = con.execute("SELECT " + ", ".join(f"count(*) FILTER(WHERE {r['id']})" for r in RULES) + " FROM classified").fetchone()
        exclude = ", ".join(r["id"] for r in RULES)
        copy_parquet(con, f"SELECT * EXCLUDE ({exclude}, violated_rules, rejection_flags) FROM classified WHERE len(rejection_flags)=0", accepted)
        copy_parquet(con, f"SELECT * EXCLUDE ({exclude}) FROM classified WHERE len(rejection_flags)>0", rejected)
        # Verificar también los archivos escritos, no sólo la clasificación en memoria.
        written_good = con.execute(f"SELECT count(*) FROM read_parquet({literal(accepted)}, hive_partitioning=false)").fetchone()[0]
        written_bad = con.execute(f"SELECT count(*) FROM read_parquet({literal(rejected)}, hive_partitioning=false)").fetchone()[0]
        reconcile(read, written_good, written_bad)
        if (good, bad) != (written_good, written_bad):
            raise ValueError("Conteos escritos difieren de la clasificación")
    report = {"month": month, "source_file": f"yellow_tripdata_{month}.parquet", "staged_file": str(staged_path),
              "rows_read": read, "rows_accepted": good, "rows_quarantined": bad,
              "reconciled": True, "rule_violations": dict(zip([r["id"] for r in RULES], counts)), "rules": RULES}
    write_json(data_dir() / "reports" / f"quality_{month}.json", report)
    LOG.info("Calidad %s: leídas=%s aceptadas=%s cuarentena=%s", month, read, good, bad)
    return accepted
