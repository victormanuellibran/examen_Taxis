"""Ejecución del modelado dbt y de las consultas de negocio.

El modo dbt consume los Parquet de la capa curada producidos por
``src.transform`` y usa un warehouse DuckDB separado del modo Python.
"""
import json
import logging
import os
import subprocess
import sys
import time
from pathlib import Path

import duckdb

from .config import ROOT, data_dir, parse_month
from .model import _markdown

LOG = logging.getLogger(__name__)
DBT_PROJECT_DIR = ROOT / "dbt_project"
SQL_DIR = ROOT / "sql"


def _dbt_vars(months: list[str]) -> dict[str, object]:
    if len(months) != 2:
        raise ValueError("El modo dbt requiere [mes_comparacion, mes_asignado]")
    comparison_month, target_month = months
    comparison_year, comparison_number = parse_month(comparison_month)
    target_year, target_number = parse_month(target_month)
    return {
        "comparison_month": comparison_month,
        "target_month": target_month,
        "comparison_year": comparison_year,
        "comparison_month_num": f"{comparison_number:02d}",
        "target_year": target_year,
        "target_month_num": f"{target_number:02d}",
    }


def _dbt_environment() -> dict[str, str]:
    root = data_dir()
    env = os.environ.copy()
    # DuckDB acepta rutas POSIX en Windows y así evitamos escapes de backslash
    # dentro de external_location de las fuentes dbt.
    data_path = str(root).replace("\\", "/")
    env["NYC_TAXI_DATA_DIR"] = data_path
    env["NYC_TAXI_DBT_WAREHOUSE"] = f"{data_path}/warehouse/nyc_taxi_dbt.duckdb"
    env["NYC_TAXI_DBT_TEMP"] = f"{data_path}/tmp"
    return env


def _run_dbt_build(months: list[str]) -> float:
    if not DBT_PROJECT_DIR.exists():
        raise FileNotFoundError(f"No existe el proyecto dbt: {DBT_PROJECT_DIR}")
    command = [
        sys.executable,
        "-m",
        "dbt.cli.main",
        "build",
        "--project-dir",
        str(DBT_PROJECT_DIR),
        "--profiles-dir",
        str(DBT_PROJECT_DIR),
        "--vars",
        json.dumps(_dbt_vars(months), separators=(",", ":")),
        "--no-partial-parse",
    ]
    LOG.info("Ejecutando dbt build para %s", months)
    started = time.perf_counter()
    result = subprocess.run(
        command,
        cwd=ROOT,
        env=_dbt_environment(),
        capture_output=True,
        text=True,
        check=False,
    )
    elapsed = time.perf_counter() - started
    if result.stdout:
        LOG.info("Salida dbt:\n%s", result.stdout.rstrip())
    if result.returncode != 0:
        detail = result.stderr.strip() or result.stdout.strip()
        raise RuntimeError(f"dbt build falló con código {result.returncode}: {detail}")
    LOG.info("dbt build finalizó correctamente en %.2f s", elapsed)
    return elapsed


def _write_business_results(months: list[str]) -> None:
    warehouse = data_dir() / "warehouse" / "nyc_taxi_dbt.duckdb"
    config = {
        "memory_limit": "2GB",
        "threads": "2",
        "temp_directory": str(data_dir() / "tmp"),
        "preserve_insertion_order": "false",
    }
    results = []
    con = duckdb.connect(str(warehouse), config=config)
    try:
        for query_file in sorted(SQL_DIR.glob("q*.sql")):
            query = query_file.read_text(encoding="utf-8")
            result = con.execute(query)
            results.append((query_file.name, query.strip(), result.description, result.fetchall()))
    finally:
        con.close()

    comparison_month, target_month = months
    output = [
        "# Resultados del modelo NYC Taxi",
        "",
        f"Motor: `dbt`; mes comparado: `{comparison_month}`; mes asignado: `{target_month}`.",
        "",
    ]
    for name, query, description, rows in results:
        headers = [item[0] for item in description]
        output.extend(
            [
                f"## {name}",
                "",
                "Consulta:",
                "```sql",
                query,
                "```",
                "",
                _markdown(headers, rows),
                "",
                "Consulta ejecutada sobre el warehouse DuckDB generado por dbt.",
                "",
            ]
        )
    output_path = ROOT / "RESULTADOS.md"
    output_path.write_text("\n".join(output), encoding="utf-8")
    LOG.info("Consultas de negocio dbt ejecutadas y resultados escritos en %s", output_path)


def run_model(months: list[str]) -> None:
    """Construye el modelo dbt y ejecuta las seis consultas del examen."""
    _run_dbt_build(months)
    _write_business_results(months)
