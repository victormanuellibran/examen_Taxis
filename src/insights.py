"""Consultas de insights (sql/insights/) sobre el warehouse ya construido.

No forma parte de las cinco etapas: se ejecuta después del pipeline y sólo lee.
Genera INSIGHTS.md con cada consulta y su tabla de resultados; la interpretación
profunda está en docs/ANALISIS_RESULTADOS.md.

    python -m src.insights                 # warehouse del modo python
    python -m src.insights --modelo dbt    # warehouse del modo dbt
"""
import argparse
import logging
from pathlib import Path

import duckdb

from .config import data_dir
from .results import markdown_table

LOG = logging.getLogger(__name__)
ROOT = Path(__file__).resolve().parents[1]
INSIGHTS_DIR = ROOT / "sql" / "insights"
WAREHOUSES = {"python": "nyc_taxi.duckdb", "dbt": "nyc_taxi_dbt.duckdb"}


def warehouse_path(modelo: str = "python") -> Path:
    return data_dir() / "warehouse" / WAREHOUSES[modelo]


def query_title(sql: str, fallback: str) -> str:
    """El título es el primer comentario de la consulta (``-- I1. ...``)."""
    for line in sql.splitlines():
        if line.startswith("--"):
            return line.lstrip("- ").strip()
    return fallback


def run_insights(con) -> list[tuple[str, str, str, list[str], list[tuple]]]:
    results = []
    for query_file in sorted(INSIGHTS_DIR.glob("i*.sql")):
        sql = query_file.read_text(encoding="utf-8")
        cursor = con.execute(sql)
        headers = [item[0] for item in cursor.description]
        results.append((query_file.name, query_title(sql, query_file.stem), sql.strip(), headers, cursor.fetchall()))
    return results


def render(results, months, modelo: str) -> str:
    comparison, target = months
    lines = [
        "# Insights adicionales NYC Taxi",
        "",
        f"Motor: `{modelo}`; mes comparado: `{comparison}`; mes asignado: `{target}`.",
        "",
        "Consultas extra sobre el mismo modelo estrella (no reemplazan a las seis de `RESULTADOS.md`).",
        "La interpretación de cada una está en [`docs/ANALISIS_RESULTADOS.md`](docs/ANALISIS_RESULTADOS.md).",
        "",
    ]
    for name, title, sql, headers, rows in results:
        lines += [f"## {title}", "", f"Archivo: [`sql/insights/{name}`](sql/insights/{name})", "",
                  "<details><summary>Consulta SQL</summary>", "", "```sql", sql, "```", "", "</details>", "",
                  markdown_table(headers, rows), ""]
    return "\n".join(lines)


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description="Genera INSIGHTS.md leyendo el warehouse (sólo lectura)")
    parser.add_argument("--modelo", choices=tuple(WAREHOUSES), default="python")
    args = parser.parse_args(argv)
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    path = warehouse_path(args.modelo)
    if not path.exists():
        LOG.error("No existe %s; correr antes el pipeline", path)
        return 1
    con = duckdb.connect(str(path), read_only=True)
    try:
        months = list(con.execute("SELECT comparison_month, target_month FROM analysis_parameters").fetchone())
        results = run_insights(con)
    finally:
        con.close()
    (ROOT / "INSIGHTS.md").write_text(render(results, months, args.modelo), encoding="utf-8")
    LOG.info("INSIGHTS.md generado con %s consultas para %s", len(results), months)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
