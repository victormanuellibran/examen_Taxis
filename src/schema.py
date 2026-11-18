"""Contrato estricto: sólo conversiones sin pérdida y evolución explícita."""
import logging
from pathlib import Path

import yaml

from .config import ROOT, connect, copy_parquet, data_dir, identifier, literal, monthly_path, parse_month, write_json

LOG = logging.getLogger(__name__)


def run_schema(month: str, raw_path: Path) -> Path:
    year, _ = parse_month(month)
    contract = yaml.safe_load((ROOT / "contracts/yellow_schema.yaml").read_text(encoding="utf-8"))
    columns = contract["columns"]
    with connect() as con:
        source = f"read_parquet({literal(raw_path)}, hive_partitioning=false)"
        actual = {row[0]: row[1] for row in con.execute(f"DESCRIBE SELECT * FROM {source}").fetchall()}
        missing = sorted(columns.keys() - actual.keys())
        new = sorted(actual.keys() - columns.keys())
        differences = {key: {"actual": actual[key], "expected": spec["type"]}
                       for key, spec in columns.items() if key in actual and actual[key] != spec["type"]}
        report = {"month": month, "source_file": str(raw_path), "contract_version": contract["version"],
                  "actual_schema": actual, "missing_columns": missing, "new_columns": new, "type_differences": differences}
        allowed_missing = {"cbd_congestion_fee"} if year < 2025 else set()
        unsafe = [key for key, diff in differences.items()
                  if not (diff["actual"] in {"TINYINT", "SMALLINT", "INTEGER"} and diff["expected"] == "BIGINT")]
        errors = []
        if new:
            errors.append(f"Columnas nuevas requieren actualización del contrato: {new}")
        if set(missing) - allowed_missing:
            errors.append(f"Columnas obligatorias faltantes: {sorted(set(missing) - allowed_missing)}")
        if unsafe:
            errors.append(f"Conversiones no autorizadas (posible pérdida de valores): {unsafe}")
        report["errors"] = errors
        write_json(data_dir() / "reports" / f"schema_{month}.json", report)
        LOG.info("Esquema %s: faltantes=%s nuevas=%s diferencias=%s", month, missing, new, differences)
        if errors:
            raise ValueError("; ".join(errors))
        expressions = [f"CAST({identifier(key) if key in actual else 'NULL'} AS {spec['type']}) AS {identifier(spec['name'])}"
                       for key, spec in columns.items()]
        expressions.append(f"{literal(raw_path.name)} AS source_file")
        target = monthly_path("staging", month, "normalized.parquet")
        copy_parquet(con, f"SELECT {', '.join(expressions)} FROM {source}", target)
    return target
