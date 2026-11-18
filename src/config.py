"""Rutas compartidas y utilidades SQL; no depende del directorio de ejecución."""
import json
import os
import re
from pathlib import Path

import duckdb

ROOT = Path(__file__).resolve().parents[1]


def data_dir() -> Path:
    return Path(os.environ.get("NYC_TAXI_DATA_DIR", ROOT / "data")).resolve()


def parse_month(month: str) -> tuple[int, int]:
    if not re.fullmatch(r"[0-9]{4}-(0[1-9]|1[0-2])", month) or int(month[:4]) < 2:
        raise ValueError(f"Mes inválido: {month!r}; usar YYYY-MM (año >= 0002)")
    return int(month[:4]), int(month[5:])


def monthly_path(layer: str, month: str, filename: str) -> Path:
    year, number = parse_month(month)
    return data_dir() / layer / "yellow" / f"year={year:04d}" / f"month={number:02d}" / filename


def literal(value) -> str:
    return "'" + str(value).replace("'", "''") + "'"


def identifier(value: str) -> str:
    return '"' + value.replace('"', '""') + '"'


def connect():
    tmp = data_dir() / "tmp"
    tmp.mkdir(parents=True, exist_ok=True)
    return duckdb.connect(config={"memory_limit": "2GB", "threads": "2", "temp_directory": str(tmp), "preserve_insertion_order": "false"})


def write_json(path: Path, content: dict):
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(json.dumps(content, indent=2, ensure_ascii=False), encoding="utf-8")
    tmp.replace(path)


def copy_parquet(con, query: str, path: Path):
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(".parquet.tmp")
    try:
        con.execute(f"COPY ({query}) TO {literal(tmp)} (FORMAT PARQUET, COMPRESSION ZSTD)")
        tmp.replace(path)
    finally:
        tmp.unlink(missing_ok=True)
