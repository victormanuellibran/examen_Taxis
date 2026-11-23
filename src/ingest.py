"""Descargas atómicas, control SHA-256 e idempotencia sobre raw inmutable."""
import csv
import hashlib
import logging
import time
from datetime import datetime, timezone
from pathlib import Path

import pyarrow.parquet as pq
import requests

from .config import data_dir, monthly_path, parse_month

LOG = logging.getLogger(__name__)
BASE = "https://d37ci6vzurychx.cloudfront.net"
FIELDS = ["filename", "url", "rows", "bytes", "sha256", "timestamp", "status", "action", "error"]


def sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def download(url: str, target: Path):
    target.parent.mkdir(parents=True, exist_ok=True)
    tmp = target.with_suffix(target.suffix + ".part")
    # Un intento inicial más hasta tres reintentos: esperas 1, 2 y 4 segundos.
    for attempt in range(4):
        try:
            with requests.get(url, stream=True, timeout=(15, 120)) as response:
                response.raise_for_status()
                with tmp.open("wb") as stream:
                    for chunk in response.iter_content(1024 * 1024):
                        stream.write(chunk)
            if target.suffix == ".parquet":
                pq.ParquetFile(tmp).metadata.num_rows
            else:
                with tmp.open(encoding="utf-8-sig", newline="") as stream:
                    reader = csv.DictReader(stream)
                    if not {"LocationID", "Borough", "Zone"}.issubset(reader.fieldnames or []):
                        raise ValueError("Tabla de zonas inválida")
                    if not list(reader):
                        raise ValueError("Tabla de zonas vacía")
            tmp.replace(target)
            return
        except Exception as exc:
            tmp.unlink(missing_ok=True)
            if not isinstance(exc, requests.RequestException) or attempt == 3:
                raise
            LOG.warning("Descarga fallida %s, reintento %s/3: %s", url, attempt + 1, exc)
            time.sleep(2 ** attempt)


def record(**values):
    path = data_dir() / "control" / "ingestion_log.csv"
    path.parent.mkdir(parents=True, exist_ok=True)
    exists = path.exists()
    with path.open("a", newline="", encoding="utf-8") as stream:
        writer = csv.DictWriter(stream, fieldnames=FIELDS)
        if not exists:
            writer.writeheader()
        writer.writerow({**values, "timestamp": datetime.now(timezone.utc).isoformat()})


def last_hash(filename: str):
    path = data_dir() / "control" / "ingestion_log.csv"
    result = None
    if path.exists():
        with path.open(newline="", encoding="utf-8") as stream:
            for row in csv.DictReader(stream):
                if row["filename"] == filename and row["status"] == "ok":
                    result = row["sha256"]
    return result


def ensure_file(url: str, path: Path) -> Path:
    details = {"filename": path.name, "url": url}
    try:
        expected = last_hash(path.name)
        if path.exists():
            actual = sha256(path)
            if expected and actual != expected:
                raise ValueError(f"SHA-256 no coincide para {path}; raw conservado, investigar antes de continuar")
            action = "reused" if expected else "adopted"
        else:
            if expected:
                LOG.warning("Archivo registrado ausente; se vuelve a descargar %s", path.name)
            download(url, path)
            actual = sha256(path)
            if expected and actual != expected:
                raise ValueError(f"Descarga difiere del hash histórico: {path}")
            action = "downloaded"
        if path.suffix == ".parquet":
            rows = pq.ParquetFile(path).metadata.num_rows
        else:
            with path.open(newline="", encoding="utf-8-sig") as stream:
                reader = csv.DictReader(stream)
                if not {"LocationID", "Borough", "Zone"}.issubset(reader.fieldnames or []):
                    raise ValueError("Tabla de zonas inválida")
                rows = sum(1 for _ in reader)
                if not rows:
                    raise ValueError("Tabla de zonas vacía")
        record(**details, rows=rows, bytes=path.stat().st_size, sha256=actual, status="ok", action=action)
        LOG.info("%s %s: %s filas, SHA-256 %s", action, path.name, rows, actual)
        return path
    except Exception as exc:
        record(**details, status="error", error=str(exc))
        raise


def run_ingest(month: str) -> Path:
    parse_month(month)
    name = f"yellow_tripdata_{month}.parquet"
    url = f"{BASE}/trip-data/{name}"
    ensure_file(f"{BASE}/misc/taxi_zone_lookup.csv", data_dir() / "reference" / "taxi_zone_lookup.csv")
    try:
        return ensure_file(url, monthly_path("raw", month, name))
    except requests.HTTPError as exc:
        status = getattr(exc.response, "status_code", None)
        if status == 404:
            raise FileNotFoundError(f"El archivo oficial del período {month} no está publicado: {url}") from exc
        raise
