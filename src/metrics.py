"""Métricas de ejecución: tiempo y pico de RAM por etapa, más un exportador Prometheus opcional.

Esta capa **no cambia ningún resultado del pipeline**: sólo observa.

- Siempre registra una fila por etapa en ``data/control/run_metrics.csv``
  (duración en segundos y pico de memoria RSS del proceso). Es la evidencia de
  "tiempo de ejecución y pico de RAM" que pide la consigna, y la lee el dashboard.
- Si existe la variable ``NYC_TAXI_METRICS_PORT`` (por ejemplo ``9108``), además
  publica las métricas en ``http://<host>:<puerto>/metrics`` para que Prometheus
  las recolecte cada segundo y Grafana las grafique en tiempo real. Funciona
  igual si el pipeline se lanza por terminal, por Dagster o dentro de Docker.

La memoria se mide con ``psutil`` sobre el propio proceso, que incluye a DuckDB
porque DuckDB corre embebido en el mismo proceso de Python.
"""
from __future__ import annotations

import csv
import json
import logging
import os
import threading
import time
import uuid
from contextlib import contextmanager
from datetime import datetime, timezone

import psutil

from .config import data_dir

LOG = logging.getLogger(__name__)
PORT_ENV = "NYC_TAXI_METRICS_PORT"
RUN_LOG_ENV = "NYC_TAXI_RUN_LOG"  # "0" desactiva el CSV (lo usan los tests)
FIELDS = ["run_id", "started_at", "entrypoint", "modelo", "period", "stage", "seconds", "peak_rss_mb", "status"]
SAMPLE_SECONDS = 0.5
GIB = 1024 ** 3
# Referencias que Grafana dibuja como líneas horizontales.
MEMORY_BUDGETS = {"duckdb_memory_limit": 2 * GIB, "contenedor_docker": 4 * GIB, "notebook_consigna": 8 * GIB}


class _Run:
    """Estado de la corrida actual dentro de este proceso."""

    def __init__(self):
        self.lock = threading.Lock()
        self.run_id = None
        self.entrypoint = None
        self.modelo = None
        self.started_at = None
        self.stage_peak = 0
        self.run_peak = 0
        self.sampler = None
        self.exporter = None


_RUN = _Run()


class _Exporter:
    """Gauges de Prometheus; sólo se crean si se pidió el puerto."""

    def __init__(self, port: int):
        from prometheus_client import CollectorRegistry, Gauge, start_http_server

        self.registry = CollectorRegistry()
        labels_stage = ["stage", "period"]
        self.up = Gauge("nyc_taxi_pipeline_running", "1 mientras el pipeline está ejecutando", ["entrypoint", "modelo"], registry=self.registry)
        self.memory = Gauge("nyc_taxi_process_memory_bytes", "Memoria RSS del proceso del pipeline (incluye DuckDB)", registry=self.registry)
        self.peak = Gauge("nyc_taxi_process_memory_peak_bytes", "Pico de RSS desde que empezó la corrida", registry=self.registry)
        self.cpu = Gauge("nyc_taxi_process_cpu_percent", "CPU del proceso; 100 = un núcleo completo", registry=self.registry)
        self.threads = Gauge("nyc_taxi_process_threads", "Hilos del proceso", registry=self.registry)
        self.stage = Gauge("nyc_taxi_stage_active", "1 para la etapa que se está ejecutando", labels_stage, registry=self.registry)
        self.duration = Gauge("nyc_taxi_stage_duration_seconds", "Duración de cada etapa terminada", labels_stage, registry=self.registry)
        self.stage_peak = Gauge("nyc_taxi_stage_memory_peak_bytes", "Pico de RSS de cada etapa terminada", labels_stage, registry=self.registry)
        self.rows = Gauge("nyc_taxi_quality_rows", "Filas del reporte de calidad", ["period", "kind"], registry=self.registry)
        self.rule = Gauge("nyc_taxi_quality_rule_violations", "Filas que violan cada regla", ["period", "rule"], registry=self.registry)
        budget = Gauge("nyc_taxi_memory_budget_bytes", "Límites de memoria de referencia", ["kind"], registry=self.registry)
        for kind, value in MEMORY_BUDGETS.items():
            budget.labels(kind).set(value)
        # Local: 127.0.0.1 (Docker Desktop llega por host.docker.internal sin abrir el firewall).
        # Dentro del contenedor, docker-compose usa 0.0.0.0 para que Prometheus lo alcance por la red interna.
        start_http_server(port, addr=os.environ.get("NYC_TAXI_METRICS_ADDR", "127.0.0.1"), registry=self.registry)


def _sample_forever(process: psutil.Process, stop: threading.Event):
    process.cpu_percent(None)  # la primera lectura de psutil siempre es 0
    while not stop.wait(SAMPLE_SECONDS):
        try:
            rss = process.memory_info().rss
            cpu = process.cpu_percent(None)
            threads = process.num_threads()
        except psutil.Error:
            continue
        with _RUN.lock:
            _RUN.stage_peak = max(_RUN.stage_peak, rss)
            _RUN.run_peak = max(_RUN.run_peak, rss)
            peak = _RUN.run_peak
        if _RUN.exporter:
            _RUN.exporter.memory.set(rss)
            _RUN.exporter.peak.set(peak)
            _RUN.exporter.cpu.set(cpu)
            _RUN.exporter.threads.set(threads)


def start(entrypoint: str, modelo: str) -> str:
    """Inicia la medición de una corrida. Es idempotente dentro del proceso."""
    if _RUN.run_id is None or _RUN.entrypoint != entrypoint:
        _RUN.run_id = uuid.uuid4().hex[:8]
        _RUN.entrypoint = entrypoint
        _RUN.started_at = datetime.now(timezone.utc).isoformat(timespec="seconds")
        _RUN.run_peak = 0
    _RUN.modelo = modelo
    port = os.environ.get(PORT_ENV)
    if port and _RUN.exporter is None:
        try:
            _RUN.exporter = _Exporter(int(port))
            LOG.info("Métricas en vivo publicadas en http://localhost:%s/metrics", port)
        except (OSError, ValueError, ImportError) as exc:  # nunca frenar el pipeline por el monitoreo
            LOG.warning("No se pudo publicar métricas en el puerto %s: %s", port, exc)
    if _RUN.exporter:
        _RUN.exporter.up.labels(entrypoint, modelo).set(1)
    if _RUN.sampler is None:
        stop = threading.Event()
        thread = threading.Thread(target=_sample_forever, args=(psutil.Process(), stop), daemon=True, name="nyc-taxi-metrics")
        thread.start()
        _RUN.sampler = (thread, stop)
    return _RUN.run_id


def _current_rss() -> int:
    try:
        return psutil.Process().memory_info().rss
    except psutil.Error:
        return 0


def _write_row(row: dict):
    if os.environ.get(RUN_LOG_ENV) == "0":
        return
    path = data_dir() / "control" / "run_metrics.csv"
    path.parent.mkdir(parents=True, exist_ok=True)
    new = not path.exists()
    with path.open("a", newline="", encoding="utf-8") as stream:
        writer = csv.DictWriter(stream, fieldnames=FIELDS)
        if new:
            writer.writeheader()
        writer.writerow(row)


@contextmanager
def stage(name: str, period: str = "ambos"):
    """Mide una etapa: duración, pico de RAM y estado (ok o error)."""
    if _RUN.run_id is None:
        start("desconocido", "python")
    with _RUN.lock:
        _RUN.stage_peak = _current_rss()
    if _RUN.exporter:
        _RUN.exporter.stage.labels(name, period).set(1)
    began = time.perf_counter()
    status = "ok"
    try:
        yield
    except BaseException:
        status = "error"
        raise
    finally:
        seconds = time.perf_counter() - began
        with _RUN.lock:
            peak = max(_RUN.stage_peak, _current_rss())
        if _RUN.exporter:
            _RUN.exporter.stage.labels(name, period).set(0)
            _RUN.exporter.duration.labels(name, period).set(seconds)
            _RUN.exporter.stage_peak.labels(name, period).set(peak)
        _write_row({
            "run_id": _RUN.run_id, "started_at": _RUN.started_at, "entrypoint": _RUN.entrypoint,
            "modelo": _RUN.modelo, "period": period, "stage": name, "seconds": round(seconds, 2),
            "peak_rss_mb": round(peak / 1024 ** 2, 1), "status": status,
        })


def publish_quality(period: str):
    """Pasa el reporte de calidad ya escrito a Grafana (no recalcula nada)."""
    if not _RUN.exporter:
        return
    report_path = data_dir() / "reports" / f"quality_{period}.json"
    if not report_path.exists():
        return
    report = json.loads(report_path.read_text(encoding="utf-8"))
    for kind, key in (("leidas", "rows_read"), ("aceptadas", "rows_accepted"), ("cuarentena", "rows_quarantined")):
        _RUN.exporter.rows.labels(period, kind).set(report[key])
    for rule, count in report["rule_violations"].items():
        _RUN.exporter.rule.labels(period, rule).set(count)


def finish(status: str = "ok", linger_seconds: float | None = None):
    """Cierra la corrida. Con exportador activo espera unos segundos para el último scrape."""
    if _RUN.exporter:
        _RUN.exporter.up.labels(_RUN.entrypoint, _RUN.modelo).set(0)
        wait = float(os.environ.get("NYC_TAXI_METRICS_LINGER", "5")) if linger_seconds is None else linger_seconds
        if wait > 0:
            time.sleep(wait)
    LOG.info("Corrida %s terminada (%s); pico de RAM del proceso: %.0f MB", _RUN.run_id, status, _RUN.run_peak / 1024 ** 2)
    _RUN.run_id = None
