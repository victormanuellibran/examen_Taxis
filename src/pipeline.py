"""Comando único para dos períodos; integración B mediante imports diferidos."""
import argparse
import importlib
import logging

from . import metrics
from .config import parse_month
from .ingest import run_ingest
from .quality import run_quality
from .schema import run_schema

LOG = logging.getLogger(__name__)


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description="Pipeline NYC Taxi: mes indicado y mismo mes del año anterior")
    parser.add_argument("--mes", default="2025-06")
    parser.add_argument("--modelo", choices=("python", "dbt"), default="python", help="Motor de modelado; python es el modo compatible por defecto")
    parser.add_argument("--hasta-calidad", action="store_true", help="Ejecutar sólo la parte A; no requiere módulos B")
    args = parser.parse_args(argv)
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    try:
        year, month = parse_month(args.mes)
        months = [f"{year - 1:04d}-{month:02d}", args.mes]
        if not args.hasta_calidad:
            try:
                transform = importlib.import_module("src.transform").run_transform
                model_module = "src.dbt_runner" if args.modelo == "dbt" else "src.model"
                model = importlib.import_module(model_module).run_model
            except (ModuleNotFoundError, AttributeError) as exc:
                raise RuntimeError("No se pudo cargar la integración de transformación/modelado. Para probar A usar --hasta-calidad.") from exc
        metrics.start("terminal", args.modelo)
        for period in months:
            LOG.info("Procesando %s", period)
            with metrics.stage("ingesta", period):
                raw = run_ingest(period)
            with metrics.stage("esquema", period):
                staged = run_schema(period, raw)
            with metrics.stage("calidad", period):
                accepted = run_quality(period, staged)
            metrics.publish_quality(period)
            if not args.hasta_calidad:
                with metrics.stage("transformacion", period):
                    transform(period, accepted)
        if not args.hasta_calidad:
            with metrics.stage("modelo"):
                model(months)
        LOG.info("Finalizó %s para %s con modelo=%s", "la ejecución parcial hasta calidad" if args.hasta_calidad else "el pipeline completo", months, args.modelo)
        metrics.finish("ok")
        return 0
    except Exception:
        LOG.exception("El pipeline falló")
        metrics.finish("error")
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
