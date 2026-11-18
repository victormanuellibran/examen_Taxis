"""Comando único para dos períodos; integración B mediante imports diferidos."""
import argparse
import importlib
import logging

from .config import parse_month
from .ingest import run_ingest
from .quality import run_quality
from .schema import run_schema

LOG = logging.getLogger(__name__)


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description="Pipeline NYC Taxi: mes indicado y mismo mes del año anterior")
    parser.add_argument("--mes", default="2025-06")
    parser.add_argument("--hasta-calidad", action="store_true", help="Ejecutar sólo la parte A; no requiere módulos B")
    args = parser.parse_args(argv)
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    try:
        year, month = parse_month(args.mes)
        months = [f"{year - 1:04d}-{month:02d}", args.mes]
        if not args.hasta_calidad:
            try:
                transform = importlib.import_module("src.transform").run_transform
                model = importlib.import_module("src.model").run_model
            except (ModuleNotFoundError, AttributeError) as exc:
                raise RuntimeError("No se pudo cargar la integración B: src.transform.run_transform y src.model.run_model. Para probar A usar --hasta-calidad.") from exc
        for period in months:
            LOG.info("Procesando %s", period)
            accepted = run_quality(period, run_schema(period, run_ingest(period)))
            if not args.hasta_calidad:
                transform(period, accepted)
        if not args.hasta_calidad:
            model(months)
        LOG.info("Finalizó %s para %s", "la ejecución parcial hasta calidad" if args.hasta_calidad else "el pipeline completo", months)
        return 0
    except Exception:
        LOG.exception("El pipeline falló")
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
