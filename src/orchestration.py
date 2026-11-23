"""Orquestación con Dagster de las cinco etapas ya implementadas en ``src/``.

Esta capa **no contiene lógica de negocio**: cada ``@op`` es un envoltorio de
tres líneas sobre la función que ya ejecuta la etapa (``run_ingest``,
``run_schema``, ``run_quality``, ``run_transform`` y ``run_model``). Dagster
aporta únicamente el grafo de dependencias, la configuración del período, los
logs por etapa y la identificación de cuál falló.

El grafo tiene nueve nodos: cuatro etapas por cada período (el mes asignado y
el mismo mes del año anterior) más la construcción del modelo, que depende de
las capas curadas de **ambos** períodos.

    ingesta_comparacion -> esquema_comparacion -> calidad_comparacion -> transformacion_comparacion -\\
                                                                                                      >-- modelo_estrella
    ingesta_asignado    -> esquema_asignado    -> calidad_asignado    -> transformacion_asignado    -/

La ejecución es ``in_process_executor`` a propósito: un solo proceso escribe
sobre el warehouse DuckDB por vez, igual que el orquestador propio
(``src/pipeline.py``), que sigue funcionando sin cambios.
"""
import importlib
import json
from pathlib import Path

from dagster import (
    ConfigurableResource,
    Definitions,
    In,
    MetadataValue,
    OpExecutionContext,
    get_dagster_logger,
    graph,
    in_process_executor,
    op,
)

from .config import data_dir, parse_month
from .ingest import run_ingest
from .quality import run_quality
from .schema import run_schema
from .transform import run_transform

ROLES = ("comparacion", "asignado")


class PipelineParams(ConfigurableResource):
    """Parámetros de la corrida, equivalentes a ``--mes`` y ``--modelo``."""

    mes: str = "2025-06"
    modelo: str = "python"

    def periods(self) -> tuple[str, str]:
        """Devuelve ``(mes_comparacion, mes_asignado)`` validando el formato."""
        year, month = parse_month(self.mes)
        return f"{year - 1:04d}-{month:02d}", self.mes

    def month_for(self, role: str) -> str:
        comparison, target = self.periods()
        return comparison if role == "comparacion" else target

    def model_module(self) -> str:
        if self.modelo not in ("python", "dbt"):
            raise ValueError(f"modelo inválido: {self.modelo!r}; usar 'python' o 'dbt'")
        return "src.dbt_runner" if self.modelo == "dbt" else "src.model"


def _quality_metadata(month: str) -> dict:
    """Lee el reporte de calidad ya generado para mostrarlo en la interfaz."""
    report_path = data_dir() / "reports" / f"quality_{month}.json"
    if not report_path.exists():
        return {}
    report = json.loads(report_path.read_text(encoding="utf-8"))
    return {
        "filas_leidas": report["rows_read"],
        "filas_aceptadas": report["rows_accepted"],
        "filas_en_cuarentena": report["rows_quarantined"],
        "reconciliado": report["reconciled"],
        "violaciones_por_regla": MetadataValue.json(report["rule_violations"]),
    }


def _build_ingest_op(role: str):
    @op(name=f"ingesta_{role}", description="Etapa 1: descarga idempotente con control SHA-256.")
    def _ingest(context: OpExecutionContext, params: PipelineParams) -> str:
        month = params.month_for(role)
        raw_path = run_ingest(month)
        context.add_output_metadata({"mes": month, "archivo_raw": MetadataValue.path(str(raw_path))})
        return str(raw_path)

    return _ingest


def _build_schema_op(role: str):
    @op(name=f"esquema_{role}", ins={"raw_path": In(str)},
        description="Etapa 2: contrato versionado, snake_case y detección de drift.")
    def _schema(context: OpExecutionContext, raw_path: str, params: PipelineParams) -> str:
        month = params.month_for(role)
        staged_path = run_schema(month, Path(raw_path))
        report = data_dir() / "reports" / f"schema_{month}.json"
        metadata = {"mes": month, "archivo_staging": MetadataValue.path(str(staged_path))}
        if report.exists():
            parsed = json.loads(report.read_text(encoding="utf-8"))
            metadata["columnas_faltantes"] = MetadataValue.json(parsed["missing_columns"])
            metadata["columnas_nuevas"] = MetadataValue.json(parsed["new_columns"])
        context.add_output_metadata(metadata)
        return str(staged_path)

    return _schema


def _build_quality_op(role: str):
    @op(name=f"calidad_{role}", ins={"staged_path": In(str)},
        description="Etapa 3: ocho reglas, cuarentena y reconciliación de conteos.")
    def _quality(context: OpExecutionContext, staged_path: str, params: PipelineParams) -> str:
        month = params.month_for(role)
        accepted_path = run_quality(month, Path(staged_path))
        context.add_output_metadata({
            "mes": month,
            "archivo_aceptados": MetadataValue.path(str(accepted_path)),
            **_quality_metadata(month),
        })
        return str(accepted_path)

    return _quality


def _build_transform_op(role: str):
    @op(name=f"transformacion_{role}", ins={"accepted_path": In(str)},
        description="Etapa 4: columnas derivadas, enriquecimiento y partición Hive.")
    def _transform(context: OpExecutionContext, accepted_path: str, params: PipelineParams) -> str:
        month = params.month_for(role)
        curated_path = run_transform(month, Path(accepted_path))
        context.add_output_metadata({"mes": month, "particion_curada": MetadataValue.path(str(curated_path))})
        return str(curated_path)

    return _transform


@op(
    name="modelo_estrella",
    ins={"curated_comparacion": In(str), "curated_asignado": In(str)},
    description="Etapa 5: modelo estrella, agg_zone_hour y las seis consultas de negocio.",
)
def modelo_estrella(
    context: OpExecutionContext,
    curated_comparacion: str,
    curated_asignado: str,
    params: PipelineParams,
) -> None:
    """Depende de las dos particiones curadas: sin ambas no puede comparar meses."""
    months = list(params.periods())
    module = params.model_module()
    run_model = importlib.import_module(module).run_model
    context.log.info("Construyendo el modelo con %s para %s", module, months)
    run_model(months)
    context.add_output_metadata({
        "motor": params.modelo,
        "meses": MetadataValue.json(months),
        "entradas_curadas": MetadataValue.json([curated_comparacion, curated_asignado]),
        "resultados": MetadataValue.path(str(Path(__file__).resolve().parents[1] / "RESULTADOS.md")),
    })


@graph(description="Pipeline NYC Taxi: dos períodos en paralelo lógico y un modelo compartido.")
def pipeline_nyc_taxi():
    curated = {}
    for role in ROLES:
        raw = _build_ingest_op(role)()
        staged = _build_schema_op(role)(raw)
        accepted = _build_quality_op(role)(staged)
        curated[role] = _build_transform_op(role)(accepted)
    modelo_estrella(
        curated_comparacion=curated["comparacion"],
        curated_asignado=curated["asignado"],
    )


pipeline_job = pipeline_nyc_taxi.to_job(
    name="pipeline_nyc_taxi",
    description="Ejecuta las cinco etapas para el mes asignado y el mismo mes del año anterior.",
    # Un solo proceso: evita dos escritores DuckDB simultáneos y acota la RAM.
    executor_def=in_process_executor,
    resource_defs={"params": PipelineParams.configure_at_launch()},
)

defs = Definitions(jobs=[pipeline_job], resources={"params": PipelineParams.configure_at_launch()})


def run_job(mes: str = "2025-06", modelo: str = "python") -> bool:
    """Ejecuta el job en proceso; útil para pruebas y para una corrida sin interfaz."""
    get_dagster_logger().info("Ejecutando el job de Dagster para %s con modelo=%s", mes, modelo)
    result = pipeline_job.execute_in_process(
        run_config={"resources": {"params": {"config": {"mes": mes, "modelo": modelo}}}}
    )
    return result.success
