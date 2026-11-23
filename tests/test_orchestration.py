"""Pruebas de la orquestación Dagster sobre datos sintéticos."""
from pathlib import Path

import pyarrow.parquet as pq
import pytest

from src import model, orchestration
from src.config import ROOT, monthly_path
from src.orchestration import PipelineParams, pipeline_job
from tests.integration.test_integration_pipeline import _setup_mock_ingest

RUN_CONFIG_2025_06 = {"resources": {"params": {"config": {"mes": "2025-06", "modelo": "python"}}}}


def _redirect_resultados(tmp_path: Path, monkeypatch):
    """Evita que una corrida de prueba sobreescriba el RESULTADOS.md del repo."""
    monkeypatch.setattr(model, "SQL_DIR", ROOT / "sql")
    original = Path.write_text

    def guarded(self: Path, data: str, *args, **kwargs):
        target = tmp_path / "RESULTADOS_test.md" if self.name == "RESULTADOS.md" else self
        return original(target, data, *args, **kwargs)

    monkeypatch.setattr(Path, "write_text", guarded)


def test_graph_has_five_stages_per_period_and_shared_model():
    """El grafo debe tener 9 nodos: 4 etapas por período más el modelo compartido."""
    graph = pipeline_job.graph
    names = {node.name for node in graph.nodes}
    assert names == {
        "ingesta_comparacion", "esquema_comparacion", "calidad_comparacion", "transformacion_comparacion",
        "ingesta_asignado", "esquema_asignado", "calidad_asignado", "transformacion_asignado",
        "modelo_estrella",
    }

    def upstream(node: str) -> set[str]:
        return {h.node_name for h in graph.dependency_structure.all_upstream_outputs_from_node(node)}

    for role in ("comparacion", "asignado"):
        assert upstream(f"ingesta_{role}") == set()
        assert upstream(f"esquema_{role}") == {f"ingesta_{role}"}
        assert upstream(f"calidad_{role}") == {f"esquema_{role}"}
        assert upstream(f"transformacion_{role}") == {f"calidad_{role}"}
    # El modelo no puede correr hasta que ambos períodos estén curados.
    assert upstream("modelo_estrella") == {"transformacion_comparacion", "transformacion_asignado"}


@pytest.mark.parametrize("mes,esperado", [
    ("2025-06", ("2024-06", "2025-06")),
    ("2025-01", ("2024-01", "2025-01")),
    ("2024-12", ("2023-12", "2024-12")),
])
def test_config_resuelve_el_par_de_periodos(mes, esperado):
    assert PipelineParams(mes=mes).periods() == esperado


def test_config_rechaza_mes_y_motor_invalidos():
    with pytest.raises(ValueError, match="Mes inválido"):
        PipelineParams(mes="2025-13").periods()
    with pytest.raises(ValueError, match="modelo inválido"):
        PipelineParams(modelo="spark").model_module()


def test_selector_de_motor_apunta_a_los_modulos_existentes():
    assert PipelineParams(modelo="python").model_module() == "src.model"
    assert PipelineParams(modelo="dbt").model_module() == "src.dbt_runner"


def test_job_ejecuta_las_cinco_etapas_sin_duplicar_resultados(tmp_path, monkeypatch):
    """Corrida completa con datos sintéticos y segunda corrida idempotente."""
    mock_dl = _setup_mock_ingest(tmp_path, monkeypatch)
    _redirect_resultados(tmp_path, monkeypatch)

    result = pipeline_job.execute_in_process(run_config=RUN_CONFIG_2025_06)
    assert result.success
    assert mock_dl.call_count == 3  # catálogo de zonas + dos mensuales

    # Las nueve etapas se ejecutaron y cada una dejó su salida.
    for role in ("comparacion", "asignado"):
        for stage in ("ingesta", "esquema", "calidad", "transformacion"):
            assert result.output_for_node(f"{stage}_{role}")
    for month in ("2024-06", "2025-06"):
        assert pq.ParquetFile(monthly_path("curated", month, "part-000.parquet")).metadata.num_rows == 2

    # Segunda corrida: reutiliza los archivos y no duplica filas curadas.
    assert pipeline_job.execute_in_process(run_config=RUN_CONFIG_2025_06).success
    assert mock_dl.call_count == 3
    for month in ("2024-06", "2025-06"):
        assert pq.ParquetFile(monthly_path("curated", month, "part-000.parquet")).metadata.num_rows == 2


def test_el_pipeline_tradicional_sigue_funcionando(tmp_path, monkeypatch):
    """Dagster es una capa adicional: `python -m src.pipeline` no cambia."""
    from src import pipeline

    _setup_mock_ingest(tmp_path, monkeypatch)
    _redirect_resultados(tmp_path, monkeypatch)
    assert pipeline.main(["--mes", "2025-06"]) == 0


def test_error_en_una_etapa_detiene_el_job_e_identifica_el_op(tmp_path, monkeypatch):
    """Un fallo en calidad debe propagarse y dejar identificada la etapa."""
    _setup_mock_ingest(tmp_path, monkeypatch)

    def exploding_quality(month, staged_path):
        raise ValueError("falla inyectada en calidad")

    monkeypatch.setattr(orchestration, "run_quality", exploding_quality)

    result = pipeline_job.execute_in_process(run_config=RUN_CONFIG_2025_06, raise_on_error=False)
    assert not result.success
    failed = {event.node_name for event in result.all_events if event.is_step_failure}
    assert failed & {"calidad_comparacion", "calidad_asignado"}
    # El modelo nunca se ejecuta si una etapa anterior falló.
    assert "modelo_estrella" not in {event.node_name for event in result.all_events if event.is_step_success}
