"""Las consultas de sql/insights/ corren sobre el modelo real que arma el pipeline."""
import duckdb

from src import insights, model
from src.config import ROOT
from tests.integration.test_integration_pipeline import _setup_mock_ingest, pipeline


def test_todas_las_consultas_de_insights_corren_sobre_el_warehouse(tmp_path, monkeypatch):
    _setup_mock_ingest(tmp_path, monkeypatch)
    monkeypatch.setattr(model, "SQL_DIR", ROOT / "sql")
    original = model.Path.write_text
    monkeypatch.setattr(
        model.Path, "write_text",
        lambda self, data, *a, **kw: original(tmp_path / self.name, data, *a, **kw)
        if self.name in ("RESULTADOS.md", "INSIGHTS.md") else original(self, data, *a, **kw),
    )
    assert pipeline.main(["--mes", "2025-06"]) == 0

    with duckdb.connect(str(tmp_path / "warehouse" / "nyc_taxi.duckdb"), read_only=True) as con:
        results = insights.run_insights(con)

    names = [name for name, *_ in results]
    assert names == sorted(p.name for p in (ROOT / "sql" / "insights").glob("i*.sql"))
    assert len(names) == 7
    for name, title, _sql, headers, _rows in results:
        assert title.startswith("I"), f"{name}: el primer comentario debe ser el título"
        assert headers, f"{name}: la consulta no devolvió columnas"
    payment = next(rows for name, *_, rows in results if name.startswith("i2"))
    for column in (4, 5):  # las participaciones de cada mes suman 100 %
        assert abs(sum(row[column] for row in payment) - 100) < 0.05

    markdown = insights.render(results, ["2024-06", "2025-06"], "python")
    assert markdown.count("## I") == 7
