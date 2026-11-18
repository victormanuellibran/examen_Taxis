import duckdb
import pytest

from src import model
from src.config import ROOT
from tests.integration.test_integration_pipeline import _setup_mock_ingest, pipeline


def test_q5_airports_includes_both_jfk_and_laguardia(tmp_path, monkeypatch):
    """Verifica que q5_airports.sql capture tanto JFK Airport como LaGuardia Airport (sin espacio en LaGuardia)."""
    _setup_mock_ingest(tmp_path, monkeypatch)
    monkeypatch.setattr(model, "SQL_DIR", ROOT / "sql")
    orig_write_text = model.Path.write_text
    monkeypatch.setattr(
        model.Path,
        "write_text",
        lambda self, data, *a, **kw: orig_write_text(tmp_path / "RESULTADOS.md", data, *a, **kw)
        if self.name == "RESULTADOS.md"
        else orig_write_text(self, data, *a, **kw),
    )

    assert pipeline.main(["--mes", "2025-06"]) == 0

    wh_path = tmp_path / "warehouse" / "nyc_taxi.duckdb"
    q5_sql = (ROOT / "sql" / "q5_airports.sql").read_text(encoding="utf-8")
    with duckdb.connect(str(wh_path)) as con:
        # En el fixture sintético:
        # Viaje 1: pickup=132 (JFK Airport) -> dropoff=161 (Midtown Center)
        # Viaje 2: pickup=138 (LaGuardia Airport) -> dropoff=132 (JFK Airport)
        # Actualizamos el viaje 2 a 138 -> 161 (exclusivo de LaGuardia) para verificar que LaGuardia por sí solo matchea
        con.execute("""
            UPDATE fact_trips
            SET dropoff_zone_id = 161, dropoff_zone = 'Midtown Center'
            WHERE pickup_zone = 'LaGuardia Airport'
        """)
        rows = {r[0]: r[1] for r in con.execute(q5_sql).fetchall()}
        assert rows == {"2024-06": 2, "2025-06": 2}, (
            f"q5_airports.sql omitió viajes de 'LaGuardia Airport' (obtuvo {rows}, esperaba 2 por mes). "
            "Causa: filtro LIKE '%la guardia%' con espacio en vez de '%laguardia%'."
        )
