import pytest


@pytest.fixture(autouse=True)
def _sin_log_de_corridas(monkeypatch):
    """Los tests no deben escribir en data/control/run_metrics.csv real."""
    monkeypatch.setenv("NYC_TAXI_RUN_LOG", "0")
    monkeypatch.delenv("NYC_TAXI_METRICS_PORT", raising=False)
