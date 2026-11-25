import csv
import json
import socket
import urllib.request

import pytest

from src import metrics


@pytest.fixture
def fresh_run(monkeypatch, tmp_path):
    monkeypatch.setenv("NYC_TAXI_DATA_DIR", str(tmp_path))
    monkeypatch.setenv("NYC_TAXI_RUN_LOG", "1")
    monkeypatch.setattr(metrics, "_RUN", metrics._Run())
    return tmp_path


def test_cada_etapa_queda_registrada_con_tiempo_y_ram(fresh_run):
    metrics.start("terminal", "python")
    with metrics.stage("ingesta", "2025-06"):
        _ = bytearray(5 * 1024 * 1024)
    with pytest.raises(ValueError), metrics.stage("calidad", "2025-06"):
        raise ValueError("falla simulada")
    metrics.finish("error", linger_seconds=0)

    rows = list(csv.DictReader((fresh_run / "control" / "run_metrics.csv").open(encoding="utf-8")))
    assert [(r["stage"], r["status"]) for r in rows] == [("ingesta", "ok"), ("calidad", "error")]
    assert rows[0]["run_id"] == rows[1]["run_id"]
    assert float(rows[0]["peak_rss_mb"]) > 0
    assert float(rows[0]["seconds"]) >= 0


def test_exportador_publica_metricas_para_prometheus(fresh_run, monkeypatch):
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        port = sock.getsockname()[1]
    monkeypatch.setenv("NYC_TAXI_METRICS_PORT", str(port))
    (fresh_run / "reports").mkdir()
    (fresh_run / "reports" / "quality_2025-06.json").write_text(json.dumps({
        "rows_read": 10, "rows_accepted": 8, "rows_quarantined": 2, "rule_violations": {"R01": 2},
    }), encoding="utf-8")

    metrics.start("dagster", "python")
    with metrics.stage("calidad", "2025-06"):
        pass
    metrics.publish_quality("2025-06")
    body = urllib.request.urlopen(f"http://127.0.0.1:{port}/metrics", timeout=5).read().decode()
    metrics.finish("ok", linger_seconds=0)

    assert 'nyc_taxi_quality_rows{kind="cuarentena",period="2025-06"} 2.0' in body
    assert 'nyc_taxi_stage_duration_seconds{period="2025-06",stage="calidad"}' in body
    assert 'nyc_taxi_memory_budget_bytes{kind="notebook_consigna"}' in body
