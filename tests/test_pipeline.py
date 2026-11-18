from pathlib import Path
from types import SimpleNamespace

from src import pipeline


def test_full_orchestration_contract(monkeypatch):
    calls = []
    def stage(name):
        def run(month, *args):
            calls.append((name, month, args))
            return Path(name + month)
        return run
    for name in ["ingest", "schema", "quality"]:
        monkeypatch.setattr(pipeline, "run_" + name, stage(name))
    modules = {
        "src.transform": SimpleNamespace(run_transform=stage("transform")),
        "src.model": SimpleNamespace(run_model=lambda months: calls.append(("model", months))),
    }
    monkeypatch.setattr(pipeline.importlib, "import_module", modules.__getitem__)
    assert pipeline.main(["--mes", "2025-06"]) == 0
    assert [call[0] for call in calls] == ["ingest", "schema", "quality", "transform"] * 2 + ["model"]
    assert calls[2][2] == (Path("schema2024-06"),)
    assert calls[3][2] == (Path("quality2024-06"),)
    assert calls[-1] == ("model", ["2024-06", "2025-06"])


def test_pipeline_propagates_failure(monkeypatch):
    def fail(month):
        raise ValueError("test failure")
    monkeypatch.setattr(pipeline, "run_ingest", fail)
    assert pipeline.main(["--hasta-calidad"]) == 1


def test_missing_integration_returns_nonzero(monkeypatch):
    def missing(name):
        raise ModuleNotFoundError(name)
    monkeypatch.setattr(pipeline.importlib, "import_module", missing)
    assert pipeline.main([]) == 1
