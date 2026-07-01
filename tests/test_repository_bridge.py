"""nirs4all-repository consumer bridge."""

from __future__ import annotations

import importlib
import types
from pathlib import Path

import pytest

from nirs4all_benchmarks.ingestion.repository import list_repository_pipelines, register_repository_pipeline
from nirs4all_benchmarks.store import ArenaStore


class FakeRepositoryPipeline:
    descriptor = types.SimpleNamespace(name="Repo PLS")

    def recipe(self):
        return {
            "steps": [
                {"class": "nirs4all.transform.SNV"},
                {"model": "sklearn.cross_decomposition.PLSRegression", "params": {"n_components": 6}},
            ],
        }


def test_register_repository_pipeline_registers_recipe_only(monkeypatch: pytest.MonkeyPatch, tmp_path: Path):
    calls: list[dict[str, object]] = []

    def fake_get(name: str, **kwargs: object) -> FakeRepositoryPipeline:
        calls.append({"name": name, **kwargs})
        return FakeRepositoryPipeline()

    fake_repository = types.SimpleNamespace(get=fake_get)
    monkeypatch.setattr(importlib, "import_module", lambda name: fake_repository if name == "nirs4all_repository" else None)

    with ArenaStore(tmp_path / "arena") as store:
        result = register_repository_pipeline(
            store,
            "baseline-pls",
            collection_id="repo",
            target_datasets=["corn"],
            repository_root=tmp_path / "repository",
            cache_dir=tmp_path / "cache",
            verify=False,
        )

        planned = store.query_one("SELECT * FROM planned_runs")
        pipeline = store.query_one("SELECT * FROM pipeline_dags")

        assert result.kind == "pipeline"
        assert result.status == "registered"
        assert result.datasets[0]["status"] == "planned"
        assert store.count("executions") == 0
        assert planned is not None
        assert planned["source"] == "nirs4all-repository:baseline-pls"
        assert pipeline is not None
        assert pipeline["human_label"] == "Repo PLS"

    assert calls == [
        {
            "name": "baseline-pls",
            "root": tmp_path / "repository",
            "cache_dir": tmp_path / "cache",
            "verify": False,
            "with_artifacts": False,
        }
    ]


def test_list_repository_pipelines_is_lazy(monkeypatch: pytest.MonkeyPatch, tmp_path: Path):
    calls: list[dict[str, object]] = []

    def fake_list(**kwargs: object) -> list[str]:
        calls.append(kwargs)
        return ["baseline-pls"]

    fake_repository = types.SimpleNamespace(list=fake_list)
    monkeypatch.setattr(importlib, "import_module", lambda name: fake_repository if name == "nirs4all_repository" else None)

    result = list_repository_pipelines(repository_root=tmp_path / "repository", framework="nirs4all")

    assert result == ["baseline-pls"]
    assert calls == [
        {
            "framework": "nirs4all",
            "task": None,
            "tag": None,
            "kind": None,
            "trust": None,
            "root": tmp_path / "repository",
        }
    ]


def test_register_repository_pipeline_errors_when_optional_dependency_missing(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
):
    def fake_import_module(name: str):
        raise ModuleNotFoundError(name=name)

    monkeypatch.setattr(importlib, "import_module", fake_import_module)

    with (
        ArenaStore(tmp_path / "arena") as store,
        pytest.raises(ImportError, match="nirs4all-repository support requires"),
    ):
        register_repository_pipeline(store, "baseline-pls")
