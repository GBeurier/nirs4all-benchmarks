"""Performance comparison harness smoke test."""

from __future__ import annotations

import json
import math
from pathlib import Path
from typing import Any

import pytest
from typer.testing import CliRunner

from nirs4all_benchmarks import performance_compare as pc
from nirs4all_benchmarks.cli import app


def _install_fake_perf_children(monkeypatch: pytest.MonkeyPatch, calls: list[dict[str, Any]]) -> None:
    def choose_child_python(
        explicit: str | Path | None = None,
        *,
        require_studio: bool = True,
    ) -> Path:
        calls.append({"kind": "choose_child_python", "explicit": explicit, "require_studio": require_studio})
        return Path("/fake/python")

    def choose_nirs4all_root(python: Path) -> Path:
        calls.append({"kind": "choose_nirs4all_root", "python": python})
        return Path("/fake/nirs4all")

    def run_child(
        *,
        suite: str,
        engine: str,
        python: Path,
        nirs4all_root: Path,
        warmups: int,
    ) -> dict[str, Any]:
        calls.append(
            {
                "kind": "run_child",
                "suite": suite,
                "engine": engine,
                "python": python,
                "nirs4all_root": nirs4all_root,
                "warmups": warmups,
            }
        )
        base = 1.0 if suite == "python_run" else 2.0
        multiplier = 1.2 if engine == "dag-ml" else 1.0
        run_s = base * multiplier
        return {
            "engine_recorded": engine,
            "best_score": 0.91,
            "num_predictions": 40 if suite == "python_run" else None,
            "variants_tested": 1 if suite == "studio_run" else None,
            "runtime_source": "rt_result" if suite == "studio_run" else None,
            "import_s": 0.01,
            "run_s": run_s,
            "total_s": run_s + 0.1,
        }

    monkeypatch.setattr(pc, "choose_child_python", choose_child_python)
    monkeypatch.setattr(pc, "choose_nirs4all_root", choose_nirs4all_root)
    monkeypatch.setattr(pc, "_run_child", run_child)


def _sample_report() -> dict[str, Any]:
    return {
        "case": {"name": "seeded_small_pls_cv"},
        "environment": {"repeats": 1, "warmups": 0},
        "suites": {
            "python_run": {
                "label": "nirs4all.run() direct",
                "engines": {
                    "legacy": {
                        "engine": "legacy",
                        "engine_recorded": "legacy",
                        "run_s_median": 1.0,
                        "total_s_median": 1.1,
                        "import_s_median": 0.01,
                        "best_score": 0.91,
                    },
                    "dag-ml": {
                        "engine": "dag-ml",
                        "engine_recorded": "dag-ml",
                        "run_s_median": 1.2,
                        "total_s_median": 1.3,
                        "import_s_median": 0.01,
                        "best_score": 0.91,
                    },
                },
                "ratios": {
                    "run_s_dag_ml_over_legacy": 1.2,
                    "total_s_dag_ml_over_legacy": 1.1818181818,
                },
            }
        },
    }


def test_child_source_uses_one_strict_workload_for_both_surfaces():
    source = pc._child_source(Path("/fake/nirs4all"))

    assert source.count("def _build_case():") == 1
    assert "pipeline, dataset = _build_case()" in source
    assert "runtime_pipeline, dataset = _build_case()" in source
    assert "allow_fallback=False" in source
    assert '"allow_fallback": False' in source


def test_run_comparison_reports_both_surfaces(monkeypatch: pytest.MonkeyPatch):
    calls: list[dict[str, Any]] = []
    _install_fake_perf_children(monkeypatch, calls)

    report = pc.run_comparison(repeats=1, warmups=0)

    assert report["case"]["name"] == "seeded_small_pls_cv"
    assert set(report["suites"]) == {"python_run", "studio_run"}
    assert calls[0] == {"kind": "choose_child_python", "explicit": None, "require_studio": True}
    assert calls[1] == {"kind": "choose_nirs4all_root", "python": Path("/fake/python")}
    assert [
        (call["suite"], call["engine"], call["warmups"])
        for call in calls
        if call["kind"] == "run_child"
    ] == [
        ("python_run", "legacy", 0),
        ("python_run", "dag-ml", 0),
        ("studio_run", "legacy", 0),
        ("studio_run", "dag-ml", 0),
    ]

    for suite_name, suite in report["suites"].items():
        assert suite["label"]
        legacy = suite["engines"]["legacy"]
        dagml = suite["engines"]["dag-ml"]
        assert "error" not in legacy, f"{suite_name} legacy failed: {legacy.get('error')}"
        assert "error" not in dagml, f"{suite_name} dag-ml failed: {dagml.get('error')}"
        assert legacy["engine_recorded"] == "legacy"
        assert dagml["engine_recorded"] == "dag-ml"
        ratio = suite["ratios"]["run_s_dag_ml_over_legacy"]
        assert isinstance(ratio, float)
        assert math.isfinite(ratio)
        assert ratio > 0

    markdown = pc.render_markdown(report)
    assert "nirs4all.run() direct" in markdown
    assert "Studio pipeline job worker" in markdown
    assert "dag-ml/legacy run ratio" in markdown


def test_run_comparison_enforces_max_ratio(monkeypatch: pytest.MonkeyPatch):
    calls: list[dict[str, Any]] = []
    _install_fake_perf_children(monkeypatch, calls)

    with pytest.raises(RuntimeError, match="performance ratio gate failed: python_run"):
        pc.run_comparison(
            suites=("python_run",),
            repeats=1,
            warmups=0,
            max_ratios={"python_run": 1.1},
        )


def test_perf_compare_cli_writes_json_and_markdown(monkeypatch: pytest.MonkeyPatch, tmp_path: Path):
    report = _sample_report()
    seen: dict[str, Any] = {}

    def run_comparison(**kwargs: Any) -> dict[str, Any]:
        seen.update(kwargs)
        return report

    monkeypatch.setattr(pc, "run_comparison", run_comparison)

    json_out = tmp_path / "perf.json"
    markdown_out = tmp_path / "perf.md"
    result = CliRunner().invoke(
        app,
        [
            "perf-compare",
            "--suite",
            "python_run",
            "--repeats",
            "1",
            "--warmups",
            "0",
            "--json-out",
            str(json_out),
            "--markdown-out",
            str(markdown_out),
            "--assert-max-ratio",
            "python_run=1.5",
        ],
    )

    assert result.exit_code == 0, result.output
    assert seen["suites"] == ["python_run"]
    assert seen["repeats"] == 1
    assert seen["warmups"] == 0
    assert seen["max_ratios"] == {"python_run": 1.5}
    assert json.loads(json_out.read_text(encoding="utf-8")) == report
    markdown = markdown_out.read_text(encoding="utf-8")
    assert "nirs4all.run() direct" in markdown
    assert "dag-ml/legacy run ratio" in markdown
