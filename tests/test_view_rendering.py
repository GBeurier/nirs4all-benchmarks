"""Chart views must not merge distinct rows, invert the metric direction or assume a score level (run in Node)."""

from __future__ import annotations

import json
import shutil
import subprocess
from pathlib import Path
from typing import Any

import pytest

WEB = Path(__file__).resolve().parents[1] / "src" / "nirs4all_benchmarks" / "web"

DRIVER = """
const [view, scenario] = process.argv.slice(2);
const { default: mod } = await import(`./views/${view}.js`);
const s = JSON.parse(scenario);
const drawn = [], tables = [], calls = [], texts = [], svgs = [];
const node = (tag, kids) => ({ tag, kids, textContent: "", addEventListener() {},
  querySelector(sel) { return sel === tag ? this : kids.map((k) => k?.querySelector?.(sel)).find(Boolean) ?? null; } });
globalThis.document = { getElementById: () => ({ replaceWith() {} }) };
const dom = new Proxy({
  el: (tag, _attrs, ...kids) => { for (const k of kids) if (typeof k === "string") texts.push(k); return node(tag, kids); },
  mount: () => {}, empty: (m) => { texts.push(m); return { empty: m }; }, loading: () => ({}),
  table: (columns, rows) => {
    tables.push({ columns: columns.map((c) => c.label), rows });
    for (const r of rows) for (const c of columns) c.render?.(r);
    return {};
  },
  controls: () => ({ element: {}, get: () => false }),
  svg: (tag, attrs, ...kids) => { svgs.push({ tag, attrs }); return node(tag, kids); },
  shortHash: (x) => String(x).slice(0, 8), fmt: (v) => String(v), fmtInt: (v) => String(v),
}, { get: (target, key) => target[key] ?? ((...args) => node(String(key), args)) });
const plot = { draw: (_n, traces, layout) => drawn.push({ traces, layout }), purge: () => {},
  sequential: [[0, "#000"], [1, "#fff"]], palette: ["#111"], marker: (m) => m };
const record = (name, result) => async (args) => { calls.push({ name, args }); return result(args); };
const api = {
  overview: async () => s.overview,
  datasets: async () => [],
  matrix: async (p) => (p?.by === "model" ? (s.matrixModel ?? {}) : s.matrix),
  robustness: async () => s.robustness,
  operatorEffect: async () => s.operatorEffect,
  stats: async () => s.stats,
  runs: record("runs", () => s.runs ?? []),
  leaderboard: record("leaderboard", () => ({ direction: s.direction, rows: s.leaderboard ?? [] })),
  datasetLeaderboard: record("datasetLeaderboard", () => s.datasetLeaderboard),
  pivot: record("pivot", () => ({ rows: [] })),
};
const state = { metric: s.metric, scope: s.scope ?? "test", dataset: "", _overview: s.stateOverview, save() {} };
await mod.render({ root: {}, api, dom, plot, state, navigate: () => {} });
console.log(JSON.stringify({ drawn, tables, calls, texts, svgs }));
"""


def _run(tmp_path: Path, view: str, scenario: dict[str, Any]) -> dict[str, Any]:
    node = shutil.which("node")
    if not node:
        pytest.skip("Node.js is required to run the browser views")
    major = int(subprocess.run([node, "-p", "process.versions.node.split('.')[0]"], capture_output=True, text=True, check=True).stdout)
    if major < 18:
        pytest.skip("Node.js 18 or newer is required to run the browser views")
    shutil.copytree(WEB / "views", tmp_path / "views", dirs_exist_ok=True)
    shutil.copytree(WEB / "lib", tmp_path / "lib", dirs_exist_ok=True)
    (tmp_path / "package.json").write_text('{"type": "module"}')
    (tmp_path / "driver.mjs").write_text(DRIVER, encoding="utf-8")
    result = subprocess.run([node, "driver.mjs", view, json.dumps(scenario)], cwd=tmp_path, capture_output=True, text=True, check=True, timeout=60)
    return json.loads(result.stdout)


def test_matrix_keeps_pipelines_and_datasets_with_equal_labels_apart(tmp_path: Path) -> None:
    scenario = {
        "metric": "nrmse",
        "matrix": {
            "direction": "min",
            "datasets": [{"dataset_fingerprint": "aaaaaaaa11", "label": "Leaf"}, {"dataset_fingerprint": "bbbbbbbb22", "label": "Leaf"}],
            "pipelines": [{"pipeline_dag_hash": "pppppppp11", "label": "PLS"}, {"pipeline_dag_hash": "qqqqqqqq22", "label": "PLS"}],
            "cells": [
                {"pipeline_dag_hash": "pppppppp11", "dataset_fingerprint": "aaaaaaaa11", "value": 0.1, "coverage": 1},
                {"pipeline_dag_hash": "qqqqqqqq22", "dataset_fingerprint": "bbbbbbbb22", "value": 0.9, "coverage": 1},
            ],
        },
    }
    trace = _run(tmp_path, "matrix", scenario)["drawn"][0]["traces"][0]

    assert len(set(trace["x"])) == len(set(trace["y"])) == 2
    assert trace["z"] == [[0.1, None], [None, 0.9]]


def test_model_matrix_is_drawn_below_the_pipeline_matrix(tmp_path: Path) -> None:
    datasets = [{"dataset_fingerprint": "aaaaaaaa11", "label": "Leaf"}, {"dataset_fingerprint": "bbbbbbbb22", "label": "Soil"}]
    scenario = {
        "metric": "rmse",
        "matrix": {"direction": "min", "datasets": datasets,
                   "pipelines": [{"pipeline_dag_hash": "pppppppp11", "label": "SNV > PLS"}],
                   "cells": [{"pipeline_dag_hash": "pppppppp11", "dataset_fingerprint": "aaaaaaaa11", "value": 0.2, "coverage": 1}]},
        "matrixModel": {"direction": "min", "datasets": datasets,
                        "models": [{"model": "sklearn.cross_decomposition.PLSRegression", "label": "PLSRegression"},
                                   {"model": "sklearn.linear_model.Ridge", "label": "Ridge"}],
                        "cells": [
                            {"model": "sklearn.cross_decomposition.PLSRegression", "dataset_fingerprint": "aaaaaaaa11", "value": 0.3, "n_pipelines": 2},
                            {"model": "sklearn.linear_model.Ridge", "dataset_fingerprint": "bbbbbbbb22", "value": 0.4, "n_pipelines": 1},
                        ]},
    }
    result = _run(tmp_path, "matrix", scenario)

    pipeline_trace, model_trace = (d["traces"][0] for d in result["drawn"])
    assert pipeline_trace["y"] == ["SNV > PLS"]
    assert model_trace["y"] == ["PLSRegression", "Ridge"]
    assert model_trace["z"] == [[0.3, None], [None, 0.4]]
    assert model_trace["customdata"] == [[2, None], [None, 1]]
    assert "Model \u00d7 Dataset" in result["texts"]


def _dataset_row(fingerprint: str, name: str, metric: str, direction: str, values: list[float], **extra: Any) -> dict[str, Any]:
    best = min(values) if direction == "min" else max(values)
    return {"dataset_fingerprint": fingerprint, "dataset_name": name, "task_hash": None, "task_type": extra.pop("task_type", "regression"),
            "metric": metric, "metric_unit": None, "direction": direction, "best_value": best,
            "best": {"run_condition_hash": "rc" + fingerprint, "pipeline_dag_hash": "p" + fingerprint, "pipeline_label": "SNV > PLS",
                     "main_model": "sklearn.cross_decomposition.PLSRegression", **extra.pop("best", {})},
            "n_best": 1, "tied_models": ["sklearn.cross_decomposition.PLSRegression"], "values": sorted(values),
            "n_scores": len(values), "available_metrics": [metric], **extra}


def _dataset_leaderboard(regression: list[dict[str, Any]], classification: list[dict[str, Any]], **extra: Any) -> dict[str, Any]:
    return {"mode": "valid", "scope": "cv", "partition": "validation",
            "metrics": {"regression": ["nrmse", "rmse"], "classification": ["balanced_accuracy", "accuracy"]},
            "families": {"regression": regression, "classification": classification}, "excluded": {"unclassified_targets": 0}, **extra}


def test_leaderboard_shows_one_table_per_task_family_with_a_strip_per_dataset(tmp_path: Path) -> None:
    regression = [
        _dataset_row("aaaaaaaa11", "Leaf", "nrmse", "min", [0.3, 0.1, 0.2]),
        _dataset_row("bbbbbbbb22", "Leaf", "rmse", "min", [2.0, 1.5]),
    ]
    classification = [_dataset_row("cccccccc33", "Seeds", "balanced_accuracy", "max", [0.7, 0.9, 0.8], task_type="multiclass")]
    scenario = {"stateOverview": {"score_scopes": ["cv", "test"]},
                "datasetLeaderboard": _dataset_leaderboard(regression, classification)}
    result = _run(tmp_path, "leaderboard", scenario)

    # The score level is chosen on validation, never on the retained test partition, and no dataset is preselected.
    assert [c["args"] for c in result["calls"] if c["name"] == "datasetLeaderboard"] == [{"scope": "cv", "include_quarantined": False}]
    assert [t["columns"] for t in result["tables"]] == [
        ["Dataset", "Best nRMSE", "Best model", "Score distribution"],
        ["Dataset", "Best Balanced accuracy", "Best model", "Score distribution"],
    ]
    # Distinct fingerprints with one name stay apart.
    assert len({r["label"] for r in result["tables"][0]["rows"]}) == 2
    assert {"Regression", "Classification", "RMSE"} <= set(result["texts"])
    # The best dot sits on the best score: the minimum for nRMSE, the maximum for balanced accuracy.
    best = [s["attrs"]["cx"] for s in result["svgs"] if s["tag"] == "circle"]
    ticks = [s["attrs"]["x1"] for s in result["svgs"] if s["attrs"].get("class") == "spark-tick"]
    assert best[0] == min(ticks[:3]) and best[2] == max(ticks[5:8])


def test_historical_leaderboard_keeps_the_disclaimer_and_names_the_holdout(tmp_path: Path) -> None:
    split = {"method": "seeded_80_20_group_aware", "seed": "0", "n_splits": 3}
    rows = [_dataset_row("aaaaaaaa11", "Leaf", "nrmse", "min", [0.3, 0.1], split=split, best={"execution_hash": "e" * 64})]
    scenario = {"stateOverview": {"historical_demo": True, "score_scopes": ["test"]},
                "datasetLeaderboard": _dataset_leaderboard(rows, [], mode="historical_reported", scope="test", partition="test")}
    result = _run(tmp_path, "leaderboard", scenario)
    caption = next(t for t in result["texts"] if t.startswith("Each row uses"))

    assert "Producer-reported test scores. Scientific validity is unreported." in result["texts"]
    assert "seed 0 of 3 (80/20 group-aware)" in caption and "not a validation-based selection" in caption
    assert [c["args"] for c in result["calls"] if c["name"] == "datasetLeaderboard"] == [{}]
    assert "No classification targets." in result["texts"]


def test_statistics_violins_for_same_named_datasets_do_not_share_a_name(tmp_path: Path) -> None:
    by_dataset = [
        {"dataset_fingerprint": f"fp{i}xxxxxxx", "label": "Leaf", "values": [0.1 * (i + 1)], "n": 1, "mean": 0.1, "median": 0.1, "std": 0.0}
        for i in range(2)
    ]
    stats = {"direction": "min", "summary": {"n": 2}, "values": [0.1, 0.2], "by_dataset": by_dataset, "correlations": []}
    result = _run(tmp_path, "statistics", {"metric": "nrmse", "stats": stats})

    violins = [t for d in result["drawn"] for t in d["traces"] if t["type"] == "violin"]
    assert len({v["name"] for v in violins}) == 2


@pytest.mark.parametrize("direction", ["min", "max"])
def test_operator_effect_puts_the_best_operator_at_the_top(tmp_path: Path, direction: str) -> None:
    series = [
        {"operator": f"pkg.Op{i}", "role": "model", "n": 3, "mean": mean, "median": mean, "stdev": 0.0, "values": [mean]}
        for i, mean in enumerate((0.2, 0.5, 0.9))
    ]
    best = "Op2" if direction == "max" else "Op0"
    result = _run(tmp_path, "operator-effect", {"metric": "m", "operatorEffect": {"direction": direction, "series": series}})

    names = [t["name"] for t in result["drawn"][0]["traces"]]
    # Plotly stacks the first category at the bottom, so the best operator must come last.
    assert names[-1] == best


@pytest.mark.parametrize(
    ("metric", "direction", "anchor", "pick"),
    [("rmse", "min", "left", min), ("accuracy", "max", "right", max)],
)
def test_robustness_highlights_the_better_corner_for_the_metric_direction(
    tmp_path: Path, metric: str, direction: str, anchor: str, pick: Any
) -> None:
    rows = [
        {"pipeline_dag_hash": f"h{i}", "dataset_fingerprint": "d", "label": f"p{i}", "n": 3, "mean": mean, "stdev": 0.01 * (i + 1), "cv_pct": 1.0}
        for i, mean in enumerate((0.2, 0.9))
    ]
    drawn = _run(tmp_path, "robustness", {"metric": metric, "direction": direction, "robustness": rows})["drawn"]

    annotation = drawn[0]["layout"]["annotations"][0]
    assert annotation["x"] == pick(0.2, 0.9)
    assert annotation["xanchor"] == anchor
    assert annotation["text"].startswith("low score" if direction == "min" else "high score")


@pytest.mark.parametrize(("scopes", "expected"), [(["cv"], "cv"), (["cv", "test"], "test"), (["fold", "cv"], "fold")])
def test_overview_queries_a_score_level_the_store_has(tmp_path: Path, scopes: list[str], expected: str) -> None:
    overview = {"metrics": ["rmse"], "score_scopes": scopes, "datasets": 0, "pipelines": 0, "run_conditions": 0, "valid_executions": 0,
                "executions": 0, "operators": 0, "residual_sets": 0, "score_sets": 0, "quarantined_executions": 0}
    result = _run(tmp_path, "overview", {"metric": "rmse", "overview": overview})

    assert {c["args"]["scope"] for c in result["calls"]} == {expected}


def test_run_explorer_requests_every_matching_run(tmp_path: Path) -> None:
    result = _run(tmp_path, "runs", {"metric": "rmse", "runs": []})

    # The histogram is built from the returned rows, so a small page would show a biased sample.
    assert [c["args"]["limit"] for c in result["calls"] if c["name"] == "runs"] == [100000]
