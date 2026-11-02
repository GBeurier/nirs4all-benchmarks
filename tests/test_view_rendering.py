"""Chart views must not merge distinct rows or invert the metric direction (run in Node with stub DOM helpers)."""

from __future__ import annotations

import json
import shutil
import subprocess
from pathlib import Path

import pytest

VIEWS = Path(__file__).resolve().parents[1] / "src" / "nirs4all_benchmarks" / "web" / "views"

DRIVER = """
const [view, scenario] = process.argv.slice(2);
const { default: mod } = await import(`./${view}.mjs`);
const drawn = [];
const node = (tag, kids) => ({ tag, kids, textContent: "",
  querySelector(sel) { return sel === tag ? this : kids.map((k) => k?.querySelector?.(sel)).find(Boolean) ?? null; } });
globalThis.document = { getElementById: () => ({}) };
const dom = {
  el: (tag, _attrs, ...kids) => node(tag, kids),
  mount: () => {}, empty: (m) => ({ empty: m }), loading: () => ({}), table: () => ({}),
  controls: () => ({ element: {}, get: () => false }),
  shortHash: (s) => String(s).slice(0, 8), fmt: (v) => String(v),
};
const plot = { draw: (_n, traces, layout) => drawn.push({ traces, layout }), purge: () => {},
  sequential: [[0, "#000"], [1, "#fff"]], palette: ["#111"], marker: (m) => m };
const s = JSON.parse(scenario);
const api = {
  matrix: async () => s.matrix,
  robustness: async () => s.robustness,
  leaderboard: async () => ({ direction: s.direction, rows: [] }),
};
await mod.render({ root: {}, api, dom, plot, state: { metric: s.metric, scope: "test", dataset: "" }, navigate: () => {} });
console.log(JSON.stringify(drawn));
"""


def _run(tmp_path: Path, view: str, scenario: dict) -> list[dict]:
    node = shutil.which("node")
    if not node:
        pytest.skip("Node.js is required to run the browser views")
    major = int(subprocess.run([node, "-p", "process.versions.node.split('.')[0]"], capture_output=True, text=True, check=True).stdout)
    if major < 18:
        pytest.skip("Node.js 18 or newer is required to run the browser views")
    (tmp_path / f"{view}.mjs").write_text((VIEWS / f"{view}.js").read_text(encoding="utf-8"), encoding="utf-8")
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
    trace = _run(tmp_path, "matrix", scenario)[0]["traces"][0]

    assert len(set(trace["x"])) == len(set(trace["y"])) == 2
    assert trace["z"] == [[0.1, None], [None, 0.9]]


@pytest.mark.parametrize(
    ("metric", "direction", "anchor", "pick"),
    [("rmse", "min", "left", min), ("accuracy", "max", "right", max)],
)
def test_robustness_highlights_the_better_corner_for_the_metric_direction(
    tmp_path: Path, metric: str, direction: str, anchor: str, pick
) -> None:
    rows = [
        {"pipeline_dag_hash": f"h{i}", "dataset_fingerprint": "d", "label": f"p{i}", "n": 3, "mean": mean, "stdev": 0.01 * (i + 1), "cv_pct": 1.0}
        for i, mean in enumerate((0.2, 0.9))
    ]
    drawn = _run(tmp_path, "robustness", {"metric": metric, "direction": direction, "robustness": rows})

    annotation = drawn[0]["layout"]["annotations"][0]
    assert annotation["x"] == pick(0.2, 0.9)
    assert annotation["xanchor"] == anchor
    assert annotation["text"].startswith("low score" if direction == "min" else "high score")
