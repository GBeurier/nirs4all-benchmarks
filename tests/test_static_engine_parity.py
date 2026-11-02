"""The browser engine must answer aggregation queries exactly like the Python ``Queries`` facade."""

from __future__ import annotations

import gzip
import hashlib
import itertools
import json
import shutil
import subprocess
from pathlib import Path

import pytest

from nirs4all_benchmarks.history import ingest_history
from nirs4all_benchmarks.site_build import build_site
from nirs4all_benchmarks.store import ArenaStore, Queries

ROOT = Path(__file__).resolve().parents[1]
PACK = ROOT / "data" / "catalog-history-2026-09"
ENGINE = ROOT / "src" / "nirs4all_benchmarks" / "web" / "lib" / "static-engine.js"

DRIVER = """
import { readFileSync } from "node:fs";
import { createStaticApi } from "./engine.mjs";

const [bundlePath, queriesJson] = process.argv.slice(2);
globalThis.fetch = async () => ({ ok: true, json: async () => JSON.parse(readFileSync(bundlePath, "utf8")) });
const api = await createStaticApi("/data");
const out = [];
for (const q of JSON.parse(queriesJson)) {
  const { dataset, ...rest } = q.args;
  const args = { ...rest, dataset: dataset || undefined };
  out.push(q.name === "composition" ? await api.composition(args) : await api.graph({ ...args, kind: "operators" }));
}
console.log(JSON.stringify(out));
"""


def _node() -> str:
    node = shutil.which("node")
    if not node:
        pytest.skip("Node.js is required to run the browser engine")
    major = int(subprocess.run([node, "-p", "process.versions.node.split('.')[0]"], capture_output=True, text=True, check=True).stdout)
    if major < 18:
        pytest.skip("Node.js 18 or newer is required to run the browser engine")
    return node


def _uneven_pack(root: Path) -> None:
    """A slice of the real pack with execution counts that differ between pipelines and datasets."""
    with gzip.open(PACK / "runs.jsonl.gz", "rt") as stream:
        lines = list(itertools.islice(stream, 900))
    lines = [line for index, line in enumerate(lines) if index % 5 != 4 and not (index % 11 == 0 and index > 300)]
    archive = root / "runs.jsonl.gz"
    with gzip.open(archive, "wt") as stream:
        stream.writelines(lines)
    metadata = json.loads((PACK / "manifest.json").read_text())
    metadata.update(executions=len(lines), archive_sha256=hashlib.sha256(archive.read_bytes()).hexdigest())
    (root / "manifest.json").write_text(json.dumps(metadata))


def test_composition_and_operator_graph_match_python(tmp_path: Path) -> None:
    node = _node()
    pack = tmp_path / "pack"
    pack.mkdir()
    _uneven_pack(pack)
    with ArenaStore(tmp_path / "store") as store:
        ingest_history(store, pack)
        queries = Queries(store)
        datasets = [d["dataset_fingerprint"] for d in queries.datasets()]
        assert len(datasets) >= 2
        cases = [
            {"metric": metric, "scope": "test", "dataset": dataset}
            for metric in ("rmse", "nrmse")
            for dataset in ("", datasets[0], datasets[1])
        ]
        expected_composition = [
            queries.composition(metric=c["metric"], scope=c["scope"], dataset_fingerprint=c["dataset"] or None)["rows"] for c in cases
        ]
        expected_graph = [
            queries.operator_graph(metric=c["metric"], scope=c["scope"], dataset_fingerprint=c["dataset"] or None)["nodes"] for c in cases
        ]
    build_site(tmp_path / "store", tmp_path / "site", domain=None)

    (tmp_path / "engine.mjs").write_text(ENGINE.read_text(encoding="utf-8"), encoding="utf-8")
    (tmp_path / "driver.mjs").write_text(DRIVER, encoding="utf-8")
    queries_json = json.dumps([{"name": name, "args": c} for name in ("composition", "graph") for c in cases])
    result = subprocess.run(
        [node, "driver.mjs", str(tmp_path / "site" / "data" / "bundle.json"), queries_json],
        cwd=tmp_path, capture_output=True, text=True, check=True, timeout=120,
    )
    answers = json.loads(result.stdout)
    composition, graph = answers[: len(cases)], answers[len(cases):]

    for case, expected, actual in zip(cases, expected_composition, composition, strict=True):
        got = {r["operator"]: r for r in actual["rows"]}
        assert set(got) == {r["operator"] for r in expected}, case
        for row in expected:
            assert got[row["operator"]]["score"] == pytest.approx(row["score"], rel=1e-9), (case, row["operator"])
            assert got[row["operator"]]["n_pipes"] == row["n_pipes"], (case, row["operator"])
            assert got[row["operator"]]["n_runs"] == row["n_runs"], (case, row["operator"])
    for case, expected, actual in zip(cases, expected_graph, graph, strict=True):
        got = {n["id"]: n for n in actual["nodes"]}
        assert set(got) == {n["id"] for n in expected}, case
        for node_ in expected:
            assert got[node_["id"]]["score"] == pytest.approx(node_["score"], rel=1e-9), (case, node_["id"])
            assert got[node_["id"]]["n_runs"] == node_["n_runs"], (case, node_["id"])
            assert got[node_["id"]]["size"] == node_["size"], (case, node_["id"])
