"""Public historical demo is explicit, hash-pinned and never scientific acceptance."""

from __future__ import annotations

import json
import shutil
import subprocess
from pathlib import Path

import pytest

from nirs4all_benchmarks.contract.strict_json import strict_loads
from nirs4all_benchmarks.demo_site import PROFILE, build_demo_site
from nirs4all_benchmarks.frozen_history import FrozenHistoryRefusal, verified_source
from nirs4all_benchmarks.site_build import build_site

ROOT = Path(__file__).resolve().parents[1]
SOURCE = ROOT / "data/catalog-history-2026-09"


@pytest.fixture(scope="module")
def demo(tmp_path_factory):
    output = tmp_path_factory.mktemp("historical-demo") / "site"
    summary = build_demo_site(SOURCE, output, domain=None)
    return output, summary, json.loads((output / "data/bundle.json").read_text())


def test_historical_demo_only_exports_exact_public_records_and_unknown_validity(demo):
    output, summary, bundle = demo
    assert summary["executions"] == summary["historical_executions"] == 4998
    assert summary["valid_executions"] == summary["qualified_executions"] == 0
    assert bundle["meta"]["profile"] == PROFILE
    _, lines = verified_source(SOURCE)
    expected = {i for i, line in enumerate(lines) if strict_loads(line)["dataset"]["visibility"] == "public"}
    published = set()
    for path in (output / "data/runs").iterdir():
        detail = json.loads(path.read_text())["detail"]
        published.add(detail["metadata"]["source_ordinal"])
        assert detail["execution"]["validity_status"] == "unreported"
        assert detail["execution"]["status"] == "unreported"
        assert detail["execution"]["oof_enforced"] is None
        assert all(score["aggregation_level"] == "unreported" for score in detail["scores"])
        assert detail["nodes"] and detail["scores"]
    assert published == expected
    assert len(lines) - len(published) == 5826
    contexts = {(d["source_dataset_fingerprint"], d["task_hash"], d["dataset_variant_hash"])
                for d in bundle["datasets"]}
    assert len(contexts) == len(bundle["datasets"])
    assert len({d["source_dataset_fingerprint"] for d in bundle["datasets"]}) == 39
    assert len(bundle["datasets"]) >= 68
    contexts_by_condition = {}
    columns = bundle["tables"]["metrics"]["columns"]
    for rc_index, context_index in zip(columns["run_condition_hash"]["s"], columns["dataset_fingerprint"]["s"], strict=True):
        rc, context = bundle["strings"][rc_index], bundle["strings"][context_index]
        assert contexts_by_condition.setdefault(rc, context) == context
    assert len(contexts_by_condition) == summary["run_conditions"] == 4998
    assert not (output / "generated").exists()


def test_demo_requires_exact_source_and_empty_output(tmp_path):
    source = tmp_path / "source"
    source.mkdir()
    for name in ("manifest.json", "runs.jsonl.gz"):
        shutil.copyfile(SOURCE / name, source / name)
    (source / "manifest.json").write_bytes((source / "manifest.json").read_bytes() + b"\n")
    with pytest.raises(FrozenHistoryRefusal, match="source_hash_mismatch"):
        build_demo_site(source, tmp_path / "site")
    assert not (tmp_path / "site").exists()
    out = tmp_path / "existing"
    out.mkdir()
    (out / "keep").write_text("keep")
    with pytest.raises(ValueError, match="empty directory"):
        build_demo_site(SOURCE, out)
    assert (out / "keep").read_text() == "keep"
    with pytest.raises(ValueError, match="publication_gate_closed"):
        build_site(tmp_path / "no-store", tmp_path / "site")


def test_static_engine_admits_historical_observations_only_under_explicit_profile(demo):
    output, _, _ = demo
    node = shutil.which("node")
    if not node:
        pytest.skip("Node.js 18 or newer is required for the static demonstration engine")
    major = int(subprocess.run(
        [node, "-p", "process.versions.node.split('.')[0]"], capture_output=True, text=True, check=True,
    ).stdout)
    if major < 18:
        pytest.skip("Node.js 18 or newer is required for the static demonstration engine")
    script = r"""
const fs = require('fs');
const assert = require('assert');
(async () => {
  const original = JSON.parse(fs.readFileSync(process.argv[1] + '/data/bundle.json'));
  let snapshot = original;
  global.fetch = async () => ({ok: true, json: async () => snapshot});
  const source = fs.readFileSync(process.argv[1] + '/lib/static-engine.js', 'utf8');
  const {createStaticApi} = await import('data:text/javascript;base64,' + Buffer.from(source).toString('base64'));
  const api = await createStaticApi();
  assert((await api.leaderboard({metric: 'rmse', scope: 'test'})).rows.length > 0);
  assert((await api.matrix({metric: 'rmse', scope: 'test'})).cells.length > 0);
  assert((await api.runs({metric: 'rmse', scope: 'test', limit: 10000})).length > 0);
  assert((await api.facets()).length > 0);
  assert.strictEqual((await api.overview()).valid_executions, 0);
  snapshot = {...original, meta: {...original.meta, profile: 'ordinary-static-snapshot'}};
  const current = await createStaticApi();
  assert.strictEqual((await current.leaderboard({metric: 'rmse', scope: 'test'})).rows.length, 0);
})().catch(error => { console.error(error); process.exit(1); });
"""
    subprocess.run([node, "-e", script, str(output)], check=True, capture_output=True, text=True)
