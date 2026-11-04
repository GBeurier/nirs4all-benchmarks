"""Positive-loss comparison boundaries and native meta-analysis delivery."""

from __future__ import annotations

import json
import os
import shutil
import subprocess
from pathlib import Path

import pytest
from fastapi.testclient import TestClient
from pydantic import ValidationError

from nirs4all_benchmarks.ranking import RankingRequest, RelativeGainRequest, quality_relative_gain
from nirs4all_benchmarks.service import create_app
from nirs4all_benchmarks.site_build import build_site
from nirs4all_benchmarks.store import ArenaStore
from test_quality_ranking import add


def effect(baseline, candidate, *, variances=None):
    effect.baseline, effect.candidate, effect.variances = baseline, candidate, variances
    return {
        "schema_version": "n4m.relative_gain/1",
        "mean_gain": 0.25,
        "meta_analysis": "dersimonian_laird" if variances else None,
    }


def request(**kwargs):
    return {"baseline_id": "B", "candidate_id": "A", **kwargs}


def test_direct_pairing_variance_alignment_and_positive_loss(tmp_path):
    with ArenaStore(tmp_path) as store:
        add(store, "D1", "A", 1)
        add(store, "D1", "B", 2)
        add(store, "D2", "B", 100)
        add(store, "D2", "C", 200)
        result = quality_relative_gain(store, request(), estimator=effect)
        assert result["state"] == "ready"
        assert effect.baseline == [2] and effect.candidate == [1] and effect.variances is None
        assert result["provenance"]["study_dataset_ids"] == ["D1"]
        assert result["studies"][0]["matched_blocks"]
        assert (
            quality_relative_gain(store, request(candidate_id="C", baseline_id="A"), estimator=effect)["reason"]
            == "no_direct_paired_dataset_evidence"
        )
        result = quality_relative_gain(store, request(effect_variances={"D1": 0.01}), estimator=effect)
        assert result["state"] == "ready" and effect.variances == [0.01]
        assert (
            quality_relative_gain(store, request(effect_variances={"D1": 0.01, "D2": 0.2}), estimator=effect)["reason"]
            == "effect_variances_must_match_paired_dataset_ids"
        )
        assert (
            quality_relative_gain(store, request(effect_variances={"D1": 0}), estimator=effect)["reason"]
            == "effect_variances_must_be_finite_positive"
        )
        assert (
            quality_relative_gain(store, request(metric="r2"), estimator=effect)["reason"]
            == "relative_gain_requires_positive_loss_metric"
        )
        assert (
            quality_relative_gain(store, request(candidate_id="B"), estimator=effect)["reason"]
            == "distinct_baseline_and_candidate_required"
        )
        store.conn.execute("UPDATE metric_observations SET metric_value=0 WHERE score_set_id LIKE 'exec-D1-A-%'")
        assert (
            quality_relative_gain(store, request(), estimator=effect)["reason"]
            == "relative_gain_requires_strictly_positive_losses"
        )


@pytest.mark.parametrize("value", [float("nan"), float("inf"), -float("inf")])
def test_nonfinite_request_values_rejected_before_serialization(value):
    with pytest.raises(ValidationError):
        RelativeGainRequest(**request(effect_variances={"D1": value}))
    with pytest.raises(ValidationError):
        RankingRequest(options={"beta": value})


def test_native_gain_service_and_snapshot(tmp_path):
    if not os.environ.get("N4M_LIB_PATH"):
        pytest.skip("native runtime opt-in")
    root = tmp_path / "store"
    with ArenaStore(root) as store:
        for ds, a, b in (("D1", 1, 2), ("D2", 3, 4)):
            add(store, ds, "A", a)
            add(store, ds, "B", b)
        descriptive = quality_relative_gain(store, request())
        assert descriptive["state"] == "ready", descriptive
        assert descriptive["result"]["mean_gain"] == pytest.approx(0.375)
        assert descriptive["result"]["pooled_gain"] is None
        assert descriptive["result"]["standard_error"] is None
        assert descriptive["result"]["meta_analysis"] is None
    req = request(effect_variances={"D2": 0.02, "D1": 0.01})
    client = TestClient(create_app(root))
    response = client.post("/api/quality-relative-gain", json=req)
    assert response.status_code == 200, response.text
    result = response.json()
    assert result["state"] == "ready", result
    assert result["result"]["meta_analysis"] == "dersimonian_laird"
    assert result["result"]["standard_error"] > 0
    assert result["provenance"]["study_dataset_ids"] == ["D1", "D2"]
    build_site(root, tmp_path / "site", relative_gain_requests=[req])
    bundle = json.loads((tmp_path / "site/data/bundle.json").read_text())
    assert bundle["quality_relative_gains"] == [result]
    node = shutil.which("node")
    if node and int(subprocess.check_output([node, "-p", "process.versions.node.split('.')[0]"], text=True)) >= 18:
        engine = Path(__file__).parents[1] / "src/nirs4all_benchmarks/web/lib/static-engine.js"
        (tmp_path / "engine.mjs").write_text(engine.read_text())
        (tmp_path / "driver.mjs").write_text("""
import {readFileSync} from "node:fs";
import {createStaticApi} from "./engine.mjs";
globalThis.fetch = async () => ({ok:true,json:async()=>JSON.parse(readFileSync(process.argv[2],"utf8"))});
const api = await createStaticApi();
const request = JSON.parse(process.argv[3]);
console.log(JSON.stringify(await Promise.all([
  api.qualityRelativeGain({...request, direction:null}),
  api.qualityRelativeGain({...request,effect_variances:{D1:0.1,D2:0.2}})])));
""")
        browser = json.loads(
            subprocess.check_output(
                [node, str(tmp_path / "driver.mjs"), str(tmp_path / "site/data/bundle.json"), json.dumps(req)],
                text=True,
            )
        )
        assert browser[0] == result
        assert browser[1]["reason"] == "static_relative_gain_request_not_precomputed"
