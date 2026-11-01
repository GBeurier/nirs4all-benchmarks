"""Real-data archive integrity, score semantics, and repeatable store reconstruction."""

from __future__ import annotations

import gzip
import hashlib
import http.server
import json
import math
import shutil
import subprocess
import threading
from collections import Counter
from functools import partial
from pathlib import Path

import pytest

from nirs4all_benchmarks.contract import ArenaRunExport, validate_manifest
from nirs4all_benchmarks.history import ingest_history
from nirs4all_benchmarks.scoring import direction_of
from nirs4all_benchmarks.site_build import build_site
from nirs4all_benchmarks.store import ArenaStore, Queries

PACK = Path(__file__).resolve().parents[1] / "data" / "catalog-history-2026-09"


def test_checked_in_real_data_archive() -> None:
    metadata = json.loads((PACK / "manifest.json").read_text())
    assert hashlib.sha256((PACK / "runs.jsonl.gz").read_bytes()).hexdigest() == metadata["archive_sha256"]
    counts: Counter[str] = Counter()
    datasets = set()
    tasks = set()
    experiments = set()
    with gzip.open(PACK / "runs.jsonl.gz", "rt") as stream:
        for line in stream:
            manifest = json.loads(line)
            export = ArenaRunExport.model_validate(manifest)
            assert export.producer.capsule != "fixture"
            assert export.dataset.dataset_card["source_registry"] == "nirs4all-datasets"
            assert export.dataset.visibility in {"public", "restricted"}
            assert export.residuals.inline is None and export.residuals.ref is None
            assert export.leakage_attestation.oof_enforced
            assert export.leakage_attestation.group_leakage_checked
            source = manifest["provenance"]["historical_source"]
            assert metadata["sources"][source["path"]]["sha256"] == source["sha256"]
            assert 2 <= source["csv_line"] <= metadata["sources"][source["path"]]["rows"] + 1
            key = tuple(source["experiment_key"])
            assert key not in experiments
            experiments.add(key)
            datasets.add(export.dataset.dataset_card["dataset_id"])
            tasks.add((key[0], key[1], key[2], key[3]))
            for observation in export.scores.observations:
                assert observation.scope == "test" and observation.partition == "test"
                assert observation.fold_id is None
                assert observation.n_samples == export.split.partition_summary["test"]
                assert observation.metric_value is not None and math.isfinite(observation.metric_value)
                assert observation.direction == direction_of(observation.metric_name)
                if observation.direction == "max":
                    assert 0 <= observation.metric_value <= 1
                counts["observations"] += 1
            counts["executions"] += 1
    assert counts["executions"] == metadata["executions"] == 11010
    assert counts["observations"] == metadata["metric_observations"] == 26178
    assert len(datasets) == metadata["datasets"] == 106
    assert len(tasks) == metadata["tasks"] == 155


def _small_pack(root: Path) -> dict:
    with gzip.open(PACK / "runs.jsonl.gz", "rt") as stream:
        manifest = json.loads(next(stream))
    assert validate_manifest(manifest) == []
    archive = root / "runs.jsonl.gz"
    with gzip.open(archive, "wt") as stream:
        stream.write(json.dumps(manifest) + "\n")
    metadata = json.loads((PACK / "manifest.json").read_text())
    metadata.update(executions=1, archive_sha256=hashlib.sha256(archive.read_bytes()).hexdigest())
    (root / "manifest.json").write_text(json.dumps(metadata))
    return manifest


def test_history_ingest_is_idempotent_and_builds_real_site(tmp_path: Path) -> None:
    manifest = _small_pack(tmp_path)
    with ArenaStore(tmp_path / "store") as store:
        result = ingest_history(store, tmp_path)
        assert result["statuses"] == {"committed": 1}
        assert ingest_history(store, tmp_path)["statuses"] == {"already_ingested": 1}
        assert store.count("executions") == 1
        assert store.count("residual_sets") == 0
        scores = store.query("SELECT metric_name, metric_value FROM metric_observations")
        assert {r["metric_name"]: r["metric_value"] for r in scores} == {
            r["metric_name"]: r["metric_value"] for r in manifest["scores"]["observations"]
        }
        assert Queries(store).leaderboard(metric=scores[0]["metric_name"], scope="test")["rows"]
        assert Queries(store).collections()[0]["kind"] == "user_run_collection"
    summary = build_site(tmp_path / "store", tmp_path / "site", domain=None)
    assert summary["executions"] == 1
    bundle = json.loads((tmp_path / "site/data/bundle.json").read_text())
    scopes = bundle["tables"]["metrics"]["columns"]["score_scope"]["s"]
    assert {bundle["strings"][i] for i in scopes} == {"test"}
    assert bundle["collections"][0]["collection_id"] == "catalog_history_2026_09"


@pytest.mark.parametrize("failure", ["checksum", "count"])
def test_invalid_history_pack_does_not_write_facts(tmp_path: Path, failure: str) -> None:
    _small_pack(tmp_path)
    metadata_path = tmp_path / "manifest.json"
    metadata = json.loads(metadata_path.read_text())
    metadata["archive_sha256" if failure == "checksum" else "executions"] = "0" * 64 if failure == "checksum" else 2
    metadata_path.write_text(json.dumps(metadata))
    with ArenaStore(tmp_path / "store") as store:
        with pytest.raises(ValueError, match="checksum mismatch" if failure == "checksum" else "count mismatch"):
            ingest_history(store, tmp_path)
        assert store.count("executions") == store.count("collections") == 0


def test_real_charts_recover_from_saved_demo_filters(tmp_path: Path) -> None:
    chrome = next((shutil.which(name) for name in ("google-chrome", "chromium", "chromium-browser")
                   if shutil.which(name)), None)
    if not chrome:
        pytest.skip("Chrome or Chromium is required for chart rendering validation")
    _small_pack(tmp_path)
    with ArenaStore(tmp_path / "store") as store:
        ingest_history(store, tmp_path)
        assert Queries(store).overview()["score_scopes"] == ["test"]
    site = tmp_path / "site"
    build_site(tmp_path / "store", site, domain=None)
    index = site / "index.html"
    original = index.read_text(encoding="utf-8")
    server = http.server.ThreadingHTTPServer(("127.0.0.1", 0), partial(http.server.SimpleHTTPRequestHandler,
                                                                     directory=str(site)))
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        cases = [
            ({"metric": "r2", "scope": "cv", "dataset": "old-demo-dataset"}, "accuracy"),
            ({"metric": "f1_weighted", "scope": "test", "dataset": ""}, "f1_weighted"),
        ]
        for number, (lens, expected_metric) in enumerate(cases):
            setup = f"localStorage.setItem('arena.state.v1', {json.dumps(json.dumps(lens))});"
            # Check actual SVG bar paths, rather than treating a rendered table
            # or the absence of JavaScript errors as proof that a chart exists.
            setup += """
                const check = setInterval(() => {
                  const plot = document.getElementById('lb-plot');
                  if (plot?.querySelector('.barlayer .point path')) {
                    document.body.dataset.chartRendered = 'true';
                    document.body.dataset.chartLens = localStorage.getItem('arena.state.v1');
                    clearInterval(check);
                  }
                }, 100);
            """
            index.write_text(original.replace("</head>", f"<script>{setup}</script></head>"), encoding="utf-8")
            result = subprocess.run(
                [chrome, "--headless", "--no-sandbox", "--disable-gpu", "--disable-dev-shm-usage",
                 f"--user-data-dir={tmp_path / f'browser-{number}'}", "--virtual-time-budget=10000", "--dump-dom",
                 f"http://127.0.0.1:{server.server_port}/#/leaderboard"],
                capture_output=True, text=True, timeout=30, check=True,
            )
            assert 'data-chart-rendered="true"' in result.stdout
            assert f'&quot;metric&quot;:&quot;{expected_metric}&quot;' in result.stdout
            assert '&quot;scope&quot;:&quot;test&quot;' in result.stdout
            assert '&quot;dataset&quot;:&quot;&quot;' in result.stdout
    finally:
        server.shutdown()
        thread.join()
        server.server_close()
