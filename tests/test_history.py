"""Real-data archive integrity, score semantics, and repeatable store reconstruction."""

from __future__ import annotations

import gzip
import hashlib
import json
import math
from collections import Counter
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
    assert counts["executions"] == metadata["executions"] == 11103
    assert counts["observations"] == metadata["metric_observations"] == 26364
    assert len(datasets) == metadata["datasets"] == 107
    assert len(tasks) == metadata["tasks"] == 156


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
    assert all(row["score_scope"] == "test" for row in bundle["metrics"])
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
