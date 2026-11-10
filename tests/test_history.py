"""Real-data archive integrity, score semantics, and repeatable store reconstruction."""

from __future__ import annotations

import gzip
import hashlib
import json
import math
from collections import Counter
from pathlib import Path

import pytest

from nirs4all_benchmarks.contract import validate_manifest
from nirs4all_benchmarks.history import ingest_history, load_history_exports
from nirs4all_benchmarks.scoring import direction_of
from nirs4all_benchmarks.site_build import build_site
from nirs4all_benchmarks.store import ArenaStore, Queries

PACK = Path(__file__).resolve().parents[1] / "data" / "catalog-history-2026-09"


def test_checked_in_real_data_archive_is_immutable_source_not_v2_attestation() -> None:
    from nirs4all_benchmarks.frozen_history import verified_source
    from nirs4all_benchmarks.ingestion import validate_metadata

    metadata, lines = verified_source(PACK)
    counts = Counter()
    datasets = set()
    tasks = set()
    experiments = set()
    for line in lines:
        raw = json.loads(line)
        assert validate_metadata(raw).issues[0]["code"] == "unsupported_export_version"
        assert raw["residuals"].get("inline") is None and raw["residuals"].get("ref") is None
        # These booleans are frozen source claims, never v2 safety evidence.
        assert raw["leakage_attestation"]["oof_enforced"] is True
        source = raw["provenance"]["historical_source"]
        assert metadata["sources"][source["path"]]["sha256"] == source["sha256"]
        assert 2 <= source["csv_line"] <= metadata["sources"][source["path"]]["rows"] + 1
        key = tuple(source["experiment_key"])
        assert key not in experiments
        experiments.add(key)
        datasets.add(raw["dataset"]["dataset_card"]["dataset_id"])
        tasks.add(tuple(key[:4]))
        for obs in raw["scores"]["observations"]:
            assert obs["scope"] == obs["partition"] == "test"
            assert obs["metric_value"] is not None and math.isfinite(obs["metric_value"])
            assert obs["direction"] == direction_of(obs["metric_name"])
            counts["observations"] += 1
        values = {o["metric_name"]: o["metric_value"] for o in raw["scores"]["observations"]}
        scale = raw["dataset"]["dataset_card"].get("target_scale")
        if "nrmse" in values:
            assert raw["task"]["task_type"] == "regression" and scale["sd"] > 0
            assert values["nrmse"] == pytest.approx(values["rmse"] / scale["sd"])
            counts["nrmse"] += 1
        else:
            assert scale is None
    assert len(lines) == metadata["executions"] == 10824
    assert counts["observations"] == metadata["metric_observations"] == 32286
    assert len(datasets) == metadata["datasets"] == 104 and len(tasks) == metadata["tasks"] == 153
    assert counts["nrmse"] == 6480


def test_converted_native_record_replays_and_stays_unranked(tmp_path):
    from nirs4all_benchmarks.frozen_history import _convert_record, verified_source
    from nirs4all_benchmarks.ingestion import ingest_exports_atomic

    _, lines = verified_source(PACK)
    export = _convert_record(lines[0], 0)
    with ArenaStore(tmp_path / "store") as store:
        assert ingest_exports_atomic(store, [export])[0].status == "committed"
        assert ingest_exports_atomic(store, [export])[0].status == "already_ingested"
        assert store.count("executions") == 1 and store.count("residual_sets") == 0
        assert [r["metric_value"] for r in store.query("SELECT metric_value FROM metric_observations")] == [
            o.metric_value for o in export.scores.observations
        ]
        assert Queries(store).leaderboard(scope="test")["rows"] == []
        assert store.count("metadata_receipts") == 1
    with pytest.raises(ValueError, match="publication_gate_closed"):
        build_site(tmp_path / "store", tmp_path / "site", domain=None)
    assert not (tmp_path / "site").exists()


def native_pack_metadata(archive):
    from nirs4all_benchmarks.frozen_history import (
        CONVERTER_ID,
        FIELD_MAPPING,
        MAPPING_VERSION,
        RECORDS_SHA256,
        SOURCE_ARCHIVE_SHA256,
        SOURCE_MANIFEST_SHA256,
    )

    return {
        "schema_version": 2,
        "kind": "arena.frozen-history/2",
        "converter_id": CONVERTER_ID,
        "mapping_version": MAPPING_VERSION,
        "converter_source_sha256": "a" * 64,
        "field_mapping": FIELD_MAPPING,
        "source": {
            "manifest_sha256": SOURCE_MANIFEST_SHA256,
            "archive_sha256": SOURCE_ARCHIVE_SHA256,
            "executions": 10824,
            "metric_observations": 32286,
            "path": "data/catalog-history-2026-09",
        },
        "output": {
            "records_sha256": RECORDS_SHA256,
            "archive_sha256": hashlib.sha256(archive).hexdigest(),
            "executions": 10824,
            "metric_observations": 32286,
        },
        "collection_id": "catalog_history_2026_09",
        "name": "Untrusted input",
        "description": "Deliberately invalid fixture",
    }


@pytest.mark.parametrize(
    "failure,code",
    [
        ("checksum", "history_archive_hash_mismatch"),
        ("count", "history_count_or_metadata_mismatch"),
        ("version", "unsupported_export_version"),
    ],
)
def test_invalid_history_pack_does_not_write_facts(tmp_path, failure, code):
    archive = gzip.compress(b"{}\n")
    metadata = native_pack_metadata(archive)
    if failure == "checksum":
        metadata["output"]["archive_sha256"] = "0" * 64
    elif failure == "count":
        metadata["output"]["executions"] = 2
    else:
        metadata["schema_version"] = 1
    (tmp_path / "manifest.json").write_text(json.dumps(metadata))
    (tmp_path / "runs.jsonl.gz").write_bytes(archive)
    with ArenaStore(tmp_path / "store") as store:
        with pytest.raises(ValueError, match=code):
            ingest_history(store, tmp_path)
        assert store.count("executions") == store.count("collections") == 0


def test_duplicate_native_pack_does_not_qualify_as_pinned_history(tmp_path):
    from test_arena_v2 import record

    archive = gzip.compress(((json.dumps(record()) + "\n") * 2).encode())
    metadata = native_pack_metadata(archive)
    (tmp_path / "manifest.json").write_text(json.dumps(metadata))
    (tmp_path / "runs.jsonl.gz").write_bytes(archive)
    with ArenaStore(tmp_path / "store") as store:
        with pytest.raises(ValueError, match="history_records_hash_mismatch"):
            ingest_history(store, tmp_path)
        assert store.count("executions") == store.count("collections") == 0


def test_frozen_history_is_refused_as_chart_input(tmp_path: Path) -> None:
    """Frozen source claims cannot become current chart eligibility."""
    from nirs4all_benchmarks.ingestion import validate_metadata

    # The original SVG/filter oracle is preserved verbatim under the historical
    # static-oracle fixtures. This refusal does not qualify browser rendering.
    with gzip.open(PACK / "runs.jsonl.gz", "rt") as stream:
        manifest = json.loads(next(stream))
    assert manifest["arena_export_schema_version"] == 1
    assert validate_manifest(manifest)
    assert validate_metadata(manifest).issues[0]["code"] == "unsupported_export_version"
    with pytest.raises(ValueError, match=r"^unsupported_export_version$"):
        load_history_exports(PACK)
    with pytest.raises(ValueError, match=r"^publication_gate_closed$"):
        build_site(tmp_path / "store", tmp_path / "site", domain=None)
    assert not (tmp_path / "store").exists()
    assert not (tmp_path / "site").exists()
