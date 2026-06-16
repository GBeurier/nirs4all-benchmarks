"""ArenaRunExport contract: schema validation + model round-trip."""

from __future__ import annotations

from nirs4all_benchmarks.contract import ArenaRunExport, validate_manifest


def _minimal() -> dict:
    return {
        "arena_export_schema_version": 1,
        "dataset": {"dataset_fingerprint": "a" * 64},
        "task": {"task_hash": "b" * 64, "task_type": "regression"},
        "dataset_variant": {"dataset_variant_hash": "c" * 64},
        "pipeline": {"nodes": [{"node_id": "g0"}]},
    }


def test_minimal_manifest_is_valid():
    assert validate_manifest(_minimal()) == []


def test_model_round_trip():
    m = _minimal()
    exp = ArenaRunExport.model_validate(m)
    assert exp.dataset.dataset_fingerprint == "a" * 64
    again = exp.to_manifest()
    assert again["task"]["task_type"] == "regression"


def test_bad_fingerprint_rejected():
    m = _minimal()
    m["dataset"]["dataset_fingerprint"] = "not-a-hash"
    errors = validate_manifest(m)
    assert errors and any("dataset_fingerprint" in e for e in errors)


def test_wrong_schema_version_rejected():
    m = _minimal()
    m["arena_export_schema_version"] = 2
    assert validate_manifest(m)


def test_unknown_task_type_rejected():
    m = _minimal()
    m["task"]["task_type"] = "ranking"
    assert validate_manifest(m)


def test_missing_required_block_rejected():
    m = _minimal()
    del m["task"]
    assert validate_manifest(m)


def test_extra_fields_are_allowed():
    m = _minimal()
    m["pipeline"]["custom_producer_field"] = {"anything": 1}
    assert validate_manifest(m) == []
    exp = ArenaRunExport.model_validate(m)
    assert exp.pipeline.model_extra.get("custom_producer_field") == {"anything": 1}
