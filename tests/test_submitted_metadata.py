"""Private source-fragment capture from synthetic exports."""

import json

from nirs4all_benchmarks.ingestion import validate_metadata
from nirs4all_benchmarks.ingestion import IngestionPolicy, ingest_export
from nirs4all_benchmarks.store import ArenaStore
from test_arena_v2 import record


def test_nested_null_capture_is_independent_and_not_in_public_validation():
    source = record()
    source["dataset"]["dataset_card"] = {"extra": {"nested": None}}
    source["dataset"]["dataset_spec_ref"] = "file:///client/dataset.json"
    source["provenance"] = {"extra": {"nested": None}}
    result = validate_metadata(source)
    assert result.status == "metadata_valid"
    captured = json.loads(result.source_metadata_json)
    assert captured == {
        "arena_context": {"schema_version": 1, "arena_export_hash": result.arena_export_hash,
                          "pipeline": {k: source["pipeline"][k] for k in ("human_label",) if k in source["pipeline"]},
                          "task": {k: source["task"][k] for k in ("target_unit",) if k in source["task"]}},
        "dataset": {"dataset_card": {"extra": {"nested": None}},
                    "dataset_spec_ref": "file:///client/dataset.json"},
        "provenance": {"extra": {"nested": None}},
    }
    normalized = result.model.dataset.model_dump(mode="json", exclude_none=True)
    assert normalized["dataset_card"]["extra"]["nested"] is None
    assert result.model.provenance.model_dump(mode="json", exclude_none=True)["extra"]["nested"] is None
    source["dataset"]["dataset_card"]["extra"]["nested"] = "changed"
    assert json.loads(result.source_metadata_json) == captured
    assert "source_metadata_json" not in result.to_json()
    assert "client/dataset" not in repr(result)


def test_private_fragments_and_card_are_bound_transactionally(tmp_path):
    from nirs4all_benchmarks.contract import ArenaRunExport
    from nirs4all_benchmarks.contract.evidence import observation_direction
    from nirs4all_benchmarks.identity import canonical_json
    from nirs4all_benchmarks.ingestion.metadata_bundle import sanitize_manifest

    source = record()
    source["dataset"]["dataset_card"] = {"citation_id": "missing", "extra": {"nested": None}}
    source["dataset"]["dataset_spec_ref"] = "/client/location"
    source["provenance"] = {"extra": {"nested": None}}
    with ArenaStore(tmp_path / "store") as store:
        rejected = ingest_export(store, source)
        assert rejected.issues[0]["code"] == "metadata_not_indexable"
        assert store.query_one("SELECT count(*) AS n FROM executions")["n"] == 0
        accepted = ingest_export(store, source, policy=IngestionPolicy(bind_card_citation=False))
        assert accepted.status == "committed"
        row = store.get("submitted_metadata", "arena_export_hash", accepted.arena_export_hash)
        assert json.loads(row["source_metadata_json"])["provenance"] == source["provenance"]
        card = store.get("dataset_cards", "dataset_card_hash", row["dataset_card_hash"])
        assert card["citation_id"] is None
        assert json.loads(card["card_json"])["citation_id"] == "missing"
        validation = validate_metadata(source)
        model = validation.model
        manifest = model.to_manifest()
        observations = [item.model_dump(mode="json") for item in model.scores.observations]
        for observation, item in zip(observations, model.scores.observations, strict=True):
            observation["direction"], observation["direction_source"] = observation_direction(item)
            observation["origin"] = observation.get("origin") or model.scores.origin or "producer_reported"
        manifest["scores"]["observations"] = observations
        expected = sanitize_manifest(ArenaRunExport.model_validate(manifest), accepted.arena_export_hash,
                                     validation.to_json())
        receipt = store.get("metadata_receipts", "arena_export_hash", accepted.arena_export_hash)
        assert receipt["receipt_json"] == canonical_json(expected)
        assert "/client/location" not in receipt["receipt_json"]
        assert ingest_export(store, source, policy=IngestionPolicy(bind_card_citation=False)).status == "already_ingested"


def test_first_normalized_equivalent_form_and_corruption(tmp_path):
    source = record()
    source.pop("provenance", None)
    alternate = record()
    alternate["provenance"] = None
    with ArenaStore(tmp_path / "store") as store:
        first = ingest_export(store, source)
        assert first.status == "committed"
        replay = ingest_export(store, alternate)
        assert replay.status == "already_ingested"
        row = store.get("submitted_metadata", "arena_export_hash", first.arena_export_hash)
        assert "provenance" not in json.loads(row["source_metadata_json"])
        store.conn.execute("UPDATE submitted_metadata SET source_metadata_json='{\"changed\":1}' WHERE arena_export_hash=?",
                           (first.arena_export_hash,))
        store.conn.commit()
        assert ingest_export(store, source).issues[0]["code"] == "stored_batch_inconsistent"


def test_store_two_is_refused_without_writes(tmp_path):
    import sqlite3
    from nirs4all_benchmarks.store import ArenaStoreRefusal
    import pytest

    root = tmp_path / "old"
    root.mkdir()
    with sqlite3.connect(root / "arena.sqlite") as db:
        db.execute("PRAGMA application_id=1312047410")
        db.execute("PRAGMA user_version=2")
    before = (root / "arena.sqlite").read_bytes()
    with pytest.raises(ArenaStoreRefusal, match="unsupported_arena_store_schema"):
        ArenaStore(root)
    assert (root / "arena.sqlite").read_bytes() == before
    assert sorted(p.name for p in root.iterdir()) == ["arena.sqlite"]


def test_post_write_failure_rolls_back_all_facts(tmp_path, monkeypatch):
    import pytest

    with ArenaStore(tmp_path / "store") as store:
        original = store.upsert
        def fail_receipt(table, row):
            if table == "metadata_receipts":
                raise RuntimeError("synthetic failure")
            return original(table, row)
        monkeypatch.setattr(store, "upsert", fail_receipt)
        with pytest.raises(RuntimeError, match="synthetic failure"):
            ingest_export(store, record())
        for table in ("executions", "ingestion_batches", "dataset_fingerprints", "metadata_receipts", "submitted_metadata"):
            assert store.count(table) == 0


def test_same_card_with_two_dataset_fingerprints(tmp_path):
    first = record()
    first["dataset"]["dataset_card"] = {"name": "shared", "extra": {"key": None}}
    second = record(execution_id="second")
    second["dataset"]["dataset_card"] = first["dataset"]["dataset_card"]
    second["dataset"]["dataset_fingerprint"] = "d" * 64
    second["task"]["task_hash"] = "e" * 64
    second["dataset_variant"]["dataset_variant_hash"] = "f" * 64
    with ArenaStore(tmp_path / "store") as store:
        assert ingest_export(store, first).status == "committed"
        assert ingest_export(store, second).status == "committed"
        assert store.count("dataset_cards") == 1
        assert store.count("submitted_metadata") == 2


def test_each_store_write_failure_rolls_back(tmp_path, monkeypatch):
    import pytest

    source = record()
    source["dataset"]["dataset_card"] = {"name": "synthetic"}
    with ArenaStore(tmp_path / "store") as store:
        methods = {name: getattr(store, name) for name in ("upsert", "insert", "update", "ensure_collection")}
        failed_positions = 0
        for position in range(1, 100):
            counter = [0]
            def wrapped(name):
                def invoke(*args, **kwargs):
                    result = methods[name](*args, **kwargs)
                    counter[0] += 1
                    if counter[0] == position:
                        raise RuntimeError("synthetic write failure")
                    return result
                return invoke
            with monkeypatch.context() as patch:
                for name in methods:
                    patch.setattr(store, name, wrapped(name))
                try:
                    result = ingest_export(store, source)
                except RuntimeError as exc:
                    assert str(exc) == "synthetic write failure"
                else:
                    assert result.status == "committed"
                    break
            failed_positions += 1
            for table in ("executions", "ingestion_batches", "collections", "dataset_cards", "metadata_receipts",
                          "submitted_metadata"):
                assert store.count(table) == 0
        else:
            pytest.fail("write sequence exceeded the bounded injection range")
        assert failed_positions > 5
        assert store.count("submitted_metadata") == 1


def test_resolver_drift_does_not_hide_corrupt_stored_execution_binding(tmp_path, monkeypatch):
    import nirs4all_benchmarks.ingestion.offline as offline

    source = record()
    with ArenaStore(tmp_path / "store") as store:
        first = ingest_export(store, source)
        assert first.status == "committed"
        original_execution = first.execution_hash
        drifted_execution = "e" * 64
        assert drifted_execution != original_execution
        # Synthetic corruption only: create a dangling binding that matches the drifted resolver.
        store.conn.execute("PRAGMA foreign_keys=OFF")
        store.conn.execute("UPDATE submitted_metadata SET execution_hash=? WHERE arena_export_hash=?",
                           (drifted_execution, first.arena_export_hash))
        store.conn.commit()
        store.conn.execute("PRAGMA foreign_keys=ON")
        assert store.conn.execute("PRAGMA foreign_keys").fetchone()[0] == 1
        resolve = offline.resolve_identities

        def drift(model):
            resolved = resolve(model)
            resolved.execution_hash = drifted_execution
            return resolved

        monkeypatch.setattr(offline, "resolve_identities", drift)
        replay = ingest_export(store, source)
        assert replay.status == "rejected"
        assert replay.issues[0]["code"] == "stored_batch_inconsistent"
        assert store.count("executions") == store.count("ingestion_batches") == store.count("metadata_receipts") == 1
        assert store.get("executions", "execution_hash", original_execution) is not None
        assert not store.conn.in_transaction
