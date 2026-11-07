"""Sole-v2 invariants and owner-shape probes; all data and stores are synthetic."""

from __future__ import annotations

import copy
import hashlib
import json
import os
import sqlite3
import subprocess
import sys
from pathlib import Path

import pytest
from fastapi.testclient import TestClient
from typer.testing import CliRunner

from nirs4all_benchmarks.adapters import WorkspaceAdapterError, workspace_to_exports
from nirs4all_benchmarks.cli import app
from nirs4all_benchmarks.contract.schema import validate_manifest
from nirs4all_benchmarks.identity import fingerprint
from nirs4all_benchmarks.ingestion import (
    AtomicIngestionError,
    ingest_export,
    ingest_exports_atomic,
    upload,
    validate_metadata,
)
from nirs4all_benchmarks.service.app import create_app
from nirs4all_benchmarks.store import ArenaStore, ArenaStoreRefusal, Queries
from nirs4all_benchmarks.store.arena_store import ARENA_APPLICATION_ID, SCHEMA_FINGERPRINT, store_header_status
from test_workspace_partitions import AGGREGATIONS, owner_workspace


def observation(value=1.0, *, fold="0", label=None, scope="cv", partition="validation", level="sample"):
    return {
        "metric_name": "rmse",
        "metric_value": value,
        "direction": "min",
        "direction_source": "registry",
        "scope": scope,
        "partition": partition,
        "aggregation_level": level,
        "fold_id": fold,
        "source_fold_id": label,
    }


def record(value=1.0, *, execution_id="synthetic", known=True):
    return {
        "arena_export_schema_version": 2,
        "identity_basis": {
            name: {"kind": "owner_reported", "owner": "synthetic-test"}
            for name in ("dataset_fingerprint", "task_hash", "dataset_variant_hash")
        },
        "dataset": {"dataset_fingerprint": "a" * 64, "visibility": "public", "visibility_source": "producer_reported"},
        "task": {"task_hash": "b" * 64, "task_type": "regression"},
        "dataset_variant": {"dataset_variant_hash": "c" * 64},
        "pipeline": {"nodes": [{"node_id": "model", "role": "model", "operator": "synthetic.Model"}]},
        "execution": {"execution_id": execution_id, "status": "ok", "terminal_status": "completed"},
        "leakage_attestation": dict.fromkeys(
            ("oof_enforced", "group_leakage_checked", "nested_cv_safe"), True if known else None
        )
        | {"unsafe_flags": [] if known else None},
        "scores": {"observations": [observation(value)]},
    }


def snapshot(root):
    if not root.exists():
        return {}
    result = {}

    def walk(path):
        info = path.lstat()
        value = (info.st_mode, info.st_size, info.st_mtime_ns, info.st_atime_ns)
        if path.is_symlink():
            # readlink itself may update a link's atime; compare its target,
            # mode/size/mtime and every regular-file/directory timestamp instead.
            result[str(path.relative_to(root))] = (*value[:-1], os.readlink(path))
        elif path.is_dir():
            result[str(path.relative_to(root))] = value
            fd = os.open(path, os.O_RDONLY | os.O_DIRECTORY | os.O_NOATIME)
            try:
                names = os.listdir(fd)
            finally:
                os.close(fd)
            for name in names:
                walk(path / name)
        else:
            fd = os.open(path, os.O_RDONLY | os.O_NOATIME)
            with os.fdopen(fd, "rb") as stream:
                result[str(path.relative_to(root))] = (*value, hashlib.sha256(stream.read()).hexdigest())

    walk(root)
    return result


@pytest.mark.parametrize("version", [None, 1, 3, "2", 2.0, True])
def test_version_is_explicit_before_any_store_mutation(tmp_path, version):
    source = record()
    if version is None:
        source.pop("arena_export_schema_version")
    else:
        source["arena_export_schema_version"] = version
    assert validate_metadata(source).issues[0]["code"] == "unsupported_export_version"
    path = tmp_path / "input.json"
    path.write_text(json.dumps(source))
    target = tmp_path / "store"
    result = CliRunner().invoke(app, ["ingest-export", str(path), "--store", str(target)])
    assert result.exit_code == 2 and "unsupported_export_version" in result.output
    assert not target.exists()
    client = TestClient(create_app(target))
    assert client.post("/api/ingest", json=source).status_code == 400
    assert not target.exists()


def test_normalized_hash_required_context_strict_keys_and_types(tmp_path):
    source = record()
    valid = validate_metadata(source)
    assert valid.status == "metadata_valid" and valid.hash_profile == "v2_normalized"
    model = valid.model
    assert model is not None
    assert valid.arena_export_hash == fingerprint(model.model_dump(mode="json", exclude={"arena_export_hash"}))
    assert validate_manifest(model.to_manifest()) == []
    for key in (
        "scope",
        "partition",
        "aggregation_level",
        "direction",
        "direction_source",
        "fold_id",
        "source_fold_id",
    ):
        bad = copy.deepcopy(source)
        del bad["scores"]["observations"][0][key]
        assert validate_metadata(bad).status == "rejected"
    for bad in ({**source, "extra": None}, {**source, "cv": {"n_folds": "5"}}):
        assert validate_metadata(bad).status == "rejected"
    bad = copy.deepcopy(source)
    bad["scores"]["observations"][0]["extra"] = None
    assert validate_metadata(bad).status == "rejected"
    source["arena_export_hash"] = "f" * 64
    with ArenaStore(tmp_path / "store") as store:
        assert ingest_export(store, source).issues[0]["code"] == "export_hash_mismatch"
        assert store.count("executions") == 0


def test_known_null_unknown_and_unsafe_never_mix(tmp_path):
    with ArenaStore(tmp_path / "store") as store:
        source = record()
        assert ingest_export(store, source).ok
        unknown = record(0.1, execution_id="unknown", known=False)
        unknown["scores"]["observations"] = [observation(0.1, fold=None, scope=None, partition=None, level=None)]
        assert ingest_export(store, unknown).ok
        false = record(0.001, execution_id="false")
        false["leakage_attestation"]["oof_enforced"] = False
        unsafe = record(0.002, execution_id="unsafe")
        unsafe["leakage_attestation"]["unsafe_flags"] = ["train_predictions_as_features"]
        for bad in (false, unsafe):
            assert ingest_export(store, bad).ok
        q = Queries(store)
        assert q.leaderboard()["rows"][0]["mean"] == 1.0
        rows = q.unknown_context_rows()
        assert rows["ranked"] is False and len(rows["rows"]) == 1
        assert rows["rows"][0]["metric_value"] == 0.1
        unknown["dataset"].update(visibility="restricted", visibility_source="local_policy")
        unknown["dataset"]["dataset_fingerprint"] = "d" * 64
        unknown["task"]["task_hash"] = "e" * 64
        unknown["dataset_variant"]["dataset_variant_hash"] = "f" * 64
        unknown["execution"]["execution_id"] = "restricted"
        assert ingest_export(store, unknown).ok
        assert len(q.unknown_context_rows()["rows"]) == 1


def test_foldless_and_duplicate_and_refit_contexts(tmp_path):
    source = record()
    source["scores"]["observations"] = [observation(1.0, fold=str(i)) for i in range(5)]
    source["scores"]["observations"].append(observation(0.1, fold=None))
    with ArenaStore(tmp_path / "store") as store:
        assert ingest_export(store, source).ok
        ranked = Queries(store).leaderboard()["rows"][0]
        assert ranked["mean"] == 1.0 and ranked["n_obs"] == 5
        duplicate = copy.deepcopy(source)
        duplicate["scores"]["observations"].append(observation(9.0, fold="0"))
        assert ingest_export(store, duplicate).issues[0]["code"] == "duplicate_score_context"
        assert store.count("executions") == 1
        refs = record(execution_id="refit")
        refs["scores"]["observations"] = [
            observation(0.49478, fold=None, label="final", scope="refit", partition="test"),
            observation(0.34076, fold=None, label="final_agg", scope="refit", partition="test", level="group"),
        ]
        assert ingest_export(store, refs).ok
        q = Queries(store)
        with pytest.raises(ValueError, match="score_partition_required"):
            q.leaderboard(scope="refit")
        assert q.leaderboard(scope="refit", partition="test")["rows"][0]["mean"] == 0.49478
        assert (
            q.leaderboard(scope="refit", partition="test", aggregation_level="group", source_fold_id="final_agg")[
                "rows"
            ][0]["mean"]
            == 0.34076
        )


@pytest.mark.parametrize("version,wal", [(0, False), (1, False), (1, True), (2, False), (3, False)])
def test_foreign_old_and_wip_files_are_untouched(tmp_path, version, wal):
    root = tmp_path / "foreign"
    root.mkdir()
    connection = sqlite3.connect(root / "arena.sqlite")
    if wal:
        connection.execute("PRAGMA journal_mode=WAL")
    connection.execute(f"PRAGMA user_version={version}")
    connection.execute("CREATE TABLE foreign_data(x)")
    connection.commit()
    if not wal:
        connection.close()
    before = snapshot(root)
    with pytest.raises(ArenaStoreRefusal) as refusal:
        ArenaStore(root)
    assert refusal.value.code == "unsupported_arena_store_schema"
    assert snapshot(root) == before
    if wal:
        connection.close()


def test_foreign_roots_empty_files_symlinks_and_fingerprint(tmp_path, monkeypatch):
    root = tmp_path / "workspace"
    root.mkdir()
    foreign = root / "store.sqlite"
    foreign.write_bytes(b"foreign")
    before = snapshot(root)
    ctimes = (root.stat().st_ctime_ns, foreign.stat().st_ctime_ns)

    def forbidden_connect(*args, **kwargs):
        pytest.fail("Unclaimed roots must refuse before opening SQLite")

    with monkeypatch.context() as patch:
        patch.setattr(sqlite3, "connect", forbidden_connect)
        with pytest.raises(ArenaStoreRefusal, match="arena_store_root_not_empty"):
            ArenaStore(root)
        assert snapshot(root) == before
        assert (root.stat().st_ctime_ns, foreign.stat().st_ctime_ns) == ctimes
        empty = tmp_path / "empty"
        empty.mkdir()
        (empty / "arena.sqlite").touch()
        before_empty = snapshot(empty)
        empty_ctimes = (empty.stat().st_ctime_ns, (empty / "arena.sqlite").stat().st_ctime_ns)
        with pytest.raises(ArenaStoreRefusal, match="unsupported_arena_store_schema"):
            ArenaStore(empty)
        assert snapshot(empty) == before_empty
        assert (empty.stat().st_ctime_ns, (empty / "arena.sqlite").stat().st_ctime_ns) == empty_ctimes
        linked = tmp_path / "link"
        linked.symlink_to(root)
        with pytest.raises(ArenaStoreRefusal, match="arena_store_unsafe_path"):
            ArenaStore(linked)
        assert linked.is_symlink() and linked.readlink() == root
        assert snapshot(root) == before
        assert (root.stat().st_ctime_ns, foreign.stat().st_ctime_ns) == ctimes

    owned = tmp_path / "owned"
    with ArenaStore(owned) as store:
        assert store.get_meta("schema_fingerprint") == SCHEMA_FINGERPRINT
        assert store.conn.execute("PRAGMA application_id").fetchone()[0] == ARENA_APPLICATION_ID
        ingest_export(store, record())
        store.set_meta("schema_fingerprint", "wrong")
        store.conn.commit()
    (owned / "arrays" / "sentinel").write_bytes(b"synthetic-array-sentinel")
    (owned / "exports" / "sentinel").write_bytes(b"synthetic-export-sentinel")

    def application_state():
        connection = sqlite3.connect(owned / "arena.sqlite")
        try:
            return tuple(connection.iterdump())
        finally:
            connection.close()

    before_rows = application_state()
    before_arrays, before_exports = snapshot(owned / "arrays"), snapshot(owned / "exports")
    statements = []
    connect = sqlite3.connect

    def traced_connect(*args, **kwargs):
        connection = connect(*args, **kwargs)
        connection.set_trace_callback(statements.append)
        return connection

    with monkeypatch.context() as patch:
        patch.setattr(sqlite3, "connect", traced_connect)
        with pytest.raises(ArenaStoreRefusal, match="arena_store_schema_fingerprint_mismatch"):
            ArenaStore(owned)
    # A4 permits SQLite recovery for a claimed owned header, but no application
    # writes or WAL-mode changes may precede the failed fingerprint check.
    assert statements == ["PRAGMA busy_timeout=30000", "SELECT key,value FROM arena_meta"]
    assert application_state() == before_rows
    assert snapshot(owned / "arrays") == before_arrays
    assert snapshot(owned / "exports") == before_exports


def test_atomic_pack_failure_has_no_rows_or_visible_receipts(tmp_path):
    with ArenaStore(tmp_path / "store") as store:
        a, b = record(execution_id="a"), record(execution_id="b")
        b["dataset"]["dataset_card"] = {"citation_id": "missing"}
        with pytest.raises(AtomicIngestionError):
            ingest_exports_atomic(store, [a, b])
        for table in (
            "executions",
            "ingestion_batches",
            "collections",
            "metadata_receipts",
            "run_conditions",
            "dataset_cards",
        ):
            assert store.count(table) == 0
        assert list(store.exports_dir.iterdir()) == []
        assert not store.conn.in_transaction
        assert all(r.ok for r in ingest_exports_atomic(store, [a, a]))
        assert store.count("executions") == store.count("metadata_receipts") == 1


def test_health_and_upload_refuse_v1_before_provisioning(tmp_path):
    root = tmp_path / "old"
    root.mkdir()
    connection = sqlite3.connect(root / "arena.sqlite")
    connection.execute("PRAGMA user_version=1")
    connection.close()
    before = snapshot(root)
    factory = create_app(root)
    client = TestClient(factory)
    response = client.get("/api/healthz")
    assert response.status_code == 503 and response.json()["code"] == "unsupported_arena_store_schema"
    assert snapshot(root) == before
    assert store_header_status(root)["status"] == "refused"
    with ArenaStore(tmp_path / "new") as store:
        source = record()
        del source["arena_export_schema_version"]
        result = upload(store, source)
        assert result.status == "rejected" and result.ingestion["issues"][0]["code"] == "unsupported_export_version"
        assert store.count("executions") == 0


@pytest.mark.parametrize(
    "keyword", ["_or_", "_range_", "_grid_", "_cartesian_", "_zip_", "_log_range_", "_chain_", "_sample_"]
)
def test_every_owner_generation_keyword_refuses_before_export(tmp_path, keyword):
    root = owner_workspace(tmp_path / "workspace", arrays=False)
    with sqlite3.connect(root / "store.sqlite") as connection:
        connection.execute("UPDATE pipelines SET expanded_config=?", (json.dumps([{keyword: [1, 2]}]),))
    before = snapshot(root)
    with pytest.raises(WorkspaceAdapterError, match="workspace_unresolved_generator"):
        workspace_to_exports(root)
    assert snapshot(root) == before


def test_workspace_target_context_and_missing_arrays_are_honest(tmp_path):
    root = owner_workspace(tmp_path / "workspace")
    (export,) = workspace_to_exports(root)
    assert export.identity_basis.task_hash.kind == "arena_derived"
    assert export.identity_basis.task_hash.inputs["target_context_sha256"]
    assert export.leakage_attestation.oof_enforced is None
    with ArenaStore(tmp_path / "store") as store:
        result = ingest_export(store, export)
        assert result.ok and Queries(store).leaderboard()["rows"] == []
    arrays = root / "arrays" / "owner-fixture.parquet"
    arrays.unlink()
    (opaque,) = workspace_to_exports(root)
    assert opaque.task.task_hash != export.task.task_hash
    assert opaque.identity_basis.task_hash.inputs["target_context_sha256"] is None


def test_fresh_two_process_race_and_crash_wal_reopen(tmp_path):
    root = tmp_path / "race"
    command = [
        sys.executable,
        "-B",
        "-c",
        "from nirs4all_benchmarks.store import ArenaStore; import sys; s=ArenaStore(sys.argv[1]); print(s.get_meta('pseudonymization_salt')); s.close()",
        str(root),
    ]
    processes = [subprocess.Popen(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True) for _ in range(2)]
    outputs = [p.communicate(timeout=30) for p in processes]
    assert all(p.returncode == 0 for p in processes), outputs
    assert outputs[0][0] == outputs[1][0]
    crash = tmp_path / "crash"
    code = "from nirs4all_benchmarks.store import ArenaStore; import sys,os; s=ArenaStore(sys.argv[1]); s.set_meta('crash_probe','committed'); s.conn.commit(); os._exit(0)"
    subprocess.run([sys.executable, "-B", "-c", code, str(crash)], check=True)
    assert (crash / "arena.sqlite-wal").stat().st_size > 0
    with ArenaStore(crash) as store:
        assert store.get_meta("crash_probe") == "committed"


@pytest.mark.parametrize("method,kwargs,values", AGGREGATIONS)
def test_every_query_keeps_validation_train_refit_and_view_context_separate(tmp_path, method, kwargs, values):
    (export,) = workspace_to_exports(owner_workspace(tmp_path / "workspace"))
    raw = export.to_manifest()
    # Explicit synthetic producer declarations solely to exercise the ranking lens.
    raw["leakage_attestation"] = record()["leakage_attestation"]
    raw["scores"]["observations"].append(observation(0.0001, fold=None, label="avg", scope="view"))
    with ArenaStore(tmp_path / "store") as store:
        assert ingest_export(store, raw).ok
        aggregate = getattr(Queries(store), method)
        assert set(values(aggregate(**kwargs))) == {1.0}
        assert set(values(aggregate(**kwargs, partition="train"))) == {0.1}
        with pytest.raises(ValueError, match="score_partition_required"):
            aggregate(**kwargs, scope="refit")
        for part, expected in (("train", 0.05), ("test", 0.5)):
            assert set(values(aggregate(**kwargs, scope="refit", partition=part))) == {expected}
        with pytest.raises(ValueError, match="source_fold_selection_required"):
            aggregate(**kwargs, scope="view")
        assert set(values(aggregate(**kwargs, scope="view", source_fold_id="avg"))) == {0.0001}
        unknown = record(0.00001, execution_id="unknown", known=False)
        assert ingest_export(store, unknown).ok
        assert 0.00001 not in values(aggregate(**kwargs))


def test_workspace_collisions_preflight_and_cross_ingest_are_atomic(tmp_path):
    import pyarrow as pa
    import pyarrow.parquet as pq

    from test_workspace_evidence_context import duplicate_pipeline

    root = owner_workspace(tmp_path / "workspace")
    with sqlite3.connect(root / "store.sqlite") as c:
        config = json.loads(c.execute("SELECT expanded_config FROM pipelines").fetchone()[0])
        duplicate_pipeline(c, "p", "p2", "other-name", config)
    path = root / "arrays/owner-fixture.parquet"
    rows = pq.read_table(path).to_pylist()
    pq.write_table(pa.Table.from_pylist(rows + [{**r, "prediction_id": "p2" + r["prediction_id"]} for r in rows]), path)
    before = snapshot(root)
    with pytest.raises(WorkspaceAdapterError, match="workspace_identity_collision"):
        workspace_to_exports(root)
    assert snapshot(root) == before
    target = tmp_path / "refused"
    outcome = CliRunner().invoke(app, ["ingest-workspace", str(root), "--store", str(target)])
    assert outcome.exit_code == 2 and not target.exists()
    (first,) = workspace_to_exports(owner_workspace(tmp_path / "a"))
    second_root = owner_workspace(tmp_path / "b")
    with sqlite3.connect(second_root / "store.sqlite") as c:
        c.execute("UPDATE pipelines SET name='different owner name'")
    (second,) = workspace_to_exports(second_root)
    with ArenaStore(tmp_path / "store") as store:
        assert ingest_export(store, first).ok
        refusal = ingest_export(store, second)
        assert refusal.issues[0]["code"] == "workspace_identity_collision"
        assert store.count("executions") == store.count("metadata_receipts") == 1
    with ArenaStore(tmp_path / "atomic") as store:
        with pytest.raises(AtomicIngestionError, match="workspace_identity_collision"):
            ingest_exports_atomic(store, [first, second])
        assert store.count("executions") == store.count("metadata_receipts") == 0


def test_different_targets_and_datasets_do_not_merge(tmp_path):
    import pyarrow as pa
    import pyarrow.parquet as pq

    (first,) = workspace_to_exports(owner_workspace(tmp_path / "first"))
    root = owner_workspace(tmp_path / "second")
    path = root / "arrays/owner-fixture.parquet"
    rows = pq.read_table(path).to_pylist()
    for row in rows:
        row["y_true"] = [900.0, 901.0]
    pq.write_table(pa.Table.from_pylist(rows), path)
    (second,) = workspace_to_exports(root)
    assert first.task.task_hash != second.task.task_hash
    assert first.dataset_variant.dataset_variant_hash != second.dataset_variant.dataset_variant_hash
    with sqlite3.connect(root / "store.sqlite") as c:
        c.execute("UPDATE pipelines SET dataset_name='different dataset',name='other name',dataset_hash=?", ("d" * 64,))
        c.execute("UPDATE predictions SET dataset_name='different dataset'")
    (third,) = workspace_to_exports(root)
    with ArenaStore(tmp_path / "store") as store:
        assert all(r.ok for r in ingest_exports_atomic(store, [first, second, third]))
        assert store.count("run_conditions") == store.count("task_specs") == 3


def test_workspace_wal_task_aliases_and_invalid_status(tmp_path):
    root = owner_workspace(tmp_path / "source", arrays=False)
    with sqlite3.connect(root / "store.sqlite") as c:
        c.execute("PRAGMA journal_mode=WAL")
        c.execute("UPDATE pipelines SET name='WAL'")
        c.commit()
        before = snapshot(root)
        with pytest.raises(WorkspaceAdapterError, match="workspace_store_not_quiescent"):
            workspace_to_exports(root)
        assert snapshot(root) == before
    c.close()
    for source, target in (("binary_classification", "binary"), ("multiclass_classification", "multiclass")):
        with sqlite3.connect(root / "store.sqlite") as c:
            c.execute("UPDATE predictions SET task_type=?", (source,))
        c.close()
        (export,) = workspace_to_exports(root)
        assert export.task.task_type == target
    with sqlite3.connect(root / "store.sqlite") as c:
        c.execute("UPDATE predictions SET task_type='classification'")
    c.close()
    with pytest.raises(WorkspaceAdapterError, match="workspace_classification_task_unsupported"):
        workspace_to_exports(root)


def test_dagml_variants_unknown_context_and_classification_refuse_honestly(tmp_path):
    from nirs4all_benchmarks.adapters import bundle_to_export

    native = record()
    report = {"variant_label":"selected", "metrics": {"rmse": 0.1}}
    export = bundle_to_export({"bundle_id": "synthetic","selected_variant_id":"selected"}, owner_export=native, metric_reports=[report])
    (obs,) = export.scores.observations
    assert (obs.scope, obs.partition, obs.aggregation_level, obs.fold_id) == (
        "unreported",
        "unreported",
        "unreported",
        None,
    )
    with ArenaStore(tmp_path / "store") as store:
        assert ingest_export(store, export).ok
        assert Queries(store).leaderboard()["rows"] == []
    for bundle, reports in (
        ({"variants": ["a", "b"]}, [report]),
        ({}, [{**report, "variant_label": "a"}, {**report, "variant_label": "b"}]),
    ):
        with pytest.raises(ValueError, match="dagml_variant_context_unsupported"):
            bundle_to_export(bundle, owner_export=native, metric_reports=reports)
    native["task"]["task_type"] = "binary"
    with pytest.raises(ValueError, match="dagml_prediction_task_unsupported"):
        bundle_to_export({}, owner_export=native)


def test_residual_context_no_defaults_and_synthetic_producer_is_unranked(tmp_path):
    from nirs4all_benchmarks.fixtures.generate import generate_fixture_exports

    source = record()
    source["residuals"] = {
        "key": "sample_id",
        "publishable": dict.fromkeys(("y_true", "y_pred", "residual", "y_proba"), True),
        "inline": [{"sample_id": "synthetic", "y_true": 1.0, "y_pred": 0.0}],
    }
    assert validate_metadata(source).status == "rejected"
    generated = generate_fixture_exports()
    assert generated and all(e.arena_export_schema_version == 2 for e in generated)
    assert all(validate_metadata(e).status == "metadata_valid" for e in generated)
    with ArenaStore(tmp_path / "store") as store:
        assert all(r.ok for r in ingest_exports_atomic(store, generated))
        assert Queries(store).leaderboard()["rows"] == []


def test_publication_gate_is_closed_before_output_or_store_mutation(tmp_path):
    from nirs4all_benchmarks.site_build import build_site

    with pytest.raises(ValueError, match="publication_gate_closed"):
        build_site(tmp_path / "store", tmp_path / "site")
    assert not (tmp_path / "site").exists() and not (tmp_path / "store").exists()
    workflow = Path(".github/workflows/pages.yml").read_text()
    assert "  push:" not in workflow and "publication_gate_closed" in workflow


def test_two_process_dedup_and_terminal_outcomes_are_honest(tmp_path):
    source = tmp_path / "input.json"
    source.write_text(json.dumps(record()))
    root = tmp_path / "concurrent"
    code = (
        "import sys; from pathlib import Path; from nirs4all_benchmarks.store import ArenaStore; "
        "from nirs4all_benchmarks.ingestion import ingest_export; "
        "s=ArenaStore(sys.argv[1]); r=ingest_export(s,Path(sys.argv[2])); print(r.status); s.close()"
    )
    command = [sys.executable, "-B", "-c", code, str(root), str(source)]
    processes = [subprocess.Popen(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True) for _ in range(2)]
    results = [p.communicate(timeout=30) for p in processes]
    assert all(p.returncode == 0 for p in processes), results
    assert sorted(r[0].strip() for r in results) == ["already_ingested", "committed"]
    with ArenaStore(root) as store:
        assert store.count("executions") == store.count("metadata_receipts") == 1
        for terminal, status in (("incomplete", "failed"), ("failed", "failed"), ("cancelled", "cancelled")):
            raw = record(0.0001, execution_id=terminal)
            raw["execution"].update(status=status, terminal_status=terminal)
            first = ingest_export(store, raw)
            assert first.ok and first.terminal_status == terminal
            assert ingest_export(store, raw).status == "already_ingested"
            detail = Queries(store).run_detail(first.execution_hash)
            assert detail["execution"]["status"] == terminal and len(detail["scores"]) == 1
        assert Queries(store).leaderboard()["rows"][0]["n_obs"] == 1


def test_upload_and_cli_v1_refusal_precedes_fresh_provisioning(tmp_path):
    raw = record()
    raw["arena_export_schema_version"] = 1
    path = tmp_path / "v1.json"
    path.write_text(json.dumps(raw))
    target = tmp_path / "cli"
    result = CliRunner().invoke(app, ["ingest-pipeline", str(path), "--store", str(target)])
    assert result.exit_code == 2 and "unsupported_export_version" in result.output and not target.exists()
    client = TestClient(create_app(tmp_path / "api"))
    for request in (
        {"data": {"text": json.dumps(raw)}},
        {"files": {"file": ("v1.json", json.dumps(raw), "application/json")}},
    ):
        response = client.post("/api/upload", **request)
        assert response.status_code == 400 and response.json()["detail"] == "unsupported_export_version"
        assert not (tmp_path / "api").exists()


def test_legacy_files_and_receipt_nested_extras_cannot_be_served(tmp_path):
    from nirs4all_benchmarks.ingestion.metadata_bundle import read_stored_metadata

    with ArenaStore(tmp_path / "store") as store:
        result = ingest_export(store, record())
        assert result.ok
        legacy = store.exports_dir / f"{result.arena_export_hash}.json"
        legacy.write_text(json.dumps({**record(), "raw_samples": ["private-synthetic-sentinel"]}))
        before = snapshot(store.exports_dir)
        q = Queries(store)
        detail = q.run_detail(result.execution_hash)
        assert "private-synthetic-sentinel" not in json.dumps(detail)
        assert detail["metadata"]["sanitized"] is True
        assert snapshot(store.exports_dir) == before
        assert read_stored_metadata(legacy, result.arena_export_hash)["reason"] == "legacy_unsanitized_export_refused"
        receipt = store.get("metadata_receipts", "arena_export_hash", result.arena_export_hash)
        raw = json.loads(receipt["receipt_json"])
        raw["pipeline"]["raw_samples"] = ["private-synthetic-sentinel"]
        assert (
            read_stored_metadata(None, result.arena_export_hash, stored_json=json.dumps(raw))["status"]
            == "metadata_unavailable"
        )
        raw = json.loads(receipt["receipt_json"])
        raw["identity_basis"]["task_hash"]["inputs"] = {"raw_samples": ["private-synthetic-sentinel"]}
        assert (
            read_stored_metadata(None, result.arena_export_hash, stored_json=json.dumps(raw))["status"]
            == "metadata_unavailable"
        )


@pytest.mark.parametrize("folds", [4, 5])
def test_exact_whitespace_pseudo_folds_cannot_complete_a_cv_series(tmp_path, folds):
    root = owner_workspace(tmp_path / "source", folds=folds)
    with sqlite3.connect(root / "store.sqlite") as c:
        columns = [r[1] for r in c.execute("PRAGMA table_info(predictions)")]
        row = dict(
            zip(
                columns,
                c.execute("SELECT * FROM predictions WHERE fold_id='0' AND partition='val'").fetchone(),
                strict=True,
            )
        )
        for fold in (" avg ", " w_avg ", "final_agg", " "):
            values = {
                **row,
                "prediction_id": "pseudo" + fold,
                "fold_id": fold,
                "scores": json.dumps({"val": {"rmse": 0.1}}),
            }
            c.execute(
                f"INSERT INTO predictions ({','.join(values)}) VALUES ({','.join('?' for _ in values)})",
                tuple(values.values()),
            )
    (export,) = workspace_to_exports(root)
    assert {o.source_fold_id for o in export.scores.observations if o.scope == "view"} >= {" avg ", " w_avg ", " "}
    assert all(o.fold_id is None for o in export.scores.observations if o.scope in {"view", "refit"})
    raw = export.to_manifest()
    raw["cv"]["n_folds"] = 5
    if folds == 4:
        assert validate_metadata(raw).issues[0]["code"] in {
            "incomplete_metric_fold_coverage",
            "incomplete_fold_coverage",
        }
        return
    assert validate_metadata(raw).status == "metadata_valid"
    raw["leakage_attestation"] = record()["leakage_attestation"]
    with ArenaStore(tmp_path / "store") as store:
        assert ingest_export(store, raw).ok
        (row,) = Queries(store).leaderboard()["rows"]
        assert row["mean"] == 1.0 and row["n_obs"] == 5


def test_live_owned_writer_refuses_snapshot_without_modifying_files(tmp_path, monkeypatch):
    root = tmp_path / "store"

    def artifact_state():
        # Reads of a valid owned store may advance atime (A2). Retain every
        # artifact, its content/target, mode, size, mtime and explicit ctime.
        result = {}
        for name in ("arrays", "exports"):
            for relative, value in snapshot(root / name).items():
                path = root / name / relative
                stable = value if path.is_symlink() else (*value[:3], *value[4:])
                result[name, relative] = (stable, path.lstat().st_ctime_ns)
        return result

    with ArenaStore(root) as store:
        store.set_meta("committed_probe", "previously-committed")
        store.conn.commit()
    before_artifacts = artifact_state()
    code = (
        "import sys; from nirs4all_benchmarks.store import ArenaStore; "
        "s=ArenaStore(sys.argv[1]); s.conn.execute('BEGIN IMMEDIATE'); "
        "s.set_meta('committed_probe','uncommitted-change'); "
        "s.set_meta('uncommitted_probe','not-visible'); print('locked',flush=True); "
        "sys.stdin.readline(); s.conn.rollback(); s.close()"
    )
    process = subprocess.Popen(
        [sys.executable, "-B", "-c", code, str(root)],
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )
    try:
        assert process.stdout.readline().strip() == "locked"
        with ArenaStore(root) as reader:
            assert reader.get_meta("committed_probe") == "previously-committed"
            assert reader.get_meta("uncommitted_probe") is None
    finally:
        _, stderr = process.communicate("release\n", timeout=30)
    assert process.returncode == 0, stderr
    with ArenaStore(root) as store:
        assert store.get_meta("committed_probe") == "previously-committed"
        assert store.get_meta("uncommitted_probe") is None
    assert artifact_state() == before_artifacts

    # A live WAL writer permits committed reads. Genuine exclusive contention
    # in rollback-journal mode is a separate, typed busy refusal.
    code = (
        "import sqlite3,sys; c=sqlite3.connect(sys.argv[1]); "
        "c.execute('PRAGMA journal_mode=DELETE'); c.execute('BEGIN EXCLUSIVE'); "
        "print('locked',flush=True); sys.stdin.readline(); c.rollback(); c.close()"
    )
    process = subprocess.Popen(
        [sys.executable, "-B", "-c", code, str(root / "arena.sqlite")],
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )
    try:
        assert process.stdout.readline().strip() == "locked"
        with pytest.raises(ArenaStoreRefusal, match=r"^arena_store_busy$") as refusal:
            ArenaStore(root, busy_timeout_ms=50)
        assert refusal.value.code == "arena_store_busy"
    finally:
        _, stderr = process.communicate("release\n", timeout=30)
    assert process.returncode == 0, stderr
    with ArenaStore(root, busy_timeout_ms=50) as store:
        assert store.get_meta("committed_probe") == "previously-committed"
        assert store.get_meta("uncommitted_probe") is None
    assert artifact_state() == before_artifacts


def test_static_python_projection_filters_every_table_and_freeform_facet(tmp_path):
    from nirs4all_benchmarks.site_build import _build_bundle, _execution_hashes, _public_run_document

    public = record()
    public["pipeline"]["nodes"][0]["params"] = {"numeric": 3, "freeform": "private-synthetic-sentinel"}
    public["pipeline"]["human_label"] = "Explicit public synthetic fixture"
    with ArenaStore(tmp_path / "store") as store:
        result = ingest_export(store, public)
        assert result.ok
        for visibility in ("restricted", "private", "unreported"):
            raw = copy.deepcopy(public)
            raw["execution"]["execution_id"] = visibility
            raw["dataset"].update(visibility=visibility, visibility_source="local_policy")
            assert ingest_export(store, raw).ok
        bundle = _build_bundle(store, Queries(store))
        assert bundle["tables"]["metrics"]["n"] == 1
        assert _execution_hashes(bundle) == [result.execution_hash]
        assert "private-synthetic-sentinel" not in json.dumps(bundle)
        document = _public_run_document(store, Queries(store), result.execution_hash, bundle)
        assert "private-synthetic-sentinel" not in json.dumps(document)


@pytest.mark.parametrize("withheld", ["y_true", "y_pred", "residual"])
def test_residual_policy_cannot_reconstruct_withheld_column(tmp_path, withheld):
    source = record()
    source["residuals"] = {
        "key": "sample_id",
        "pseudonymized": False,
        "publishable": {key: key != withheld for key in ("y_true", "y_pred", "residual", "y_proba")},
        "inline": [
            {
                "sample_id": "sensitive-synthetic-id",
                "scope": "cv",
                "partition": "validation",
                "fold_id": "0",
                "y_true": 1.0,
                "y_pred": 0.5,
                "residual": 0.5,
            }
        ],
    }
    with ArenaStore(tmp_path / "store") as store:
        result = ingest_export(store, source)
        assert result.ok
        residual_set = store.query_one("SELECT * FROM residual_sets")
        (row,) = store.residuals.read(residual_set["residual_set_id"])
        assert row[withheld] is None
        assert sum(row[key] is not None for key in ("y_true", "y_pred", "residual")) <= 1
        assert row["sample_id"] != "sensitive-synthetic-id"


def test_threaded_native_dedup_preserves_one_receipt(tmp_path):
    from concurrent.futures import ThreadPoolExecutor

    root = tmp_path / "threaded"

    def consume(_):
        with ArenaStore(root) as store:
            return ingest_export(store, record()).status

    with ThreadPoolExecutor(max_workers=3) as executor:
        statuses = list(executor.map(consume, range(3)))
    assert sorted(statuses) == ["already_ingested", "already_ingested", "committed"]
    with ArenaStore(root) as store:
        assert store.count("executions") == store.count("metadata_receipts") == 1


@pytest.mark.parametrize("component", ["arrays", "exports", "arena.sqlite-wal", "arena.sqlite-shm", "arena.sqlite"])
def test_store_component_symlinks_refuse_before_mutation(tmp_path, component):
    root = tmp_path / "store"
    with ArenaStore(root):
        pass
    path = root / component
    if path.is_dir():
        path.rmdir()
    else:
        path.unlink(missing_ok=True)
    target = tmp_path / "other"
    target.write_bytes(b"synthetic foreign content")
    path.symlink_to(target)
    before = snapshot(tmp_path)
    with pytest.raises(ArenaStoreRefusal, match="arena_store_unsafe_path"):
        ArenaStore(root)
    assert snapshot(tmp_path) == before


def test_residual_refs_and_unknown_direction_never_gain_context_on_replay(tmp_path):
    with ArenaStore(tmp_path / "store") as store:
        assert ingest_export(store, record()).ok
        for ref in ("../escape.parquet", "/absolute.parquet", {"path": "../escape.parquet"}):
            raw = record()
            raw["residuals"] = {"ref": ref}
            result = ingest_export(store, raw)
            assert result.issues[0]["code"] in {"residual_ref_unsupported", "schema_invalid"}
        raw = record(execution_id="unknown-direction")
        raw["pipeline"]["nirs4all_identity_hash"] = "0123456789abcdef"
        raw["scores"]["observations"][0].update(metric_name="r2:y", direction=None, direction_source="unreported")
        result = ingest_export(store, raw)
        assert result.ok
        detail = Queries(store).run_detail(result.execution_hash)
        assert detail["scores"][0]["direction"] is None
        assert detail["metadata"]["pipeline"]["nirs4all_identity_hash"] == "0123456789abcdef"
        with pytest.raises(ValueError, match="metric_direction_unreported_or_conflicting"):
            Queries(store).leaderboard(metric="r2:y")
        assert store.count("executions") == 2


def test_partial_target_arrays_stay_opaque_and_duplicate_rows_refuse(tmp_path):
    import pyarrow as pa
    import pyarrow.parquet as pq

    root = owner_workspace(tmp_path / "source")
    (complete,) = workspace_to_exports(root)
    path = root / "arrays/owner-fixture.parquet"
    rows = pq.read_table(path).to_pylist()
    pq.write_table(pa.Table.from_pylist(rows[1:]), path)
    (partial,) = workspace_to_exports(root)
    assert partial.identity_basis.task_hash.inputs["target_context_sha256"] is None
    assert partial.task.task_hash != complete.task.task_hash
    raw = partial.to_manifest()
    raw["leakage_attestation"] = record()["leakage_attestation"]
    with ArenaStore(tmp_path / "store") as store:
        assert ingest_export(store, raw).ok
        assert Queries(store).leaderboard()["rows"] == []
    pq.write_table(pa.Table.from_pylist([*rows, rows[0]]), path)
    with pytest.raises(WorkspaceAdapterError, match="workspace_array_identity_ambiguous"):
        workspace_to_exports(root)


@pytest.mark.parametrize("config", [None, "[]", '[\u007b"model":\u007b"class":"a","class":"b"\u007d\u007d]'])
def test_missing_or_duplicate_config_refuses_without_inventing_a_model_only_pipeline(tmp_path, config):
    root = owner_workspace(tmp_path / "source", arrays=False)
    with sqlite3.connect(root / "store.sqlite") as c:
        c.execute("UPDATE pipelines SET expanded_config=?", (config,))
    with pytest.raises(WorkspaceAdapterError, match=r"workspace_pipeline_(structure_required|config_malformed)"):
        workspace_to_exports(root)


def test_resolved_owner_variants_keep_1039_and_2389_separate(tmp_path):
    exports = []
    for index, value in enumerate((1.039, 2.389)):
        root = owner_workspace(tmp_path / f"variant-{index}")
        with sqlite3.connect(root / "store.sqlite") as c:
            c.execute(
                "UPDATE pipelines SET name=?,expanded_config=?,original_template=?",
                (
                    f"Resolved synthetic variant {index}",
                    json.dumps(
                        [
                            {
                                "model": {"class": "sklearn.cross_decomposition.PLSRegression"},
                                "params": {"n_components": index + 1},
                            }
                        ]
                    ),
                    json.dumps([{"_or_": ["a", "b"]}]),
                ),
            )
            c.execute(
                "UPDATE predictions SET val_score=?,scores=?",
                (value, json.dumps({"val": {"rmse": value}, "train": {"rmse": 0.1}, "test": {"rmse": 0.5}})),
            )
        (export,) = workspace_to_exports(root)
        raw = export.to_manifest()
        # Explicit synthetic declarations test only the lens; the actual adapter
        # emitted null safety and this test did not execute either owner engine.
        assert all(v is None for v in raw["leakage_attestation"].values())
        raw["leakage_attestation"] = record()["leakage_attestation"]
        assert raw["scores"]["score_version"] == "unreported"
        exports.append(raw)
    with ArenaStore(tmp_path / "store") as store:
        assert all(r.ok for r in ingest_exports_atomic(store, exports))
        rows = Queries(store).leaderboard()["rows"]
        assert [r["mean"] for r in rows] == pytest.approx([1.039, 2.389])
        assert [r["n_obs"] for r in rows] == [5, 5]


@pytest.mark.parametrize(
    "flat,shape",
    [([0.1, 0.9, 0.2, 0.8, 0.5], [2, 2]), ([0.1, 0.9], [2, 2]), ([0.1, 0.9], None), ([True, False], [2, 1])],
)
def test_unsupported_owner_probability_shape_refuses_without_dropping_values(flat, shape):
    from nirs4all_benchmarks.adapters.nirs4all_workspace import _reshape_proba

    with pytest.raises(WorkspaceAdapterError, match="workspace_probability_shape_unsupported"):
        _reshape_proba(flat, shape, 2)
    assert _reshape_proba([0.1, 0.9, 0.2, 0.8], [2, 2], 2) == [[0.1, 0.9], [0.2, 0.8]]


def test_malformed_dag_reports_and_flags_are_typed_refusals():
    from nirs4all_benchmarks.adapters import bundle_to_export

    for reports in (["unsupported"], [{"variant_label": 123, "metrics": {}}]):
        with pytest.raises(ValueError, match=r"dagml_(metric_report|variant_context)_unsupported"):
            bundle_to_export({}, owner_export=record(), metric_reports=reports)
    for flags in (False, "unsafe", [123]):
        with pytest.raises(ValueError, match="dagml_unsafe_flags_unsupported"):
            bundle_to_export({"unsafe_flags": flags}, owner_export=record())


def test_failed_owner_run_without_scores_ingests_without_automatic_scoring(tmp_path):
    from nirs4all_benchmarks.ingestion import IngestionPolicy

    root = owner_workspace(tmp_path / "source", status="failed")
    with sqlite3.connect(root / "store.sqlite") as c:
        c.execute("UPDATE predictions SET scores='{}', val_score=NULL, train_score=NULL, test_score=NULL")
    (export,) = workspace_to_exports(root)
    assert export.scores.observations == [] and export.residuals.inline
    with ArenaStore(tmp_path / "store") as store:
        result = ingest_export(store, export)
        assert result.ok and result.terminal_status == "failed"
        assert result.scientific_verification == "not_performed"
        assert store.count("metric_observations") == 0
        assert Queries(store).run_detail(result.execution_hash)["scores"] == []
        assert ingest_export(store, export).status == "already_ingested"
    with ArenaStore(tmp_path / "explicit-recompute") as store:
        result = ingest_export(store, export, policy=IngestionPolicy(recompute_scores=True))
        assert result.status == "rejected" and result.issues[0]["code"] == "recompute_score_context_unsupported"
        assert store.count("executions") == 0
