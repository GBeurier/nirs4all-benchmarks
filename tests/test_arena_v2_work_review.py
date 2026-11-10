"""Self-contained R3–R6 counterexamples; synthetic metadata, no owner training."""

from __future__ import annotations

import copy
import json
import os
import shutil
import subprocess
import sys
from pathlib import Path

import pytest

from nirs4all_benchmarks.adapters import bundle_to_export
from nirs4all_benchmarks.ingestion import AtomicIngestionError, ingest_export, ingest_exports_atomic, validate_metadata
from nirs4all_benchmarks.store import ArenaStore
from test_arena_v2 import observation, record


def residual_record(execution_id="a", owner="owner-a"):
    source = record(execution_id=execution_id)
    source["execution"]["owner_pipeline_name"] = owner
    source["residuals"] = {
        "key": "sample_id",
        "pseudonymized": False,
        "publishable": dict.fromkeys(("y_true", "y_pred", "residual", "y_proba"), True),
        "inline": [
            {
                "sample_id": f"s{i}",
                "scope": "cv",
                "fold_id": "0",
                "partition": "validation",
                "y_true": float(i),
                "y_pred": 1.5,
            }
            for i in range(3)
        ],
    }
    return source


@pytest.mark.parametrize("entry", ["health", "store", "race"])
def test_r3_fifo_and_replacement_do_not_block(tmp_path, entry):
    root = tmp_path / entry
    root.mkdir()
    if entry == "race":
        with ArenaStore(root):
            pass
    else:
        os.mkfifo(root / "arena.sqlite")
    code = """
import os,sys
from pathlib import Path
from nirs4all_benchmarks.store.arena_store import ArenaStore,ArenaStoreRefusal,store_header_status
root=Path(sys.argv[1]); kind=sys.argv[2]
if kind=='race':
    original=os.open
    def replace(path,flags,*args,**kwargs):
        if Path(path)==root/'arena.sqlite':
            (root/'arena.sqlite').unlink();os.mkfifo(root/'arena.sqlite')
            os.open=original
        return original(path,flags,*args,**kwargs)
    os.open=replace
if kind in {'health','race'}:
    assert store_header_status(root)['code']=='arena_store_unsafe_file'
else:
    try: ArenaStore(root)
    except ArenaStoreRefusal as exc: assert exc.code=='arena_store_unsafe_file'
    else: raise AssertionError('FIFO accepted')
"""
    result = subprocess.run(
        [sys.executable, "-B", "-c", code, str(root), entry], capture_output=True, text=True, timeout=4
    )
    assert result.returncode == 0, result.stderr


def block(label="winner", **extra):
    return {
        "variant_label": label,
        "sample_ids": ["s"],
        "values": [0.5],
        "y_true": [1.0],
        "scope": "cv",
        "partition": "validation",
        "fold_id": "0",
        **extra,
    }


@pytest.mark.parametrize("where", ["reports", "caches", "blocks"])
def test_r4_selected_variant_is_required_and_must_match(where):
    report = {"variant_label": "loser", "metrics": {"rmse": 1.0}}
    for selected, code in ((None, "dagml_selected_variant_required"), ("winner", "dagml_variant_selection_mismatch")):
        bundle = {} if selected is None else {"selected_variant_id": selected}
        options = {"metric_reports": [report]} if where == "reports" else {}
        if where == "caches":
            bundle["prediction_caches"] = [block("loser")]
        if where == "blocks":
            options["prediction_blocks"] = [block("loser")]
        with pytest.raises(ValueError, match=code):
            bundle_to_export(bundle, owner_export=record(), **options)
    with pytest.raises(ValueError, match="dagml_variant_context_unsupported"):
        bundle_to_export(
            {"selected_variant_id": "winner", "prediction_caches": [block(), block("loser")]}, owner_export=record()
        )


@pytest.mark.parametrize("where", ["reports", "caches", "blocks"])
def test_r4_unlabelled_reports_and_predictions_cannot_bypass_selection(where):
    item = (
        {"metrics": {"rmse": 1.0}} if where == "reports" else {k: v for k, v in block().items() if k != "variant_label"}
    )
    bundle = {"prediction_caches": [item]} if where == "caches" else {}
    options = (
        {"metric_reports": [item]} if where == "reports" else {"prediction_blocks": [item]} if where == "blocks" else {}
    )
    with pytest.raises(ValueError, match="dagml_selected_variant_required"):
        bundle_to_export(bundle, owner_export=record(), **options)
    with pytest.raises(ValueError, match="dagml_variant_selection_mismatch"):
        bundle_to_export({**bundle, "selected_variant_id": "winner"}, owner_export=record(), **options)


def test_r4_duplicate_residuals_refuse_but_distinct_context_is_retained():
    source = residual_record()
    source["residuals"]["inline"].append(copy.deepcopy(source["residuals"]["inline"][0]))
    assert validate_metadata(source).issues[0]["code"] == "duplicate_residual_context"
    source["residuals"]["inline"][-1]["partition"] = "train"
    assert validate_metadata(source).status == "metadata_valid"
    with pytest.raises(ValueError, match="duplicate_residual_context"):
        bundle_to_export({"selected_variant_id": "winner"}, owner_export=record(), prediction_blocks=[block(), block()])
    mapped = bundle_to_export(
        {"selected_variant_id": "winner"}, owner_export=record(), prediction_blocks=[block(), block(partition="train")]
    )
    assert len(mapped.residuals.inline) == 2


def test_r5_late_pack_refusal_removes_targets_and_keeps_committed_blobs(tmp_path):
    with ArenaStore(tmp_path / "store") as store:
        prior = residual_record("prior")
        assert ingest_export(store, prior).ok
        before = {p.name: (p.stat().st_ino, p.read_bytes()) for p in store.arrays_dir.iterdir()}
        a, b = residual_record("new"), residual_record("conflict", "owner-b")
        with pytest.raises(AtomicIngestionError, match="workspace_identity_collision"):
            ingest_exports_atomic(store, [a, b])
        assert {p.name: (p.stat().st_ino, p.read_bytes()) for p in store.arrays_dir.iterdir()} == before
        assert store.count("executions") == store.count("residual_sets") == store.count("metadata_receipts") == 1
    with ArenaStore(tmp_path / "empty") as store:
        with pytest.raises(AtomicIngestionError, match="workspace_identity_collision"):
            ingest_exports_atomic(store, [a, b])
        assert list(store.arrays_dir.iterdir()) == []
        assert all(
            store.count(table) == 0 for table in ("executions", "residual_sets", "metadata_receipts", "collections")
        )


def test_r5_exception_after_blob_write_and_cancellation_cleanup(tmp_path, monkeypatch):
    import nirs4all_benchmarks.ingestion.ingest as ingestion

    def fail(*args, **kwargs):
        raise KeyboardInterrupt("synthetic cancellation")

    monkeypatch.setattr(ingestion, "sanitize_manifest", fail)
    with ArenaStore(tmp_path / "store") as store:
        with pytest.raises(KeyboardInterrupt):
            ingest_export(store, residual_record())
        assert not store.conn.in_transaction and store.count("executions") == 0
        assert list(store.arrays_dir.iterdir()) == []


def test_r5_rollback_does_not_delete_a_waiting_writers_committed_blob(tmp_path, monkeypatch):
    import nirs4all_benchmarks.ingestion.ingest as ingestion

    source = residual_record("race")
    payload = tmp_path / "input.json"
    payload.write_text(json.dumps(source))
    root = tmp_path / "store"
    with ArenaStore(root) as store:
        code = """
import sys
from pathlib import Path
from nirs4all_benchmarks.store import ArenaStore
from nirs4all_benchmarks.ingestion import ingest_export
s=ArenaStore(sys.argv[1]);print('ready',flush=True);sys.stdin.readline()
r=ingest_export(s,Path(sys.argv[2]));print(r.status,flush=True);s.close()
"""
        process = subprocess.Popen(
            [sys.executable, "-B", "-c", code, str(root), str(payload)],
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
        )
        assert process.stdout.readline().strip() == "ready"
        original = ingestion._write_facts

        def signal(*args, **kwargs):
            original(*args, **kwargs)
            process.stdin.write("commit after rollback\n")
            process.stdin.flush()

        monkeypatch.setattr(ingestion, "_write_facts", signal)
        try:
            with pytest.raises(AtomicIngestionError):
                ingest_exports_atomic(store, [source, residual_record("conflict", "owner-b")])
            stdout, stderr = process.communicate(timeout=30)
            assert process.returncode == 0 and stdout.strip() == "committed", stderr
        finally:
            if process.poll() is None:
                process.terminate()
                process.wait(timeout=10)
        assert store.count("executions") == store.count("residual_sets") == 1
        stored = store.query_one("SELECT residual_set_id FROM residual_sets")
        assert [row["y_true"] for row in store.residuals.read(stored["residual_set_id"])] == [0.0, 1.0, 2.0]
        assert len(list(store.arrays_dir.iterdir())) == 1


def test_r5_post_commit_interrupt_keeps_committed_blob_and_replays(tmp_path):
    with ArenaStore(tmp_path / "store") as store:
        connection = store.conn

        class InterruptedCommit:
            def __getattr__(self, name):
                return getattr(connection, name)

            def commit(self):
                connection.commit()
                raise KeyboardInterrupt("after durable commit")

        store.conn = InterruptedCommit()
        source = residual_record()
        with pytest.raises(KeyboardInterrupt):
            ingest_export(store, source)
        store.conn = connection
        assert store.count("executions") == store.count("residual_sets") == 1
        assert len(list(store.arrays_dir.iterdir())) == 1
        assert ingest_export(store, source).status == "already_ingested"


def test_r5_immutable_preexisting_blob_is_not_overwritten_or_removed(tmp_path):
    with ArenaStore(tmp_path / "store") as store:
        rows = [{"sample_id": "s", "scope": "cv", "partition": "validation", "y_true": 1.0}]
        path = store.residuals.write("preexisting", rows)
        before = path.stat().st_ino, path.read_bytes()
        with pytest.raises(ValueError, match="residual_blob_conflict"), store.residuals.transaction():
            store.residuals.write("preexisting", [{**rows[0], "y_true": 99.0}])
        assert (path.stat().st_ino, path.read_bytes()) == before
        assert len(list(store.arrays_dir.iterdir())) == 1


def _guard_selftest_in_child(request, tmp_path):
    """Delegate only these five selftests; never guard the bare pytest parent."""
    if getattr(sys, "_arena_test_guards", False):
        return False
    assert request.node.name in {
        "test_guards_are_active_before_runtime_imports_and_are_persistent",
        "test_guard_fd_relative_writes_and_cleanup_remain_inside_scratch",
        "test_guard_allows_only_audited_node_metadata_argv",
        "test_synthetic_fixture_venv_grant_rejects_other_origins_options_and_paths",
        "test_synthetic_fixture_grant_rejects_unknown_execution_and_pip",
    }
    assert request.node.nodeid == f"tests/test_arena_v2_work_review.py::{request.node.name}"
    assert "ARENA_GUARD_SELFTEST_CHILD" not in os.environ, "guard_selftest_bootstrap_failed_no_recursion"
    assert tmp_path == tmp_path.resolve() and not tmp_path.is_symlink() and tmp_path.is_dir()
    assert tmp_path.stat().st_uid == os.geteuid()
    scratch = tmp_path / "guarded-selftest"
    scratch.mkdir(mode=0o700)
    guard = scratch / "bootstrap"
    guard.mkdir(mode=0o700)
    temporary = scratch / "tmp"
    temporary.mkdir(mode=0o700)
    repository = Path(__file__).resolve().parents[1]
    bootstrap = (
        "import runpy,sys\n"
        "try:\n"
        f"    runpy.run_path({str(repository / 'tests/conftest.py')!r},run_name='arena_guard_bootstrap')\n"
        "except BaseException as error:\n"
        "    raise SystemExit('guard_selftest_bootstrap_failed') from error\n"
        "if not getattr(sys,'_arena_test_guards',False):\n"
        "    raise SystemExit('guard_selftest_bootstrap_failed')\n"
    )
    (guard / "sitecustomize.py").write_text(bootstrap)
    environment = {
        **os.environ,
        **dict.fromkeys(("OMP_NUM_THREADS", "OPENBLAS_NUM_THREADS", "MKL_NUM_THREADS", "NUMEXPR_NUM_THREADS",
                         "VECLIB_MAXIMUM_THREADS", "BLIS_NUM_THREADS"), "1"),
        "ARENA_TEST_SCRATCH": str(scratch),
        "ARENA_TEST_GUARD_DIR": str(guard),
        "ARENA_GUARD_SELFTEST_CHILD": request.node.nodeid,
        "PYTHONPATH": str(guard) + os.pathsep + os.environ.get("PYTHONPATH", ""),
        "PYTHONDONTWRITEBYTECODE": "1",
        "PYTEST_DISABLE_PLUGIN_AUTOLOAD": "1",
        "TMPDIR": str(temporary),
        "XDG_CACHE_HOME": str(scratch / "cache"),
    }
    junit = scratch / "junit.xml"
    child = subprocess.run(
        [sys.executable, "-B", "-m", "pytest", "-p", "no:cacheprovider", "--basetemp", str(scratch / "pytest"),
         "--junitxml", str(junit), request.node.nodeid],
        cwd=repository, env=environment, capture_output=True, text=True, timeout=60,
    )
    assert child.returncode == 0, child.stdout + child.stderr
    import xml.etree.ElementTree as ET

    assert junit.is_file(), "guard_selftest_junit_missing"
    report = ET.parse(junit).getroot()
    suites = list(report.iter("testsuite"))
    assert len(suites) == 1, "guard_selftest_junit_suite_count"
    assert {key: suites[0].get(key) for key in ("tests", "failures", "errors", "skipped")} == {
        "tests": "1", "failures": "0", "errors": "0", "skipped": "0",
    }, "guard_selftest_junit_counts"
    cases = list(report.iter("testcase"))
    assert len(cases) == 1, "guard_selftest_junit_case_count"
    assert (cases[0].get("classname"), cases[0].get("name")) == (
        "tests.test_arena_v2_work_review", request.node.name,
    ), "guard_selftest_junit_identity"
    assert not any(list(report.iter(tag)) for tag in ("skipped", "failure", "error")), "guard_selftest_junit_nonpass"
    return True


def test_guards_are_active_before_runtime_imports_and_are_persistent(tmp_path, request):
    if _guard_selftest_in_child(request, tmp_path):
        return
    import importlib
    import socket

    import numpy as np

    assert getattr(sys, "_arena_test_guards", False)
    for _ in range(2):
        with pytest.raises(PermissionError, match="scientific_runtime"):
            importlib.import_module("sklearn")
        assert np.polyfit.__name__ == "no_fit"  # Never call a real fit even if a guard regresses.
        with pytest.raises(PermissionError, match="scientific_fit"):
            np.polyfit([1.0], [2.0], 1)
        with pytest.raises(PermissionError, match="network_socket"):
            socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    with pytest.raises(PermissionError, match="non_python_process"):
        subprocess.run(["/bin/sh", "-c", "exit 0"], check=True)


def test_guard_fd_relative_writes_and_cleanup_remain_inside_scratch(tmp_path, request):
    if _guard_selftest_in_child(request, tmp_path):
        return
    assert getattr(sys, "_arena_test_guards", False)
    directory = tmp_path / "fd-context"
    directory.mkdir()
    fd = os.open(directory, os.O_RDONLY | os.O_DIRECTORY)
    outside = os.open(os.sep, os.O_RDONLY | os.O_DIRECTORY)
    try:
        child = os.open("payload", os.O_CREAT | os.O_WRONLY, 0o600, dir_fd=fd)
        os.close(child)
        os.rename("payload", "renamed", src_dir_fd=fd, dst_dir_fd=fd)
        os.unlink("renamed", dir_fd=fd)
        os.mkdir("nested", dir_fd=fd)
        os.rmdir("nested", dir_fd=fd)
        with pytest.raises(PermissionError, match="write_outside_scratch"):
            os.open("arena-guard-must-not-create", os.O_CREAT | os.O_WRONLY, 0o600, dir_fd=outside)
        assert list(directory.iterdir()) == []
    finally:
        os.close(fd)
        os.close(outside)


def test_guard_allows_only_audited_node_metadata_argv(tmp_path, request):
    if _guard_selftest_in_child(request, tmp_path):
        return
    node = shutil.which("node")
    assert node is not None, "Node.js is required for the audited metadata guard selftest"
    assert getattr(sys, "_arena_test_guards", False)
    metadata = subprocess.run(
        [node, "-p", "process.versions.node.split('.')[0]"],
        capture_output=True,
        text=True,
        check=True,
    )
    assert metadata.stdout.strip().isdigit()
    # Never execute a browser, numerical or WASM body as part of this guard test.
    for argv in ([node, "--eval", "void 0"], [node, "driver.mjs", "{}"]):
        with pytest.raises(PermissionError, match="node_argv_not_authorized"):
            subprocess.run(argv, check=True)
    with pytest.raises(PermissionError, match="node_preload_environment_not_authorized"):
        subprocess.run(
            [node, "-p", "process.versions.node.split('.')[0]"],
            env={**os.environ, "NODE_OPTIONS": "--require=unauthorized-preload"},
            check=True,
        )


@pytest.mark.parametrize("fold", [" 0", "0 ", "\tzero", "zero\n"])
def test_r6_padded_score_and_residual_fold_ids_cannot_fake_coverage(fold):
    source = record()
    source["cv"] = {"n_folds": 5}
    source["scores"]["observations"] = [observation(1.0, fold=f) for f in ("0", fold, "1", "2", "3")]
    assert validate_metadata(source).issues[0]["code"] == "noncanonical_fold_id"
    source = residual_record()
    source["residuals"]["inline"][0]["fold_id"] = fold
    assert validate_metadata(source).issues[0]["code"] == "noncanonical_fold_id"


@pytest.mark.parametrize("torn", [False, True], ids=["crash-30", "torn-uncommitted-tail"])
def test_r1_row1_sqlite_recovers_committed_rows_after_crash(tmp_path, torn):
    root = tmp_path / "crash"
    code = """
import os,sys
from nirs4all_benchmarks.store import ArenaStore
from nirs4all_benchmarks.ingestion import ingest_export
s=ArenaStore(sys.argv[1]);s.conn.execute('PRAGMA wal_autocheckpoint=0')
import json
payload=json.loads(sys.argv[3])
for i in range(30):
    payload['execution']['execution_id']=str(i)
    assert ingest_export(s,payload).ok
if sys.argv[2]=='torn':
    s.conn.execute('PRAGMA cache_size=1');s.conn.execute('BEGIN IMMEDIATE')
    s.set_meta('uncommitted_tail','x'*2000000)
os._exit(0)
"""
    subprocess.run(
        [sys.executable, "-B", "-c", code, str(root), "torn" if torn else "clean", json.dumps(record())], check=True
    )
    wal = root / "arena.sqlite-wal"
    assert wal.stat().st_size > 32
    if torn:
        os.truncate(wal, wal.stat().st_size - 100)
    for _ in range(2):
        with ArenaStore(root) as store:
            assert store.count("executions") == store.count("metadata_receipts") == 30
            assert store.get_meta("uncommitted_tail") is None
            assert store.query_one("PRAGMA quick_check")["quick_check"] == "ok"


def test_r1_row1_fifteen_ingests_second_open(tmp_path):
    with ArenaStore(tmp_path / "store") as first:
        for i in range(15):
            assert ingest_export(first, record(execution_id=str(i))).ok
        with ArenaStore(first.root) as second:
            assert second.count("executions") == 15


def test_r1_row2_sparse_large_store_has_only_bounded_header_reads(tmp_path, monkeypatch):
    import nirs4all_benchmarks.store.arena_store as module

    root = tmp_path / "large"
    with ArenaStore(root):
        pass
    path = root / "arena.sqlite"
    os.truncate(path, 512_000_001)
    assert path.stat().st_size > 512_000_000
    read = module.os.read
    sizes = []

    def counted(fd, size):
        sizes.append(size)
        return read(fd, size)

    monkeypatch.setattr(module.os, "read", counted)
    try:
        with ArenaStore(root) as store:
            assert store.count("executions") == 0
        assert sizes and set(sizes) == {100}
    finally:
        # Do not retain a >512MB logical sparse fixture in the whole-suite scratch.
        path.unlink()


def test_r1_row3_live_connections_and_health_never_raw_open_main(tmp_path, monkeypatch):
    from fastapi.testclient import TestClient

    from nirs4all_benchmarks.service import create_app
    from nirs4all_benchmarks.store.arena_store import _CONNECTIONS, store_header_status

    root = tmp_path / "live #?percent%"
    with ArenaStore(root) as first:
        client = TestClient(create_app(root))
        key = first._inode_key
        original = os.open
        raw = []

        def counted(path, flags, *args, **kwargs):
            if Path(path) == root / "arena.sqlite":
                raw.append(path)
            return original(path, flags, *args, **kwargs)

        monkeypatch.setattr(os, "open", counted)
        for _ in range(8):
            assert store_header_status(root)["status"] == "schema_3_header"
            response = client.get("/api/healthz")
            assert response.status_code == 200 and response.json()["fingerprint_checked"] is False
            with ArenaStore(root):
                assert _CONNECTIONS[key] == 2
                assert store_header_status(root)["status"] == "schema_3_header"
            assert _CONNECTIONS[key] == 1
        assert raw == []
        client.close()
    assert key not in _CONNECTIONS


def test_r1_row4_noatime_is_optional_and_header_flags_are_safe(tmp_path, monkeypatch):
    import errno

    from nirs4all_benchmarks.store.arena_store import store_header_status

    root = tmp_path / "flags"
    with ArenaStore(root):
        pass
    original = os.open
    flags_seen = []

    def permission(path, flags, *args, **kwargs):
        if Path(path) == root / "arena.sqlite":
            flags_seen.append(flags)
            if flags & os.O_NOATIME:
                raise OSError(errno.EPERM, "synthetic owner permission")
        return original(path, flags, *args, **kwargs)

    monkeypatch.setattr(os, "open", permission)
    assert store_header_status(root)["status"] == "schema_3_header"
    assert len(flags_seen) == 2 and not flags_seen[-1] & os.O_NOATIME
    for flags in flags_seen:
        assert flags & os.O_NONBLOCK and flags & os.O_NOFOLLOW and flags & os.O_CLOEXEC


@pytest.mark.parametrize(
    "suffix,kind", [("-wal", "symlink"), ("-wal", "fifo"), ("-shm", "fifo"), ("-journal", "directory")]
)
def test_r1_row5_unsafe_sidecar_refuses_before_connect(tmp_path, monkeypatch, suffix, kind):
    import nirs4all_benchmarks.store.arena_store as module

    root = tmp_path / "sidecar"
    with ArenaStore(root):
        pass
    path = root / ("arena.sqlite" + suffix)
    if kind == "symlink":
        target = tmp_path / "target"
        target.write_bytes(b"unchanged")
        path.symlink_to(target)
    elif kind == "fifo":
        os.mkfifo(path)
    else:
        path.mkdir()

    def forbidden(*args, **kwargs):
        raise AssertionError("SQLite must not be opened")

    monkeypatch.setattr(module.sqlite3, "connect", forbidden)
    with pytest.raises(module.ArenaStoreRefusal, match="arena_store_unsafe"):
        ArenaStore(root)


def test_r1_row6_removed_after_header_never_recreates_empty_database(tmp_path, monkeypatch):
    import nirs4all_benchmarks.store.arena_store as module

    root = tmp_path / "removed"
    with ArenaStore(root):
        pass
    path = root / "arena.sqlite"
    original = module.sqlite3.connect

    def vanish(database, *args, **kwargs):
        assert database == path.as_uri() + "?mode=rw" and kwargs["uri"] is True
        path.unlink()
        return original(database, *args, **kwargs)

    monkeypatch.setattr(module.sqlite3, "connect", vanish)
    with pytest.raises(module.ArenaStoreRefusal, match="arena_store_corrupt"):
        ArenaStore(root)
    assert not path.exists()


def test_r1_row7_forged_fingerprint_refuses_before_application_work(tmp_path, monkeypatch):
    import nirs4all_benchmarks.store.arena_store as module

    root = tmp_path / "forged"
    with ArenaStore(root):
        pass
    (root / "arrays").rmdir()
    (root / "exports").rmdir()
    code = "import sqlite3,sys,os;c=sqlite3.connect(sys.argv[1]);c.execute('PRAGMA journal_mode=WAL');c.execute('PRAGMA wal_autocheckpoint=0');c.execute(\"UPDATE arena_meta SET value='wrong' WHERE key='schema_fingerprint'\");c.commit();os._exit(0)"
    subprocess.run([sys.executable, "-B", "-c", code, str(root / "arena.sqlite")], check=True)
    assert (root / "arena.sqlite-wal").stat().st_size > 32
    original = module.sqlite3.connect
    sql = []

    def traced(*args, **kwargs):
        connection = original(*args, **kwargs)
        connection.set_trace_callback(sql.append)
        return connection

    monkeypatch.setattr(module.sqlite3, "connect", traced)
    with pytest.raises(module.ArenaStoreRefusal, match="arena_store_schema_fingerprint_mismatch"):
        ArenaStore(root)
    assert not (root / "arrays").exists() and not (root / "exports").exists()
    assert sql == ["PRAGMA busy_timeout=30000", "SELECT key,value FROM arena_meta"]
    connection = original((root / "arena.sqlite").as_uri() + "?mode=rw", uri=True)
    try:
        assert connection.execute("SELECT COUNT(*) FROM executions").fetchone()[0] == 0
    finally:
        connection.close()
    assert module._CONNECTIONS == {}
    # No byte-equality promise: SQLite recovery/checkpoint/sidecars are permitted.


@pytest.mark.parametrize("kind", ["v1-live-wal", "foreign", "empty"])
def test_r1_row8_unclaimed_refusal_does_not_open_sqlite_or_change_files(tmp_path, monkeypatch, kind):
    import hashlib
    import sqlite3

    import nirs4all_benchmarks.store.arena_store as module

    root = tmp_path / kind
    root.mkdir()
    path = root / "arena.sqlite"
    keeper = None
    if kind == "empty":
        path.touch()
    else:
        keeper = sqlite3.connect(path)
        keeper.execute("CREATE TABLE foreign_rows(x)")
        if kind == "v1-live-wal":
            keeper.execute("PRAGMA user_version=1")
            keeper.execute("PRAGMA journal_mode=WAL")
        keeper.execute("INSERT INTO foreign_rows VALUES(1)")
        keeper.commit()

    def state():
        return {
            p.name: (hashlib.sha256(p.read_bytes()).hexdigest(), p.stat().st_mtime_ns)
            for p in root.iterdir()
            if p.is_file()
        }

    before = state()

    def forbidden(*args, **kwargs):
        raise AssertionError("SQLite must not be opened")

    monkeypatch.setattr(module.sqlite3, "connect", forbidden)
    try:
        with pytest.raises(module.ArenaStoreRefusal, match="unsupported_arena_store_schema"):
            ArenaStore(root)
        assert state() == before
    finally:
        if keeper is not None:
            keeper.close()


@pytest.mark.parametrize("round_id", range(3))
def test_r1_row9_eight_process_fresh_race(tmp_path, round_id):
    root = tmp_path / f"race{round_id}"
    code = "import sys;from nirs4all_benchmarks.store import ArenaStore;s=ArenaStore(sys.argv[1]);assert s.count('executions')==0;s.close()"
    processes = [
        subprocess.Popen(
            [sys.executable, "-B", "-c", code, str(root)], stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True
        )
        for _ in range(8)
    ]
    outcomes = []
    try:
        for process in processes:
            stdout, stderr = process.communicate(timeout=40)
            outcomes.append((process.returncode, stdout, stderr))
    finally:
        for process in processes:
            if process.poll() is None:
                process.terminate()
                process.wait(timeout=10)
    assert all(code == 0 for code, _, _ in outcomes), outcomes
    with ArenaStore(root) as store:
        assert store.count("executions") == 0
    assert not list(tmp_path.glob(".arena-provision-*"))


def test_r1_busy_and_corrupt_are_fixed_distinct_refusals(tmp_path, monkeypatch):
    import nirs4all_benchmarks.store.arena_store as module

    root = tmp_path / "busy"
    with ArenaStore(root):
        pass
    code = "import sqlite3,sys;c=sqlite3.connect(sys.argv[1]);c.execute('PRAGMA journal_mode=DELETE');c.execute('BEGIN EXCLUSIVE');print('ready',flush=True);sys.stdin.readline();c.rollback();c.close()"
    process = subprocess.Popen(
        [sys.executable, "-B", "-c", code, str(root / "arena.sqlite")],
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )
    try:
        assert process.stdout.readline().strip() == "ready"
        with pytest.raises(module.ArenaStoreRefusal, match="arena_store_busy"):
            ArenaStore(root, busy_timeout_ms=50)
    finally:
        process.communicate("release\n", timeout=10)
    corrupt = tmp_path / "corrupt"
    with ArenaStore(corrupt, busy_timeout_ms=50):
        pass
    with (corrupt / "arena.sqlite").open("r+b") as stream:
        stream.seek(4096)
        stream.write(b"\xff" * 4096)
    with pytest.raises(module.ArenaStoreRefusal, match="arena_store_corrupt"):
        ArenaStore(corrupt, busy_timeout_ms=50)


def test_r1_registry_releases_garbage_collected_store(tmp_path):
    import gc
    import weakref

    from nirs4all_benchmarks.store.arena_store import _CONNECTIONS

    store = ArenaStore(tmp_path / "gc")
    key = store._inode_key
    reference = weakref.ref(store)
    assert _CONNECTIONS[key] == 1
    del store
    gc.collect()
    assert reference() is None and key not in _CONNECTIONS


def test_r1_header_swap_to_another_live_inode_never_raw_closes_it(tmp_path, monkeypatch):
    import nirs4all_benchmarks.store.arena_store as module

    a, b = tmp_path / "a", tmp_path / "b"
    with ArenaStore(a):
        pass
    with ArenaStore(b) as live:
        key = live._inode_key
        original_open, original_close = os.open, os.close
        intercepted = []
        swapped = False

        def swap(path, flags, *args, **kwargs):
            nonlocal swapped
            if Path(path) == a / "arena.sqlite" and not swapped:
                swapped = True
                (a / "arena.sqlite").rename(a / "original.sqlite")
                os.link(b / "arena.sqlite", a / "arena.sqlite")
            return original_open(path, flags, *args, **kwargs)

        def close(fd):
            info = os.fstat(fd)
            if (info.st_dev, info.st_ino) == key and module._CONNECTIONS.get(key, 0):
                intercepted.append(fd)
                raise AssertionError("raw close would cancel a live SQLite lock")
            return original_close(fd)

        monkeypatch.setattr(os, "open", swap)
        monkeypatch.setattr(os, "close", close)
        try:
            with pytest.raises(module.ArenaStoreRefusal, match="arena_store_unsafe_file"):
                ArenaStore(a)
            assert intercepted == []
            assert len(module._DEFERRED_RAW[key]) == 1
            assert live.count("executions") == 0
        finally:
            live.close()
            for fd in intercepted:
                original_close(fd)
            (a / "arena.sqlite").unlink()
            (a / "original.sqlite").rename(a / "arena.sqlite")
    assert key not in module._CONNECTIONS
    assert key not in module._DEFERRED_RAW


def test_r1_registry_gc_on_another_thread_preserves_correct_refcounts(tmp_path):
    code = """
import gc,sys,threading
from nirs4all_benchmarks.store import ArenaStore
from nirs4all_benchmarks.store.arena_store import _CONNECTIONS,_DEFERRED_RAW
store=ArenaStore(sys.argv[1]);key=store._inode_key;holder=[store];del store
thread=threading.Thread(target=lambda:(holder.clear(),gc.collect()))
thread.start();thread.join(timeout=10)
assert not thread.is_alive()
assert key not in _CONNECTIONS and key not in _DEFERRED_RAW
with ArenaStore(sys.argv[1]) as reopened:assert reopened.count('executions')==0
"""
    process = subprocess.run(
        [sys.executable, "-B", "-c", code, str(tmp_path / "thread-gc")], capture_output=True, text=True, timeout=20
    )
    assert process.returncode == 0, process.stderr
    assert "Exception ignored" not in process.stderr


@pytest.mark.parametrize(
    "code,budget", [(5, 250), (6, 250), (11, 250), (5, 0)], ids=["busy", "locked", "corrupt", "busy-expired"]
)
def test_r1_journal_mode_retries_only_transient_busy(tmp_path, monkeypatch, code, budget):
    import sqlite3

    import nirs4all_benchmarks.store.arena_store as module

    root = tmp_path / "journal"
    with ArenaStore(root):
        pass
    original = module.sqlite3.connect
    calls = []

    class Connection:
        def __init__(self, real):
            object.__setattr__(self, "real", real)

        def __getattr__(self, name):
            return getattr(self.real, name)

        def __setattr__(self, name, value):
            setattr(self.real, name, value)

        def execute(self, sql, *args):
            if sql == "PRAGMA journal_mode=WAL":
                calls.append(sql)
                if len(calls) <= 2:
                    error = sqlite3.OperationalError("synthetic SQLite result")
                    error.sqlite_errorcode = code
                    raise error
            return self.real.execute(sql, *args)

    monkeypatch.setattr(module.sqlite3, "connect", lambda *args, **kwargs: Connection(original(*args, **kwargs)))
    if code == 11 or budget == 0:
        expected = "arena_store_corrupt" if code == 11 else "arena_store_busy"
        with pytest.raises(module.ArenaStoreRefusal, match=expected):
            ArenaStore(root, busy_timeout_ms=budget)
        assert len(calls) == 1
    else:
        with ArenaStore(root, busy_timeout_ms=budget) as store:
            assert store.count("executions") == 0
        assert len(calls) == 3


def test_synthetic_fixture_venv_grant_rejects_other_origins_options_and_paths(tmp_path, monkeypatch, request):
    if _guard_selftest_in_child(request, tmp_path):
        return
    import venv

    assert getattr(sys, "_arena_test_guards", False)
    assert venv.EnvBuilder.create.__name__ == "fixture_environment"
    monkeypatch.setattr(
        sys,
        "_arena_test_context",
        "tests/test_python_legacy_adapter.py::test_python_oracle_finalizes_only_explicit_legacy",
    )
    with pytest.raises(PermissionError, match="fixture_venv_origin"):
        venv.EnvBuilder(with_pip=False, clear=True).create(tmp_path / "runtime")
    for options in (
        {"with_pip": True, "clear": True},
        {"with_pip": False, "clear": False},
        {"with_pip": False, "clear": True, "symlinks": True},
    ):
        with pytest.raises(PermissionError, match="fixture_venv_options"):
            venv.EnvBuilder(**options).create(tmp_path / "runtime")
    workspace = Path(__file__).resolve().parents[1]
    with pytest.raises(PermissionError, match="fixture_path_outside_owned_case"):
        venv.EnvBuilder(with_pip=False, clear=True).create(workspace / ".venv")
    alias = tmp_path / "alias"
    alias.symlink_to(workspace, target_is_directory=True)
    with pytest.raises(PermissionError, match="fixture_symlink_or_owner"):
        venv.EnvBuilder(with_pip=False, clear=True).create(alias / "runtime")
    assert not (tmp_path / "runtime").exists()


def test_synthetic_fixture_grant_rejects_unknown_execution_and_pip(tmp_path, monkeypatch, request):
    if _guard_selftest_in_child(request, tmp_path):
        return
    assert getattr(sys, "_arena_test_guards", False)
    monkeypatch.setattr(
        sys,
        "_arena_test_context",
        "tests/test_python_legacy_adapter.py::test_python_oracle_finalizes_only_explicit_legacy",
    )
    for argv in (
        [sys.executable, "-c", "print('not admitted')"],
        [sys.executable, "-m", "pip", "--version"],
        ["/usr/bin/python3.11", "-m", "venv", "--without-pip", "--copies", str(tmp_path / "runtime")],
    ):
        with pytest.raises(PermissionError, match="unlisted_fixture_subprocess"):
            subprocess.run(argv, check=True)
    monkeypatch.setattr(
        sys,
        "_arena_test_context",
        "tests/test_arena_v2_work_review.py::test_synthetic_fixture_grant_rejects_unknown_execution_and_pip",
    )
    with pytest.raises(PermissionError, match="package_installation"):
        subprocess.run([sys.executable, "-m", "pip", "--version"], check=True)


@pytest.mark.parametrize("peer", ["owned", "v1", "foreign"])
def test_f1_peer_publication_is_validated_without_clobber(tmp_path, monkeypatch, peer):
    import nirs4all_benchmarks.store.arena_store as module

    root = tmp_path / "race"
    original = module._components
    calls = 0
    published = {}
    code = """
import json,sqlite3,sys
from pathlib import Path
from nirs4all_benchmarks.store import ArenaStore
root=Path(sys.argv[1]);kind=sys.argv[2]
if kind=='owned':
    with ArenaStore(root) as store:
        store.set_meta('peer_sentinel','retained');store.conn.commit()
        print(json.dumps({'salt':store.get_meta('pseudonymization_salt')}))
elif kind=='v1':
    c=sqlite3.connect(root/'arena.sqlite');c.execute('PRAGMA user_version=1')
    c.execute('CREATE TABLE peer_sentinel(value)');c.commit();c.close()
else:
    (root/'store.sqlite').write_bytes(b'foreign-peer')
"""

    def interleave(candidate):
        nonlocal calls
        calls += 1
        if candidate == root and calls == 2:
            child = subprocess.run(
                [sys.executable, "-B", "-c", code, str(root), peer],
                capture_output=True, text=True, check=True, timeout=10,
            )
            published["stdout"] = child.stdout
            published["ctime"] = root.stat().st_ctime_ns
            published["files"] = {p.name: p.read_bytes() for p in root.iterdir() if p.is_file()}
        return original(candidate)

    monkeypatch.setattr(module, "_components", interleave)
    if peer == "owned":
        with ArenaStore(root) as store:
            assert store.get_meta("peer_sentinel") == "retained"
            assert store.get_meta("pseudonymization_salt") == json.loads(published["stdout"])["salt"]
            assert store.query_one("PRAGMA quick_check")["quick_check"] == "ok"
        with ArenaStore(root) as store:
            assert store.get_meta("peer_sentinel") == "retained"
            assert store.count("executions") == 0
    else:
        refusal_code = "unsupported_arena_store_schema" if peer == "v1" else "arena_store_root_not_empty"
        with pytest.raises(module.ArenaStoreRefusal, match=refusal_code):
            ArenaStore(root)
        assert root.stat().st_ctime_ns == published["ctime"]
        assert {p.name: p.read_bytes() for p in root.iterdir() if p.is_file()} == published["files"]
        assert not (root / "arrays").exists() and not (root / "exports").exists()
    assert published and list(tmp_path.glob(".arena-provision-*")) == []


@pytest.mark.parametrize("error_code,expected", [(5, "arena_store_busy"), (6, "arena_store_busy"), (11, "arena_store_corrupt")])
def test_f2_transaction_storage_codes_roll_back_without_conflation(tmp_path, error_code, expected):
    import sqlite3

    from nirs4all_benchmarks.store import ArenaStoreRefusal

    with ArenaStore(tmp_path / "store") as store:
        error = sqlite3.DatabaseError("synthetic SQLite result-code injection")
        error.sqlite_errorcode = error_code
        with pytest.raises(ArenaStoreRefusal, match=expected) as refusal, store.transaction():
            store.set_meta("rolled_back_probe", "temporary")
            raise error
        assert refusal.value.code == expected
        assert store.get_meta("rolled_back_probe") is None
        assert not store.conn.in_transaction
        # Existing indexing errors keep their original type for metadata refusal.
        with pytest.raises(sqlite3.IntegrityError), store.transaction():
            store.conn.execute("INSERT INTO arena_meta(key,value) VALUES('null-probe',NULL)")
        assert store.get_meta("null-probe") is None
