"""FastAPI service smoke tests via TestClient."""

from __future__ import annotations

import builtins
import hashlib
import importlib
import json
import sqlite3
from pathlib import Path

import pytest

pytest.importorskip("fastapi")
from fastapi.testclient import TestClient

from conftest import rankable_fixture_exports as generate_fixture_exports
from nirs4all_benchmarks.service import create_app
from nirs4all_benchmarks.store import ArenaStore


def _store_snapshot(root: Path) -> dict[str, object]:
    conn = sqlite3.connect(f"file:{root / 'arena.sqlite'}?mode=ro", uri=True)
    try:
        user_version = conn.execute("PRAGMA user_version").fetchone()[0]
        database = tuple(conn.iterdump())
    finally:
        conn.close()

    files = {}
    for path in sorted(p for p in root.rglob("*") if p.is_file()):
        if path.name.endswith("-shm"):
            continue
        files[str(path.relative_to(root))] = hashlib.sha256(path.read_bytes()).hexdigest()
    return {"user_version": user_version, "database": database, "files": files}


def _forbid_repository_imports(monkeypatch: pytest.MonkeyPatch) -> None:
    original_import = builtins.__import__
    original_import_module = importlib.import_module

    def fail_if_repository(name: str) -> None:
        if name == "nirs4all_repository" or name.startswith("nirs4all_repository."):
            raise AssertionError("/api/pipelines must read the local benchmark store, not nirs4all-repository")

    def guarded_import(name: str, *args: object, **kwargs: object) -> object:
        fail_if_repository(name)
        return original_import(name, *args, **kwargs)

    def guarded_import_module(name: str, package: str | None = None) -> object:
        fail_if_repository(name)
        return original_import_module(name, package)

    monkeypatch.setattr(builtins, "__import__", guarded_import)
    monkeypatch.setattr(importlib, "import_module", guarded_import_module)


def _forbid_store_mutators(monkeypatch: pytest.MonkeyPatch) -> None:
    def fail(*_args: object, **_kwargs: object) -> None:
        raise AssertionError("/api/pipelines must stay read-only")

    for name in ("set_meta", "upsert", "insert", "update", "ensure_collection", "transaction"):
        monkeypatch.setattr(ArenaStore, name, fail)


@pytest.fixture
def client(seeded_root: Path) -> TestClient:
    app = create_app(seeded_root)
    return TestClient(app)


def test_healthz(client: TestClient):
    r = client.get("/api/healthz")
    assert r.status_code == 200
    assert r.json()["status"] == "schema_3_header"


def test_overview(client: TestClient):
    r = client.get("/api/overview")
    assert r.status_code == 200
    assert r.json()["pipelines"] == 7


def test_pipelines_endpoint_is_read_only(
    client: TestClient,
    seeded_root: Path,
    monkeypatch: pytest.MonkeyPatch,
):
    _forbid_repository_imports(monkeypatch)
    _forbid_store_mutators(monkeypatch)
    before_snapshot = _store_snapshot(seeded_root)
    before = client.get("/api/overview").json()

    r = client.get("/api/pipelines")

    after = client.get("/api/overview").json()
    after_snapshot = _store_snapshot(seeded_root)

    assert r.status_code == 200
    assert before == after
    assert after_snapshot == before_snapshot
    body = r.json()
    assert len(body) == 7
    assert {row["n_run_conditions"] for row in body} == {2}
    assert all(row["pipeline_dag_hash"] and row["human_label"] for row in body)


def test_leaderboard_endpoint(client: TestClient):
    r = client.get("/api/leaderboard", params={"metric": "rmse", "scope": "cv", "limit": 5})
    assert r.status_code == 200
    body = r.json()
    assert body["direction"] == "min"
    means = [row["mean"] for row in body["rows"]]
    assert means == sorted(means)


def test_matrix_endpoint(client: TestClient):
    r = client.get("/api/matrix", params={"metric": "rmse"})
    assert r.status_code == 200
    assert len(r.json()["cells"]) == 14


def test_run_detail_404(client: TestClient):
    assert client.get("/api/run/deadbeef").status_code == 404


def test_invalid_scope_returns_400(client: TestClient):
    r = client.get("/api/leaderboard", params={"scope": "bogus"})
    assert r.status_code == 400


def test_ingest_endpoint_idempotent(client: TestClient):
    manifest = json.loads(json.dumps(generate_fixture_exports()[0].to_manifest()))
    r = client.post("/api/ingest", params={"collection": "fixtures"}, json=manifest)
    assert r.status_code == 200
    assert r.json()["status"] == "already_ingested"


def test_static_index_served(client: TestClient):
    r = client.get("/")
    assert r.status_code == 200
    assert "Arena" in r.text or "nirs4all" in r.text


def test_facets_and_pivot_endpoints(client: TestClient):
    keys = {f["facet_key"] for f in client.get("/api/facets").json()}
    assert "param:n_components" in keys  # always exposed (LIKE 'param:%')
    params = {"group_by": "param:n_components", "metric": "rmse", "scope": "cv"}
    unfiltered = client.get("/api/pivot", params=params).json()
    assert unfiltered["rows"] == []
    assert unfiltered["score_context"]["status"] == "withheld"
    assert unfiltered["score_context"]["withheld"]["count"] > 0
    dataset = client.get("/api/datasets").json()[0]["dataset_fingerprint"]
    pv = client.get("/api/pivot", params={**params, "dataset": dataset}).json()
    assert pv["rows"]
    assert pv["score_context"]["status"] == "single"


def test_upload_file_multipart_does_not_500(client: TestClient):
    # Regression for the critical UploadFile forward-ref bug: a multipart file POST
    # must not 500. Send a pipeline recipe as an extension-less file part.
    recipe = json.dumps([{"class": "nirs4all.transform.SNV"},
                         {"model": "sklearn.cross_decomposition.PLSRegression", "params": {"n_components": 8}}])
    r = client.post("/api/upload", files={"file": ("pipe.json", recipe, "application/json")},
                    data={"collection": "uploads", "target_datasets": ""})
    assert r.status_code == 200, r.text
    assert r.json()["kind"] == "pipeline"


def test_stats_graph_composition_endpoints(client: TestClient):
    params = {"metric": "rmse", "scope": "cv"}
    unfiltered = client.get("/api/stats", params=params).json()
    assert unfiltered["summary"] == {"n": 0}
    assert unfiltered["values"] == [] and unfiltered["correlations"] == []
    assert unfiltered["score_context"]["status"] == "withheld"
    assert unfiltered["score_context"]["withheld"]["count"] > 0
    dataset = client.get("/api/datasets").json()[0]["dataset_fingerprint"]
    st = client.get("/api/stats", params={**params, "dataset": dataset}).json()
    assert st["summary"]["n"] > 0 and "values" in st
    assert st["score_context"]["status"] == "single"
    g = client.get("/api/graph", params={"kind": "pipelines", "metric": "rmse", "dataset": dataset}).json()
    assert "nodes" in g
    c = client.get("/api/composition", params={"metric": "rmse", "dataset": dataset}).json()
    assert "rows" in c


def test_upload_text_arena_export(client: TestClient):
    from conftest import rankable_fixture_exports as generate_fixture_exports

    manifest = json.dumps(generate_fixture_exports()[0].to_manifest())
    r = client.post("/api/upload", data={"text": manifest, "collection": "fixtures"})
    assert r.status_code == 200
    assert r.json()["kind"] == "arena_export"


def test_runs_accepts_the_limit_the_run_explorer_requests(client: TestClient):
    r = client.get("/api/runs", params={"metric": "rmse", "scope": "cv", "limit": 100000})
    assert r.status_code == 200
    assert r.json()


def test_upload_text_never_reads_a_file_on_the_server(client: TestClient, tmp_path: Path):
    secret = tmp_path / "secret.txt"
    secret.write_text("first line\nTOP-SECRET-SECOND-LINE: [unterminated\n")
    for text in (str(secret), json.dumps(str(secret))):
        r = client.post("/api/upload", data={"text": text, "collection": "uploads"})
        assert r.status_code == 400
        assert "TOP-SECRET" not in r.text


def test_uploaded_file_naming_a_server_path_is_not_followed(client: TestClient, tmp_path: Path):
    secret = tmp_path / "secret.txt"
    secret.write_text("first line\nTOP-SECRET-SECOND-LINE: [unterminated\n")
    r = client.post("/api/upload", files={"file": ("pointer.txt", str(secret), "text/plain")},
                    data={"collection": "uploads", "target_datasets": ""})
    assert r.status_code == 400
    assert "TOP-SECRET" not in r.text


@pytest.mark.parametrize("route", ["ingest", "upload-export", "upload-pipeline"])
def test_f2_held_writer_returns_503_code_and_cleans_upload(tmp_path, monkeypatch, route):
    import tempfile
    from functools import partial

    from test_arena_v2_work_review import residual_record

    service_app = importlib.import_module("nirs4all_benchmarks.service.app")
    monkeypatch.setattr(service_app, "ArenaStore", partial(ArenaStore, busy_timeout_ms=30))
    root = tmp_path / "store"
    source = residual_record()
    payload = {"nodes": source["pipeline"]["nodes"]} if route == "upload-pipeline" else source
    temporary = []
    original = tempfile.mkstemp

    def tracked(*args, **kwargs):
        fd, path = original(*args, **kwargs)
        temporary.append(Path(path))
        return fd, path

    monkeypatch.setattr(tempfile, "mkstemp", tracked)
    with ArenaStore(root, busy_timeout_ms=30) as store, TestClient(create_app(root), raise_server_exceptions=False) as client:
        writer = sqlite3.connect(store.db_path)
        try:
            writer.execute("BEGIN IMMEDIATE")
            writer.execute("UPDATE arena_meta SET value=value")
            if route == "ingest":
                response = client.post("/api/ingest", json=source)
            else:
                response = client.post("/api/upload", files={"file": ("synthetic.json", json.dumps(payload), "application/json")})
            assert response.status_code == 503, response.text
            assert response.json() == {"detail": "arena_store_busy"}
            assert not any(p.exists() for p in temporary)
            if route != "ingest":
                assert temporary
            for table in ("executions", "ingestion_batches", "metadata_receipts", "pipeline_dags", "collections"):
                assert store.count(table) == 0
            assert list(store.arrays_dir.iterdir()) == list(store.exports_dir.iterdir()) == []
        finally:
            writer.rollback()
            writer.close()
        if route == "ingest":
            response = client.post("/api/ingest", json=source)
        else:
            response = client.post("/api/upload", files={"file": ("synthetic.json", json.dumps(payload), "application/json")})
        assert response.status_code == 200, response.text
        assert not any(p.exists() for p in temporary)
        assert store.count("pipeline_dags") == 1
