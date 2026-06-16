"""FastAPI service smoke tests via TestClient."""

from __future__ import annotations

import json
from pathlib import Path

import pytest

pytest.importorskip("fastapi")
from fastapi.testclient import TestClient

from nirs4all_benchmarks.fixtures import generate_fixture_exports
from nirs4all_benchmarks.service import create_app


@pytest.fixture
def client(seeded_root: Path) -> TestClient:
    app = create_app(seeded_root)
    return TestClient(app)


def test_healthz(client: TestClient):
    r = client.get("/api/healthz")
    assert r.status_code == 200
    assert r.json()["status"] == "ok"


def test_overview(client: TestClient):
    r = client.get("/api/overview")
    assert r.status_code == 200
    assert r.json()["pipelines"] == 7


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
    pv = client.get("/api/pivot", params={"group_by": "param:n_components", "metric": "rmse", "scope": "cv"}).json()
    assert pv["rows"]


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
    st = client.get("/api/stats", params={"metric": "rmse", "scope": "cv"}).json()
    assert st["summary"]["n"] > 0 and "values" in st
    g = client.get("/api/graph", params={"kind": "pipelines", "metric": "rmse"}).json()
    assert "nodes" in g
    c = client.get("/api/composition", params={"metric": "rmse"}).json()
    assert "rows" in c


def test_upload_text_arena_export(client: TestClient):
    from nirs4all_benchmarks.fixtures import generate_fixture_exports

    manifest = json.dumps(generate_fixture_exports()[0].to_manifest())
    r = client.post("/api/upload", data={"text": manifest, "collection": "fixtures"})
    assert r.status_code == 200
    assert r.json()["kind"] == "arena_export"
