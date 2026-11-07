"""Per-export context and immutable task-unit regressions."""

import hashlib
import json

import pytest

from nirs4all_benchmarks.contract import ArenaRunExport
from nirs4all_benchmarks.identity import canonical_json
from nirs4all_benchmarks.ingestion import ingest_export
from nirs4all_benchmarks.service.project import _metadata
from nirs4all_benchmarks.store import ArenaStore, Queries
from test_offline_arena import manifest


def test_per_export_context_and_unit_conflict(tmp_path):
    a = manifest(execution_id="a")
    a["pipeline"]["human_label"] = "private-a"
    a["pipeline"]["engine_graph_fingerprint"] = "a" * 64
    a["dataset_variant"]["sample_manifest_hash"] = "c" * 64
    a["task"]["target_unit"] = "mg"
    b = manifest(execution_id="b")
    b["pipeline"]["human_label"] = "private-b"
    b["pipeline"]["engine_graph_fingerprint"] = "b" * 64
    b["dataset_variant"]["sample_manifest_hash"] = "d" * 64
    b["task"]["target_unit"] = "mg"
    with ArenaStore(tmp_path / "store") as store:
        first = ingest_export(store, a)
        second = ingest_export(store, b)
        assert first.ok and second.ok, (first.issues, second.issues)
        for outcome, label, graph, sample in ((first, "private-a", "a" * 64, "c" * 64),
                                               (second, "private-b", "b" * 64, "d" * 64)):
            detail = Queries(store).run_api_detail(outcome.execution_hash, project=True)
            sub = detail["submission"]
            assert sub["pipeline_human_label"] == {"state": "reported", "value": label}
            assert sub["target_unit"] == {"state": "reported", "value": "mg"}
            assert sub["engine_graph_fingerprint"]["value"] == graph
            assert sub["sample_manifest_hash"]["value"] == sample
            assert detail["pipeline"]["association_scope"] == "pipeline_first_accepted"
            assert _metadata(store.root, outcome.execution_hash)["submitted_metadata"]["context"]["pipeline"]["human_label"] == label
            receipt = store.get("metadata_receipts", "arena_export_hash", outcome.arena_export_hash)["receipt_json"]
            assert label not in receipt and '"mg"' not in receipt
        bad = manifest(execution_id="bad")
        bad["task"]["target_unit"] = "kg"
        before = store.count("executions")
        result = ingest_export(store, bad)
        assert result.issues[0]["code"] == "identity_dimension_conflict"
        assert store.count("executions") == before
        assert ingest_export(store, a).status == "already_ingested"


@pytest.mark.parametrize("first,second", [(None, "mg"), ("mg", None), ("a", "b")])
def test_unit_null_and_known_conflict(tmp_path, first, second):
    a, b = manifest(execution_id="a"), manifest(execution_id="b")
    a["task"]["target_unit"], b["task"]["target_unit"] = first, second
    with ArenaStore(tmp_path / "store") as store:
        assert ingest_export(store, a).ok
        assert ingest_export(store, b).issues[0]["code"] == "identity_dimension_conflict"
        assert store.count("executions") == 1


def test_replay_and_binding_fail_closed(tmp_path):
    source = manifest()
    source["pipeline"].pop("human_label", None)
    with ArenaStore(tmp_path / "store") as store:
        result = ingest_export(store, source)
        assert result.ok
        q = Queries(store)
        assert q.submission_context(result.execution_hash)["pipeline_human_label"]["state"] == "absent"
        replay = manifest()
        replay["pipeline"]["human_label"] = None
        assert ingest_export(store, replay).status == "already_ingested"
        row = store.get("submitted_metadata", "arena_export_hash", result.arena_export_hash)
        data = json.loads(row["source_metadata_json"])
        data["arena_context"]["arena_export_hash"] = "f" * 64
        text = canonical_json(data)
        store.conn.execute("UPDATE submitted_metadata SET source_metadata_json=?, source_metadata_sha256=? WHERE arena_export_hash=?",
                           (text, hashlib.sha256(text.encode()).hexdigest(), result.arena_export_hash))
        store.conn.commit()
        assert q.submission_context(result.execution_hash)["context_status"] == "binding_inconsistent"
        assert ingest_export(store, source).issues[0]["code"] == "stored_batch_inconsistent"
        with pytest.raises(RuntimeError):
            _metadata(store.root, result.execution_hash)


def test_normalized_null_and_old_row(tmp_path):
    source = manifest()
    source["pipeline"]["human_label"] = None
    with ArenaStore(tmp_path / "store") as store:
        result = ingest_export(store, ArenaRunExport.model_validate(source))
        assert result.ok
        assert Queries(store).submission_context(result.execution_hash)["pipeline_human_label"]["state"] == "normalized_null"
        row = store.get("submitted_metadata", "arena_export_hash", result.arena_export_hash)
        data = json.loads(row["source_metadata_json"])
        del data["arena_context"]
        text = canonical_json(data)
        store.conn.execute("UPDATE submitted_metadata SET source_metadata_json=?, source_metadata_sha256=? WHERE arena_export_hash=?",
                           (text, hashlib.sha256(text.encode()).hexdigest(), result.arena_export_hash))
        store.conn.commit()
        assert Queries(store).submission_context(result.execution_hash)["context_status"] == "unavailable"
        assert ingest_export(store, source).status == "already_ingested"
        assert "arena_context" not in json.loads(store.get("submitted_metadata", "arena_export_hash", result.arena_export_hash)["source_metadata_json"])


@pytest.fixture
def card_only_store(tmp_path):
    root = tmp_path / "store"
    a, b = manifest(execution_id="a"), manifest(execution_id="b")
    a["pipeline"]["human_label"], b["pipeline"]["human_label"] = "SHARED_FIRST_LABEL", "CARD_ONLY_LABEL"
    a["task"]["target_unit"] = b["task"]["target_unit"] = "CARD_ONLY_UNIT"
    with ArenaStore(root) as store:
        first, second = ingest_export(store, a), ingest_export(store, b)
        assert first.ok and second.ok
    return root, second.execution_hash


def test_run_detail_never_reads_private_card(card_only_store):
    root, execution = card_only_store
    with ArenaStore(root) as store:
        detail = Queries(store).run_detail(execution)
        receipt = store.get("metadata_receipts", "arena_export_hash", detail["execution"]["arena_export_hash"])
        assert all(value not in receipt["receipt_json"] for value in ("CARD_ONLY_LABEL", "CARD_ONLY_UNIT", "arena_context"))
        batch = store.get("ingestion_batches", "ingestion_batch_id", detail["execution"]["ingestion_batch_id"])
        assert all(value not in batch["clean_report_json"] for value in ("CARD_ONLY_LABEL", "CARD_ONLY_UNIT", "arena_context"))
    assert detail["pipeline"]["human_label"] == "SHARED_FIRST_LABEL"
    assert all(value not in json.dumps(detail) for value in ("CARD_ONLY_LABEL", "CARD_ONLY_UNIT", "arena_context"))


def test_offline_owner_profile_is_scoped_without_card(card_only_store, caplog):
    from fastapi.testclient import TestClient

    from nirs4all_benchmarks.service.app import create_app

    root, execution = card_only_store
    client = TestClient(create_app(root))
    detail = client.get(f"/api/run/{execution}").json()
    assert detail["pipeline"]["human_label"] == "SHARED_FIRST_LABEL"
    assert detail["pipeline"]["association_scope"] == "pipeline_first_accepted"
    assert detail["submission"]["context_status"] == "not_exposed"
    assert "pipeline_human_label" not in detail["submission"] and "target_unit" not in detail["submission"]
    assert all(value not in json.dumps(detail) for value in ("CARD_ONLY_LABEL", "CARD_ONLY_UNIT", "arena_context"))
    assert client.get(f"/api/run/{execution}/metadata").status_code == 404
    assert "CARD_ONLY_LABEL" not in caplog.text and "CARD_ONLY_UNIT" not in caplog.text


def test_offline_routes_expose_no_card_values(card_only_store):
    from fastapi.testclient import TestClient

    from nirs4all_benchmarks.service.app import create_app

    root, execution = card_only_store
    client = TestClient(create_app(root))
    paths = ["/api/pipelines", "/api/runs", "/api/leaderboard", "/api/matrix",
             "/api/parallel?dimensions=seed", "/api/parameter-effect?param=x", "/api/robustness",
             "/api/planned", "/api/unknown-context", "/api/graph", f"/api/run/{execution}"]
    for path in paths:
        response = client.get(path)
        assert response.status_code == 200, (path, response.text)
        body = response.json()
        assert all(value not in response.text for value in ("CARD_ONLY_LABEL", "CARD_ONLY_UNIT", "arena_context"))
        if path.startswith("/api/run/"):
            assert body["pipeline"]["association_scope"] == "pipeline_first_accepted"
        elif isinstance(body, list):
            assert all(row["pipeline_label_scope"] == "pipeline_first_accepted" for row in body)
        else:
            assert body["pipeline_label_scope"] == "pipeline_first_accepted"


@pytest.mark.parametrize("reverse", [False, True])
def test_atomic_pack_unit_conflict_rolls_back(tmp_path, reverse):
    from nirs4all_benchmarks.ingestion.ingest import AtomicIngestionError, ingest_exports_atomic
    a, b = manifest(execution_id="a"), manifest(execution_id="b")
    a["task"]["target_unit"], b["task"]["target_unit"] = None, "mg"
    with ArenaStore(tmp_path / "store") as store:
        with pytest.raises(AtomicIngestionError, match="identity_dimension_conflict"):
            ingest_exports_atomic(store, [b, a] if reverse else [a, b])
        assert store.count("executions") == 0
        assert store.count("submitted_metadata") == 0


def test_coherent_rewrite_readable_but_replay_refused(tmp_path):
    source = manifest()
    source["pipeline"]["human_label"] = "original"
    with ArenaStore(tmp_path / "store") as store:
        result = ingest_export(store, source)
        row = store.get("submitted_metadata", "arena_export_hash", result.arena_export_hash)
        data = json.loads(row["source_metadata_json"])
        data["arena_context"]["pipeline"]["human_label"] = "rewritten"
        text = canonical_json(data)
        store.conn.execute("UPDATE submitted_metadata SET source_metadata_json=?, source_metadata_sha256=? WHERE arena_export_hash=?",
                           (text, hashlib.sha256(text.encode()).hexdigest(), result.arena_export_hash))
        store.conn.commit()
        assert Queries(store).submission_context(result.execution_hash)["pipeline_human_label"]["value"] == "rewritten"
        assert ingest_export(store, source).issues[0]["code"] == "stored_batch_inconsistent"


def test_project_isolation_and_per_execution_lists(tmp_path):
    from fastapi.testclient import TestClient
    from nirs4all_access import ProjectPrincipal, issue_credential

    from nirs4all_benchmarks.service.project import create_project_app, provision_project

    tokens, credentials = [], []
    for name in ("a", "b"):
        provision_project(tmp_path, name)
        token, credential = issue_credential(ProjectPrincipal(name, "test", frozenset({"arena"}),
                                                               frozenset({"arena.read", "arena.add"})))
        tokens.append(token)
        credentials.append(credential)
    client = TestClient(create_project_app(tmp_path, credentials))
    headers = [{"Authorization": "Bearer " + token} for token in tokens]
    a, b = manifest(execution_id="a"), manifest(execution_id="b")
    a["pipeline"]["human_label"], b["pipeline"]["human_label"] = "SHARED_FIRST_LABEL", "CARD_ONLY_LABEL"
    a["task"]["target_unit"] = b["task"]["target_unit"] = "CARD_ONLY_UNIT"
    r1 = client.post("/api/ingest", headers=headers[0], json=a)
    r2 = client.post("/api/ingest", headers=headers[0], json=b)
    assert r1.status_code == r2.status_code == 200
    e1, e2 = r1.json()["execution_hash"], r2.json()["execution_hash"]
    rows = client.get("/api/runs", headers=headers[0]).json()
    assert {r["execution_hash"]: r["pipeline_human_label"]["value"] for r in rows} == {
        e1: "SHARED_FIRST_LABEL", e2: "CARD_ONLY_LABEL"}
    parallel = client.get("/api/parallel?dimensions=seed", headers=headers[0]).json()["rows"]
    assert {r["execution_hash"]: r["pipeline_human_label"]["value"] for r in parallel} == {
        e1: "SHARED_FIRST_LABEL", e2: "CARD_ONLY_LABEL"}
    detail = client.get(f"/api/run/{e2}", headers=headers[0]).json()
    assert detail["submission"]["pipeline_human_label"] == {"state": "reported", "value": "CARD_ONLY_LABEL"}
    assert detail["submission"]["target_unit"] == {"state": "reported", "value": "CARD_ONLY_UNIT"}
    assert detail["pipeline"]["association_scope"] == "pipeline_first_accepted"
    submitted = client.get(f"/api/run/{e2}/metadata", headers=headers[0]).json()["submitted_metadata"]
    assert submitted["context"]["pipeline"]["human_label"] == "CARD_ONLY_LABEL"
    assert "arena_context" not in submitted["metadata"]
    assert client.get(f"/api/run/{e1}", headers=headers[1]).status_code == 404
    b["task"]["target_unit"] = "kg"
    other = client.post("/api/ingest", headers=headers[1], json=b)
    assert other.status_code == 200
    own = client.get(f"/api/run/{other.json()['execution_hash']}", headers=headers[1]).json()
    assert own["submission"]["target_unit"]["value"] == "kg"
    assert client.post("/api/ingest", headers=headers[0], json=b).status_code == 409
    from test_arena_v2 import record

    unknowns = {}
    for name in ("UNKNOWN_FIRST", "UNKNOWN_SECOND"):
        source = record(execution_id=name, known=False)
        source["dataset"].update(dataset_fingerprint="d" * 64, visibility="unreported")
        source["task"]["task_hash"] = "e" * 64
        source["dataset_variant"]["dataset_variant_hash"] = "f" * 64
        source["pipeline"]["human_label"] = name
        response = client.post("/api/ingest", headers=headers[0], json=source)
        assert response.status_code == 200, response.text
        unknowns[response.json()["execution_hash"]] = name
    unknown_rows = client.get("/api/unknown-context", headers=headers[0]).json()["rows"]
    assert {r["execution_hash"]: r["pipeline_human_label"]["value"] for r in unknown_rows} == unknowns


@pytest.mark.parametrize("kind", ["missing", "extra", "wrong_type", "wrong_export"])
def test_invalid_context_shape_fails_closed(tmp_path, kind):
    source = manifest()
    with ArenaStore(tmp_path / "store") as store:
        outcome = ingest_export(store, source)
        row = store.get("submitted_metadata", "arena_export_hash", outcome.arena_export_hash)
        data = json.loads(row["source_metadata_json"])
        context = data["arena_context"]
        if kind == "missing":
            del context["task"]
        elif kind == "extra":
            context["extra"] = 1
        elif kind == "wrong_type":
            context["pipeline"]["human_label"] = 123
        else:
            context["arena_export_hash"] = "f" * 64
        text = canonical_json(data)
        store.conn.execute("UPDATE submitted_metadata SET source_metadata_json=?, source_metadata_sha256=? WHERE arena_export_hash=?",
                           (text, hashlib.sha256(text.encode()).hexdigest(), outcome.arena_export_hash))
        store.conn.commit()
        assert Queries(store).submission_context(outcome.execution_hash)["context_status"] == "binding_inconsistent"
        assert ingest_export(store, source).issues[0]["code"] == "stored_batch_inconsistent"
        with pytest.raises(RuntimeError):
            _metadata(store.root, outcome.execution_hash)


def test_submitted_null_state(tmp_path):
    source = manifest()
    source["pipeline"]["human_label"] = None
    with ArenaStore(tmp_path / "store") as store:
        outcome = ingest_export(store, source)
        assert Queries(store).submission_context(outcome.execution_hash)["pipeline_human_label"]["state"] == "declared_null"


@pytest.mark.parametrize("public_label", ["PUBLIC_LABEL", None])
def test_public_projection_ignores_shared_private_rows(tmp_path, public_label):
    from nirs4all_benchmarks.site_build import _build_bundle, _public_run_document, build_site
    from test_arena_work_review import run

    private, public = run("PRIVATE", "private"), run()
    private["pipeline"]["nodes"] = public["pipeline"]["nodes"]
    private["pipeline"].update(human_label="PRIVATE_LABEL", engine_graph_fingerprint="f" * 64)
    private["task"]["target_unit"] = "PRIVATE_UNIT"
    private["dataset"]["dataset_card"] = {"name": "PRIVATE_CARD"}
    public["pipeline"]["human_label"] = public_label
    with ArenaStore(tmp_path / "store") as store:
        assert ingest_export(store, private).ok
        result = ingest_export(store, public)
        assert result.ok, result.issues
        q = Queries(store)
        bundle = _build_bundle(store, q)
        document = _public_run_document(store, q, result.execution_hash, bundle)
        blob = json.dumps([bundle, document])
        assert all(x not in blob for x in ("PRIVATE_LABEL", "PRIVATE_UNIT", "PRIVATE_CARD", '"' + "f" * 64 + '"',
                                             "arena_context", "submission", "association_scope"))
        labels = bundle["tables"]["pipelines"]["columns"]["human_label"]["s"]
        assert [bundle["strings"][i] if i is not None else None for i in labels] == [public_label]
        assert document["detail"]["pipeline"]["human_label"] == public_label
        if public_label:
            assert public_label in blob
        else:
            assert all(x is None for x in bundle["tables"]["metrics"]["columns"]["pipeline_label"]["s"])
        later = run("UNKNOWN", "private")
        later["pipeline"]["nodes"] = public["pipeline"]["nodes"]
        later["pipeline"]["human_label"] = "LATER_PRIVATE_LABEL"
        assert ingest_export(store, later).ok
        assert _build_bundle(store, q) == bundle
    with pytest.raises(ValueError, match="publication_gate_closed"):
        build_site(tmp_path / "store", tmp_path / "site")
    assert not (tmp_path / "site").exists()


def test_private_first_same_fingerprint_cannot_publish(tmp_path):
    from nirs4all_benchmarks.site_build import _build_bundle
    from test_arena_work_review import run

    private, public = run("PRIVATE", "private"), run()
    private["dataset"]["dataset_fingerprint"] = public["dataset"]["dataset_fingerprint"]
    private["pipeline"]["nodes"] = public["pipeline"]["nodes"]
    with ArenaStore(tmp_path / "store") as store:
        assert ingest_export(store, private).ok
        result = ingest_export(store, public)
        assert result.ok, result.issues
        with pytest.raises(ValueError, match="zero rankable explicitly public"):
            _build_bundle(store, Queries(store))


def test_project_readback_states_without_shared_fallback(tmp_path):
    from fastapi.testclient import TestClient
    from nirs4all_access import ProjectPrincipal, issue_credential

    from nirs4all_benchmarks.service.project import create_project_app, provision_project

    provision_project(tmp_path, "a")
    token, credential = issue_credential(ProjectPrincipal("a", "test", frozenset({"arena"}),
                                                          frozenset({"arena.read", "arena.add"})))
    headers = {"Authorization": "Bearer " + token}
    root = tmp_path / "projects" / "a"
    values = [("shared", "FIRST_LABEL"), ("absent", ...), ("declared_null", None),
              ("reported", "ACTUAL_LABEL"), ("normalized_null", None), ("old", None)]
    executions = {}
    with ArenaStore(root) as store:
        for state, label in values:
            source = manifest(execution_id=state)
            if label is ...:
                source["pipeline"].pop("human_label", None)
            else:
                source["pipeline"]["human_label"] = label
            incoming = ArenaRunExport.model_validate(source) if state == "normalized_null" else source
            result = ingest_export(store, incoming)
            assert result.ok, result.issues
            executions[state] = result.execution_hash
            if state == "old":
                row = store.get("submitted_metadata", "arena_export_hash", result.arena_export_hash)
                data = json.loads(row["source_metadata_json"])
                del data["arena_context"]
                text = canonical_json(data)
                store.conn.execute("UPDATE submitted_metadata SET source_metadata_json=?, source_metadata_sha256=? "
                                   "WHERE arena_export_hash=?", (text, hashlib.sha256(text.encode()).hexdigest(),
                                                                result.arena_export_hash))
                store.conn.commit()
    client = TestClient(create_project_app(tmp_path, [credential]))
    for state in ("absent", "declared_null", "reported", "normalized_null", "old"):
        body = client.get(f"/api/run/{executions[state]}", headers=headers).json()
        label = body["submission"]["pipeline_human_label"]
        assert label["state"] == ("unavailable" if state == "old" else state)
        assert label["value"] == ("ACTUAL_LABEL" if state == "reported" else None)
        assert body["submission"]["sample_manifest_hash"] == {"state": "not_reported", "value": None}
        assert body["submission"]["engine_graph_fingerprint"] == {"state": "not_reported", "value": None}
        assert body["pipeline"]["first_accepted_human_label"] == "FIRST_LABEL"
        if state == "old":
            assert body["submission"]["context_status"] == "unavailable"
