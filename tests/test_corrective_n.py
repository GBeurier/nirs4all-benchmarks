"""Focused synthetic owner regressions for Arena N1–N6."""

import copy
import json

import pytest

from nirs4all_benchmarks.identity import fingerprint
from nirs4all_benchmarks.ingestion import ingest_export, validate_metadata
from nirs4all_benchmarks.service.project import _ingest, _metadata
from nirs4all_benchmarks.store import Queries
from test_offline_arena import manifest


def test_numeric_domains_and_exact_seed(empty_store):
    store = empty_store
    for seed in (0, 2**63 - 1, 2**63, 2**64 - 1):
        source = manifest(execution_id=f"seed-{seed}")
        source["rng"] = {"root_seed": seed}
        outcome = ingest_export(store, source)
        assert outcome.ok, outcome.issues
        detail = Queries(store).run_detail(outcome.execution_hash)
        assert detail["rng"]["root_seed"] == str(seed)
        facet = store.query_one("SELECT facet_value, facet_num FROM run_facets WHERE run_condition_hash=? AND facet_key='seed'", (outcome.run_condition_hash,))
        assert facet["facet_value"] == str(seed)
        assert facet["facet_num"] == (float(seed) if int(float(seed)) == seed else None)
        assert ingest_export(store, source).status == "already_ingested"
    for field, values in (("root_seed", (-1, 2**64, 2**70, True, 1.0, "1")),
                          ("n_samples", (-1, 2**63, 2**64 - 1, 2**64, 2**70, True, 1.0, "1"))):
        for value in values:
            source = manifest(execution_id=f"bad-{field}-{value!r}")
            source.setdefault("rng" if field == "root_seed" else "dataset", {})[field] = value
            before = store.count("executions")
            validation = validate_metadata(source)
            assert validation.status == "rejected", (field, value)
            assert validation.issues[0]["code"] == "schema_invalid"
            assert ingest_export(store, source).status == "rejected"
            status, response = _ingest(store.root, json.dumps(source).encode(), "uploads")
            assert status == 422, (field, value, status, response)
            assert store.count("executions") == before


def test_card_graph_and_large_scalar_refusals(empty_store):
    cases = [
        {"dataset": {"dataset_card": {"axis": {"n": 2**63}}}},
        {"dataset": {"dataset_card": {"axis": {"range": [10**400]}}}},
        {"dataset": {"dataset_card": {"task_type": 10**400}}},
        {"pipeline": {"graph": {"nodes": [{"id": "a", "operator": 10**400}]}}},
        {"execution": {"time_ms": 10**400}},
        {"scores": {"observations": [{"metric_name": "rmse", "metric_value": 1.0,
          "direction": "min", "direction_source": "registry", "scope": "cv", "partition": "validation",
          "aggregation_level": "sample", "fold_id": "0", "source_fold_id": None, "coverage": 10**400}]}},
    ]
    for overrides in cases:
        source = manifest()
        for block, changes in overrides.items():
            source[block].update(changes)
        status, response = _ingest(empty_store.root, json.dumps(source).encode(), "uploads")
        assert status < 500, (overrides, response)
        assert ingest_export(empty_store, source).status in {"rejected", "committed"}


def test_binding_refuses_partial_rewrite_and_projects_receipt(empty_store):
    source = manifest(terminal="failed")
    outcome = ingest_export(empty_store, source)
    assert outcome.ok
    receipt = empty_store.get("metadata_receipts", "arena_export_hash", outcome.arena_export_hash)
    original = receipt["receipt_json"]
    data = json.loads(original)
    data["extra_secret"] = "not returned"
    empty_store.conn.execute("UPDATE metadata_receipts SET receipt_json=?, receipt_sha256=? WHERE arena_export_hash=?",
                             (json.dumps(data), fingerprint(data), outcome.arena_export_hash))
    empty_store.conn.commit()
    assert "extra_secret" not in _metadata(empty_store.root, outcome.execution_hash)["receipt"]
    data["metadata_validation"]["terminal_status"] = "completed"
    empty_store.conn.execute("UPDATE metadata_receipts SET receipt_json=?, receipt_sha256=? WHERE arena_export_hash=?",
                             (json.dumps(data), fingerprint(data), outcome.arena_export_hash))
    empty_store.conn.commit()
    assert ingest_export(empty_store, source).issues[0]["code"] == "stored_batch_inconsistent"
    assert Queries(empty_store).run_detail(outcome.execution_hash)["metadata"]["status"] == "metadata_unavailable"
    try:
        _metadata(empty_store.root, outcome.execution_hash)
    except RuntimeError:
        pass
    else:
        raise AssertionError("inconsistent receipt was served")


def test_exact_submission_card_scope(empty_store):
    first = manifest(execution_id="one")
    first["dataset"]["dataset_card"] = {"name": "first"}
    second = copy.deepcopy(first)
    second["execution"]["execution_id"] = "two"
    second["dataset"]["dataset_card"] = {"name": "second"}
    third = copy.deepcopy(first)
    third["execution"]["execution_id"] = "three"
    third["dataset"]["dataset_card"] = {}
    results = [ingest_export(empty_store, x) for x in (first, second, third)]
    assert all(x.ok for x in results)
    q = Queries(empty_store)
    detail = q.run_api_detail(results[1].execution_hash)
    assert detail["dataset"]["association_scope"] == "dataset_fingerprint_first_accepted"
    assert detail["submission"]["dataset_card_hash"] == fingerprint(second["dataset"]["dataset_card"])
    assert q.run_api_detail(results[2].execution_hash)["submission"]["dataset_card_hash"] is None
    assert all(x["card_scope"] == "first_accepted_fingerprint" for x in q.datasets_api())


def test_all_count_domains_and_scalar_sweep(empty_store):
    paths = (("dataset", "n_samples"), ("dataset", "n_features"),
             ("scores", "expected_fold_count"), ("scores", "completed_fold_count"),
             ("cv", "n_folds"), ("cv", "n_repeats"))
    for block, name in paths:
        for value in (2**63, 2**64 - 1, 2**64, 2**70, True, 1.0, "1"):
            source = manifest()
            source.setdefault(block, {})[name] = value
            before = empty_store.count("executions")
            assert validate_metadata(source).status == "rejected", (block, name, value)
            assert ingest_export(empty_store, source).status == "rejected"
            status, _ = _ingest(empty_store.root, json.dumps(source).encode(), "uploads")
            assert status == 422, (block, name, value, status)
            assert empty_store.count("executions") == before
    source = manifest()
    source["dataset"]["dataset_card"] = {"name": "card", "axis": {"n": 4, "unit": "nm", "range": [1, 2]}}
    source["rng"] = {"root_seed": 1}
    source["execution"]["time_ms"] = 2.0

    def leaves(value, path=()):
        if isinstance(value, dict):
            for key, item in value.items():
                yield from leaves(item, (*path, key))
        elif isinstance(value, list):
            for index, item in enumerate(value):
                yield from leaves(item, (*path, index))
        else:
            yield path

    for path in leaves(source):
        for value in (2**63, 2**64, 2**70, 10**400):
            variant = copy.deepcopy(source)
            target = variant
            for key in path[:-1]:
                target = target[key]
            target[path[-1]] = value
            status, response = _ingest(empty_store.root, json.dumps(variant).encode(), "uploads")
            assert status < 500, (path, value, response)


def test_replay_before_owner_collision(empty_store, monkeypatch):
    import nirs4all_benchmarks.ingestion.offline as offline

    first = manifest(execution_id="owner-a")
    first["execution"]["owner_pipeline_name"] = "a"
    other = manifest(execution_id="owner-b")
    other["execution"]["owner_pipeline_name"] = "b"
    other["rng"] = {"root_seed": 33}
    original = ingest_export(empty_store, first)
    collision = ingest_export(empty_store, other)
    assert original.ok and collision.ok
    resolve = offline.resolve_identities

    def drift(model):
        result = resolve(model)
        result.run_condition_hash = collision.run_condition_hash
        return result

    monkeypatch.setattr(offline, "resolve_identities", drift)
    assert ingest_export(empty_store, first).status == "already_ingested"
    fresh = copy.deepcopy(first)
    fresh["execution"]["execution_id"] = "new-export"
    result = ingest_export(empty_store, fresh)
    assert result.issues[0]["code"] == "workspace_identity_collision"


def test_count_limits_and_identity_literal(empty_store):
    source = manifest(execution_id="seed-0")
    source["rng"] = {"root_seed": 0}
    validation = validate_metadata(source)
    assert validation.arena_export_hash == "8463737a4c0daeb97312f7458f867959c8a3344808b5cf95f84c4012b5f5e4a9"
    assert validation.resolved.rng_context_hash == "70be26895b717e8ac0cb5dc61aec948b97371ff083cff9a0ceca21b131f3cd1a"
    assert validation.resolved.run_condition_hash == "d3ce23b5d506ee487f954fc481708199d462de16833aec68a3f765b1bac7c82e"
    for block, key in (("dataset", "n_samples"), ("dataset", "n_features"),
                       ("scores", "completed_fold_count"), ("scores", "expected_fold_count"),
                       ("cv", "n_folds"), ("cv", "n_repeats")):
        for value in (1, 2**63 - 1):
            candidate = manifest(terminal="failed", execution_id=f"{block}-{key}-{value}")
            candidate["scores"]["observations"] = []
            candidate.setdefault(block, {})[key] = value
            valid = validate_metadata(candidate)
            assert valid.status == "metadata_valid", (block, key, value, valid.issues)
    for value in (0, 2**63 - 1):
        candidate = manifest(terminal="failed")
        candidate["scores"]["observations"][0]["n_samples"] = value
        assert validate_metadata(candidate).status == "metadata_valid"
    for value in (-1, 2**63, 2**64 - 1, 2**70, True, 1.0, "1"):
        candidate = manifest(terminal="failed")
        candidate["scores"]["observations"][0]["n_samples"] = value
        assert validate_metadata(candidate).status == "rejected"


def test_repoint_and_row_copy_refused(empty_store):
    one = manifest(terminal="failed", execution_id="one")
    one["dataset"]["dataset_card"] = {"name": "one"}
    two = manifest(terminal="failed", execution_id="two")
    two["dataset"]["dataset_card"] = {"name": "two"}
    two["provenance"] = {"source": "different"}
    a, b = (ingest_export(empty_store, item) for item in (one, two))
    assert a.ok and b.ok
    conn = empty_store.conn
    conn.execute("PRAGMA foreign_keys=OFF")
    conn.execute("UPDATE executions SET arena_export_hash=? WHERE execution_hash=?", (b.arena_export_hash, a.execution_hash))
    conn.commit()
    assert ingest_export(empty_store, one).issues[0]["code"] == "stored_batch_inconsistent"
    assert Queries(empty_store).run_detail(a.execution_hash)["metadata"]["status"] == "metadata_unavailable"
    conn.execute("UPDATE executions SET arena_export_hash=? WHERE execution_hash=?", (a.arena_export_hash, a.execution_hash))
    row = empty_store.get("submitted_metadata", "arena_export_hash", b.arena_export_hash)
    columns = ("source_metadata_json", "source_metadata_sha256", "normalized_provenance_sha256",
               "normalized_dataset_card_sha256", "dataset_card_hash")
    conn.execute("UPDATE submitted_metadata SET " + ", ".join(f"{key}=?" for key in columns)
                 + " WHERE arena_export_hash=?", (*(row[key] for key in columns), a.arena_export_hash))
    conn.commit()
    assert ingest_export(empty_store, one).issues[0]["code"] == "stored_batch_inconsistent"
    assert Queries(empty_store).run_detail(a.execution_hash)["metadata"]["status"] == "metadata_unavailable"
    try:
        _metadata(empty_store.root, a.execution_hash)
    except RuntimeError:
        pass
    else:
        raise AssertionError("copied row was served")


def test_previous_schema_fingerprint_refused_without_changes(tmp_path):
    import hashlib
    import sqlite3

    import pytest

    from nirs4all_benchmarks.store import ArenaStore, ArenaStoreRefusal
    from nirs4all_benchmarks.store.arena_store import _SCHEMA_SQL, ARENA_APPLICATION_ID

    old_schema = _SCHEMA_SQL.replace(
        "    split_spec_hash      TEXT REFERENCES split_specs (split_spec_hash),\n",
        "    split_spec_hash      TEXT REFERENCES split_specs (split_spec_hash),\n    rng_context_hash     TEXT,\n",
    ).replace(
        "    cv_spec_hash              TEXT REFERENCES cv_specs (cv_spec_hash),\n",
        "    cv_spec_hash              TEXT REFERENCES cv_specs (cv_spec_hash),\n    rng_context_hash          TEXT,\n",
    ).replace("root_seed             TEXT", "root_seed             INTEGER")
    old_digest = "4769beb922ec38cdb940f2bc47f1b9d24f24600e799797f8430050f9020f95ac"
    assert hashlib.sha256(old_schema.encode()).hexdigest() == old_digest
    root = tmp_path / "old-schema"
    root.mkdir()
    (root / "arrays").mkdir()
    (root / "exports").mkdir()
    db = root / "arena.sqlite"
    with sqlite3.connect(db) as conn:
        conn.executescript(old_schema)
        conn.execute(f"PRAGMA application_id={ARENA_APPLICATION_ID}")
        conn.execute("PRAGMA user_version=3")
        conn.executemany("INSERT INTO arena_meta(key,value) VALUES (?,?)", (
            ("arena_schema_version", "3"), ("schema_fingerprint", old_digest),
            ("pseudonymization_salt", "0" * 32)))
    before = hashlib.sha256(db.read_bytes()).hexdigest()
    with pytest.raises(ArenaStoreRefusal, match="arena_store_schema_fingerprint_mismatch"):
        ArenaStore(root, create=False)
    assert hashlib.sha256(db.read_bytes()).hexdigest() == before
    with sqlite3.connect(f"file:{db}?mode=ro&immutable=1", uri=True) as conn:
        assert conn.execute("PRAGMA user_version").fetchone()[0] == 3


def test_complete_integer_matrix_and_transactional_refusal(empty_store):
    from nirs4all_benchmarks.ingestion import IngestionPolicy

    policy = IngestionPolicy(collection_id="uploads", bind_card_citation=False)
    positions = (("rng", "root_seed"), ("dataset", "n_samples"), ("dataset", "n_features"),
                 ("scores", "expected_fold_count"), ("scores", "completed_fold_count"),
                 ("cv", "n_folds"), ("cv", "n_repeats"), ("observation", "n_samples"))
    rejected_values = (-1, 2**63, 2**64 - 1, 2**64, 2**70, True, 1.0, "1")

    def snapshot():
        tables = empty_store.query("SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'")
        counts = {row["name"]: empty_store.count(row["name"]) for row in tables}
        return counts, sorted(path.name for path in empty_store.arrays_dir.iterdir()), sorted(
            path.name for path in empty_store.exports_dir.iterdir())

    for block, field in positions:
        valid = (0, 2**63 - 1, 2**63, 2**64 - 1) if block == "rng" else (
            (1, 2**63 - 1) if field in {"expected_fold_count", "n_folds", "n_repeats"} else (0, 2**63 - 1))
        invalid = tuple(v for v in rejected_values if not (block == "rng" and type(v) is int and 0 <= v < 2**64))
        if field in {"expected_fold_count", "n_folds", "n_repeats"}:
            invalid = (0, *invalid)
        for value in (*valid, *invalid):
            source = manifest(terminal="failed", execution_id=f"matrix-{block}-{field}-{value!r}")
            source["scores"]["observations"] = [] if block != "observation" else source["scores"]["observations"]
            target = source["scores"]["observations"][0] if block == "observation" else source.setdefault(block, {})
            target[field] = value
            before = snapshot()
            verdict = validate_metadata(source)
            status, response = _ingest(empty_store.root, json.dumps(source).encode(), "uploads")
            if any(type(value) is type(v) and value == v for v in valid):
                assert verdict.status == "metadata_valid", (block, field, value, verdict.issues)
                assert status == 200 and response["status"] == "committed", (block, field, value, response)
                assert ingest_export(empty_store, source, policy=policy).status == "already_ingested"
            else:
                assert verdict.status == "rejected", (block, field, value)
                assert status == 422 and response["code"] == verdict.issues[0]["code"]
                assert ingest_export(empty_store, source, policy=policy).status == "rejected"
                assert snapshot() == before, (block, field, value)


def test_inprocess_project_associations_and_partial_forgery(tmp_path):
    from fastapi.testclient import TestClient
    from nirs4all_access import ProjectPrincipal, issue_credential

    from nirs4all_benchmarks.service.project import create_project_app, provision_project
    from nirs4all_benchmarks.store import ArenaStore

    credentials, tokens = [], []
    for project in ("a", "b"):
        provision_project(tmp_path, project)
        principal = ProjectPrincipal(project, "test", frozenset({"arena"}), frozenset({"arena.read", "arena.add"}))
        token, credential = issue_credential(principal)
        tokens.append(token)
        credentials.append(credential)
    client = TestClient(create_project_app(tmp_path, credentials))
    headers = [{"Authorization": "Bearer " + token} for token in tokens]
    sources = []
    for number, card in enumerate(({"name": "first"}, {"name": "second"}, {})):
        source = manifest(terminal="failed", execution_id=f"project-{number}")
        source["dataset"]["n_samples"] = 10 + number
        source["dataset"]["dataset_card"] = card
        sources.append(source)
    responses = [client.post("/api/ingest", headers=headers[0], json=source) for source in sources]
    assert all(response.status_code == 200 for response in responses)
    foreign = client.post("/api/ingest", headers=headers[1], json=sources[0])
    assert foreign.status_code == 200
    run = responses[1].json()["execution_hash"]
    no_card = responses[2].json()["execution_hash"]
    detail = client.get(f"/api/run/{run}", headers=headers[0]).json()
    assert detail["dataset"]["association_scope"] == "dataset_fingerprint_first_accepted"
    assert detail["dataset"]["n_samples"] == 10
    assert detail["submission"]["dataset_card_hash"] == fingerprint(sources[1]["dataset"]["dataset_card"])
    assert client.get(f"/api/run/{no_card}", headers=headers[0]).json()["submission"]["dataset_card_hash"] is None
    assert client.get("/api/datasets", headers=headers[0]).json()[0]["card_scope"] == "first_accepted_fingerprint"
    before_b = client.get(f"/api/run/{foreign.json()['execution_hash']}/metadata", headers=headers[1]).json()
    with ArenaStore(tmp_path / "projects" / "a", create=False) as store:
        export_hash = responses[1].json()["arena_export_hash"]
        receipt = store.get("metadata_receipts", "arena_export_hash", export_hash)
        data = json.loads(receipt["receipt_json"])
        data["metadata_validation"]["terminal_status"] = "completed"
        store.conn.execute("UPDATE metadata_receipts SET receipt_json=?, receipt_sha256=? WHERE arena_export_hash=?",
                           (json.dumps(data), fingerprint(data), export_hash))
        store.conn.commit()
    assert client.get(f"/api/run/{run}/metadata", headers=headers[0]).status_code == 500
    assert client.get(f"/api/run/{run}", headers=headers[0]).json()["metadata"]["status"] == "metadata_unavailable"
    assert client.post("/api/ingest", headers=headers[0], json=sources[1]).status_code == 500
    assert client.get(f"/api/run/{foreign.json()['execution_hash']}/metadata", headers=headers[1]).json() == before_b


def test_maximal_scalar_sweep_is_typed_and_in_process(empty_store):
    source = manifest(terminal="failed")
    source["dataset"].update(n_samples=10, n_features=2, dataset_card={
        "name": "test", "task_type": "regression", "axis": {"n": 2, "unit": "nm", "range": [1, 2], "resolution": 1}})
    source["pipeline"] = {"graph": {"nodes": [{"id": "m", "source_id": "s", "operator": "synthetic.Model",
                          "params": {"strength": 2}}]}}
    source["rng"] = {"root_seed": 1, "framework_seeds": {"framework": 1}}
    source["cv"] = {"n_folds": 1, "n_repeats": 1}
    source["execution"]["time_ms"] = 1.0
    source["scores"]["observations"][0].update(n_samples=1, coverage=1.0)
    source["residuals"] = {"key": "sample_id", "publishable": dict.fromkeys(
        ("y_true", "y_pred", "residual", "y_proba"), True), "inline": [{
            "sample_id": "synthetic", "scope": "cv", "partition": "validation", "fold_id": "0",
            "y_true": 1.0, "y_pred": 1.0, "residual": 0.0}]}
    assert validate_metadata(source).status == "metadata_valid"

    def leaves(value, path=()):
        if isinstance(value, dict):
            for key, item in value.items():
                yield from leaves(item, (*path, key))
        elif isinstance(value, list):
            for index, item in enumerate(value):
                yield from leaves(item, (*path, index))
        else:
            yield path

    for path in leaves(source):
        for number in (2**63, 2**64, 2**70, 10**400):
            candidate = copy.deepcopy(source)
            target = candidate
            for key in path[:-1]:
                target = target[key]
            target[path[-1]] = number
            status, result = _ingest(empty_store.root, json.dumps(candidate).encode(), "uploads")
            assert status < 500, (path, number, result)


def test_consistent_rewrite_is_not_a_tamper_resistance_claim(empty_store):
    source = manifest(terminal="failed")
    outcome = ingest_export(empty_store, source)
    receipt = empty_store.get("metadata_receipts", "arena_export_hash", outcome.arena_export_hash)
    data = json.loads(receipt["receipt_json"])
    data["metadata_validation"]["terminal_status"] = "completed"
    empty_store.conn.execute("UPDATE metadata_receipts SET receipt_json=?, receipt_sha256=? WHERE arena_export_hash=?",
                             (json.dumps(data), fingerprint(data), outcome.arena_export_hash))
    empty_store.conn.execute("UPDATE executions SET status='completed' WHERE execution_hash=?", (outcome.execution_hash,))
    empty_store.conn.commit()
    assert ingest_export(empty_store, source).status == "already_ingested"
    assert _metadata(empty_store.root, outcome.execution_hash)["terminal_status"] == "completed"


def test_large_parameter_public_ingest_replay_and_exact_readback(tmp_path):
    from fastapi.testclient import TestClient
    from nirs4all_access import ProjectPrincipal, issue_credential

    from nirs4all_benchmarks.service.project import create_project_app, provision_project
    from nirs4all_benchmarks.store import ArenaStore

    provision_project(tmp_path, "a")
    principal = ProjectPrincipal("a", "test", frozenset({"arena"}), frozenset({"arena.read", "arena.add"}))
    token, credential = issue_credential(principal)
    client = TestClient(create_project_app(tmp_path, [credential]))
    headers = {"Authorization": "Bearer " + token}
    source = manifest(execution_id="large-parameter")
    source["pipeline"]["nodes"][0]["params"] = {"huge": 10**400, "ordinary": 1.25}
    validation = validate_metadata(source)
    assert validation.status == "metadata_valid"
    first = client.post("/api/ingest", headers=headers, json=source)
    assert first.status_code == 200, first.text
    assert first.json()["status"] == "committed"
    assert first.json()["arena_export_hash"] == validation.arena_export_hash
    assert first.json()["run_condition_hash"] == validation.resolved.run_condition_hash
    replay = client.post("/api/ingest", headers=headers, json=source)
    assert replay.status_code == 200 and replay.json()["status"] == "already_ingested"
    assert replay.json()["execution_hash"] == first.json()["execution_hash"]
    detail = client.get(f"/api/run/{first.json()['execution_hash']}", headers=headers)
    assert detail.status_code == 200
    params = {row["name"]: row["value_json"] for row in detail.json()["node_params"]}
    assert json.loads(params["huge"]) == 10**400
    assert json.loads(params["ordinary"]) == 1.25
    with ArenaStore(tmp_path / "projects" / "a", create=False) as store:
        rows = {row["name"]: row for row in store.query(
            "SELECT name, value_json, numeric_value FROM parameter_values WHERE name IN ('huge', 'ordinary')")}
        assert rows["huge"]["value_json"] == str(10**400)
        assert rows["huge"]["numeric_value"] is None
        assert rows["ordinary"]["numeric_value"] == 1.25
        before = (store.count("executions"), store.count("ingestion_batches"), store.count("parameter_values"))
    malformed = json.dumps(source).replace('"ordinary": 1.25', '"ordinary": NaN')
    assert "NaN" in malformed
    refusal = client.post("/api/ingest", headers={**headers, "Content-Type": "application/json"}, content=malformed)
    assert refusal.status_code == 422
    with ArenaStore(tmp_path / "projects" / "a", create=False) as store:
        assert (store.count("executions"), store.count("ingestion_batches"), store.count("parameter_values")) == before


def test_adopted_rng_conflicts_are_atomic_and_project_scoped(tmp_path):
    import pytest
    from fastapi.testclient import TestClient
    from nirs4all_access import ProjectPrincipal, issue_credential

    from nirs4all_benchmarks.ingestion import AtomicIngestionError, IngestionPolicy, ingest_exports_atomic
    from nirs4all_benchmarks.service.project import create_project_app, provision_project
    from nirs4all_benchmarks.store import ArenaStore

    credentials, headers = [], []
    for project in ("a", "b"):
        provision_project(tmp_path, project)
        token, credential = issue_credential(ProjectPrincipal(project, "test", frozenset({"arena"}),
                                           frozenset({"arena.read", "arena.add"})))
        credentials.append(credential)
        headers.append({"Authorization": "Bearer " + token})
    client = TestClient(create_project_app(tmp_path, credentials))
    root = tmp_path / "projects" / "a"

    def snapshot():
        with ArenaStore(root, create=False) as store:
            tables = store.query("SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'")
            return ({row["name"]: store.count(row["name"]) for row in tables},
                    sorted(p.name for p in store.arrays_dir.iterdir()),
                    sorted(p.name for p in store.exports_dir.iterdir()))

    for index, (first_seed, second_seed) in enumerate(((1, 2), (None, 2), (2**64 - 1, 0))):
        first = manifest(execution_id=f"rng-first-{index}")
        first["rng"] = {"rng_context_hash": str(index + 1) * 64, "root_seed": first_seed}
        second = copy.deepcopy(first)
        second["execution"]["execution_id"] = f"rng-second-{index}"
        second["rng"]["root_seed"] = second_seed
        accepted = client.post("/api/ingest", headers=headers[0], json=first)
        assert accepted.status_code == 200 and accepted.json()["status"] == "committed"
        assert client.post("/api/ingest", headers=headers[0], json=first).json()["status"] == "already_ingested"
        same = copy.deepcopy(first)
        same["execution"]["execution_id"] = f"rng-same-{index}"
        assert client.post("/api/ingest", headers=headers[0], json=same).status_code == 200
        before = snapshot()
        refused = client.post("/api/ingest", headers=headers[0], json=second)
        assert refused.status_code == 409 and refused.json()["code"] == "identity_dimension_conflict"
        assert snapshot() == before
        detail = client.get(f"/api/run/{accepted.json()['execution_hash']}", headers=headers[0]).json()
        assert detail["rng"]["root_seed"] == (str(first_seed) if first_seed is not None else None)
        other = client.post("/api/ingest", headers=headers[1], json=second)
        assert other.status_code == 200
        other_detail = client.get(f"/api/run/{other.json()['execution_hash']}", headers=headers[1]).json()
        assert other_detail["rng"]["root_seed"] == str(second_seed)
        fresh = manifest(execution_id=f"atomic-new-{index}")
        fresh["rng"] = {"root_seed": 100 + index}
        with (ArenaStore(root, create=False) as store,
              pytest.raises(AtomicIngestionError, match="identity_dimension_conflict")):
            ingest_exports_atomic(store, [fresh, second], policy=IngestionPolicy(collection_id="uploads"))
        assert snapshot() == before

    for index, (field, changed) in enumerate((("derivation", "other"), ("framework_seeds", {"x": 2}),
                                               ("determinism_flags", {"x": True}))):
        first = manifest(execution_id=f"rng-context-{index}")
        first["rng"] = {"rng_context_hash": str(index + 4) * 64, "root_seed": 7}
        assert client.post("/api/ingest", headers=headers[0], json=first).status_code == 200
        second = copy.deepcopy(first)
        second["execution"]["execution_id"] += "-changed"
        second["rng"][field] = changed
        before = snapshot()
        refused = client.post("/api/ingest", headers=headers[0], json=second)
        assert refused.status_code == 409 and refused.json()["code"] == "identity_dimension_conflict"
        assert snapshot() == before

    for seed in (11, 12):
        source = manifest(execution_id=f"derived-{seed}")
        source["rng"] = {"root_seed": seed}
        response = client.post("/api/ingest", headers=headers[0], json=source)
        assert response.status_code == 200
        assert client.get(f"/api/run/{response.json()['execution_hash']}", headers=headers[0]).json()["rng"]["root_seed"] == str(seed)


@pytest.mark.parametrize("key", ("source_id", "operator"))
def test_graph_node_source_id_and_operator_are_strictly_strings(empty_store, key):
    for bad in (42, 1.5, True, [], {}):
        source = manifest()
        source["pipeline"] = {"graph": {"nodes": [{"id": "m", key: bad}]}}
        verdict = validate_metadata(source)
        assert verdict.status == "rejected" and verdict.issues[0]["code"] == "graph_invalid", (key, bad)
        status, body = _ingest(empty_store.root, json.dumps(source).encode(), "uploads")
        assert status == 422 and body["code"] == "graph_invalid"
        assert empty_store.count("executions") == 0


def test_repeats_schema_minimum_and_runtime_refusal():
    from nirs4all_benchmarks.contract.schema import ARENA_RUN_EXPORT_SCHEMA, validate_manifest

    schema = ARENA_RUN_EXPORT_SCHEMA["$defs"]["CVBlock"]["properties"]["n_repeats"]
    assert next(branch for branch in schema["anyOf"] if branch.get("type") == "integer")["minimum"] == 1
    for value in (0, -1):
        source = manifest()
        source["cv"] = {"n_repeats": value}
        assert validate_manifest(source)
        verdict = validate_metadata(source)
        assert verdict.status == "rejected" and verdict.issues[0]["code"] == "fold_count_contradiction"


def test_adopted_refit_immutable_facts_and_project_isolation(tmp_path):
    from fastapi.testclient import TestClient
    from nirs4all_access import ProjectPrincipal, issue_credential

    from nirs4all_benchmarks.ingestion import AtomicIngestionError, IngestionPolicy, ingest_exports_atomic
    from nirs4all_benchmarks.service.project import create_project_app, provision_project
    from nirs4all_benchmarks.store import ArenaStore

    credentials, headers = [], []
    for name in ("a", "b"):
        provision_project(tmp_path, name)
        token, credential = issue_credential(ProjectPrincipal(name, "test", frozenset({"arena"}),
                                           frozenset({"arena.read", "arena.add"})))
        credentials.append(credential)
        headers.append({"Authorization": "Bearer " + token})
    client = TestClient(create_project_app(tmp_path, credentials))
    root = tmp_path / "projects" / "a"

    def snapshot():
        with ArenaStore(root, create=False) as store:
            tables = store.query("SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'")
            return ({row["name"]: store.count(row["name"]) for row in tables},
                    sorted(p.name for p in store.arrays_dir.iterdir()),
                    sorted(p.name for p in store.exports_dir.iterdir()))

    source = manifest(terminal="failed", execution_id="refit-first")
    source["refit"] = {"refit_strategy_hash": "e" * 64, "strategy": "first"}
    result = client.post("/api/ingest", headers=headers[0], json=source)
    assert result.status_code == 200 and result.json()["status"] == "committed"
    assert client.post("/api/ingest", headers=headers[0], json=source).json()["status"] == "already_ingested"
    identical = copy.deepcopy(source)
    identical["execution"]["execution_id"] = "refit-same-facts"
    assert client.post("/api/ingest", headers=headers[0], json=identical).status_code == 200
    for field, value in (("strategy", "second"), ("selection_scope", "cv"),
                         ("train_scope", "train"), ("params", {"x": 1})):
        conflict = copy.deepcopy(source)
        conflict["execution"]["execution_id"] = "refit-conflict-" + field
        conflict["refit"][field] = value
        before = snapshot()
        response = client.post("/api/ingest", headers=headers[0], json=conflict)
        assert response.status_code == 409 and response.json()["code"] == "identity_dimension_conflict"
        assert snapshot() == before
    different = copy.deepcopy(source)
    different["execution"]["execution_id"] = "refit-other-project"
    different["refit"]["strategy"] = "second"
    assert client.post("/api/ingest", headers=headers[1], json=different).status_code == 200
    before = snapshot()
    fresh = manifest(terminal="failed", execution_id="refit-atomic-fresh")
    fresh["refit"] = {"strategy": "new"}
    with (ArenaStore(root, create=False) as store,
          pytest.raises(AtomicIngestionError, match="identity_dimension_conflict")):
        ingest_exports_atomic(store, [fresh, different], policy=IngestionPolicy(collection_id="uploads"))
    assert snapshot() == before
    known = manifest(terminal="failed", execution_id="refit-known")
    known["refit"] = {"refit_strategy_hash": "f" * 64, "selection_scope": "cv"}
    assert client.post("/api/ingest", headers=headers[0], json=known).status_code == 200
    unknown = copy.deepcopy(known)
    unknown["execution"]["execution_id"] = "refit-unknown"
    unknown["refit"].pop("selection_scope")
    before = snapshot()
    refused = client.post("/api/ingest", headers=headers[0], json=unknown)
    assert refused.status_code == 409 and refused.json()["code"] == "identity_dimension_conflict"
    assert snapshot() == before
    for strategy in ("derived-first", "derived-second"):
        derived = manifest(terminal="failed", execution_id=strategy)
        derived["refit"] = {"strategy": strategy}
        assert client.post("/api/ingest", headers=headers[0], json=derived).status_code == 200


@pytest.mark.parametrize("kind", ("split", "cv"))
def test_adopted_split_cv_immutable_facts_and_isolation(tmp_path, kind):
    from nirs4all_benchmarks.ingestion import AtomicIngestionError, IngestionPolicy, ingest_exports_atomic
    from nirs4all_benchmarks.store import ArenaStore

    roots = [tmp_path / "a", tmp_path / "b"]
    for root in roots:
        with ArenaStore(root):
            pass
    def post(root, value):
        return _ingest(root, json.dumps(value).encode(), "uploads")
    def snapshot(root):
        with ArenaStore(root, create=False) as store:
            tables = store.query("SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'")
            return ({row["name"]: store.count(row["name"]) for row in tables},
                    sorted(p.name for p in store.arrays_dir.iterdir()),
                    sorted(p.name for p in store.exports_dir.iterdir()))
    source = manifest(terminal="failed", execution_id=f"{kind}-first")
    if kind == "split":
        source["split"] = {"split_instance_hash": "a" * 64, "partition_summary": {"sizes": [1]}}
        field, changed = "partition_summary", {"sizes": [2]}
    else:
        source["cv"] = {"cv_instance_hash": "b" * 64, "n_folds": 2}
        field, changed = "n_folds", 3
    status, accepted = post(roots[0], source)
    assert status == 200 and accepted["status"] == "committed"
    assert post(roots[0], source)[1]["status"] == "already_ingested"
    same = copy.deepcopy(source)
    same["execution"]["execution_id"] += "-same"
    assert post(roots[0], same)[0] == 200
    conflict = copy.deepcopy(source)
    conflict["execution"]["execution_id"] += "-changed"
    conflict[kind][field] = changed
    before = snapshot(roots[0])
    status, refused = post(roots[0], conflict)
    assert status == 409 and refused["code"] == "identity_dimension_conflict"
    assert snapshot(roots[0]) == before
    assert post(roots[1], conflict)[0] == 200
    fresh = manifest(terminal="failed", execution_id=f"{kind}-atomic")
    fresh["refit"] = {"strategy": f"{kind}-atomic"}
    with (ArenaStore(roots[0], create=False) as store,
          pytest.raises(AtomicIngestionError, match="identity_dimension_conflict")):
        ingest_exports_atomic(store, [fresh, conflict], policy=IngestionPolicy(collection_id="uploads"))
    assert snapshot(roots[0]) == before
    for index, value in enumerate((1, 2)):
        derived = manifest(terminal="failed", execution_id=f"{kind}-derived-{index}")
        derived[kind] = {field: {"sizes": [value]} if kind == "split" else value}
        assert post(roots[0], derived)[0] == 200
    unknown = manifest(terminal="failed", execution_id=f"{kind}-unknown")
    unknown[kind] = ({"split_instance_hash": "c" * 64} if kind == "split" else {"cv_instance_hash": "d" * 64})
    assert post(roots[0], unknown)[0] == 200
    known = copy.deepcopy(unknown)
    known["execution"]["execution_id"] += "-known"
    known[kind][field] = changed
    before = snapshot(roots[0])
    status, refused = post(roots[0], known)
    assert status == 409 and refused["code"] == "identity_dimension_conflict"
    assert snapshot(roots[0]) == before


def test_per_run_rng_and_derived_dimensions(tmp_path):
    from nirs4all_benchmarks.store import ArenaStore

    root = tmp_path / "arena"
    with ArenaStore(root) as store:
        expected = []
        for seed in (1, 2, 2**64 - 1, None):
            source = manifest(terminal="failed", execution_id=f"seed-{seed}")
            source["rng"] = {"root_seed": seed}
            validation = validate_metadata(source)
            status, result = _ingest(root, json.dumps(source).encode(), "uploads")
            assert status == 200 and result["status"] == "committed"
            detail = Queries(store).run_api_detail(result["execution_hash"])
            assert detail["rng"]["root_seed"] == (str(seed) if seed is not None else None)
            assert detail["run_condition"]["rng_context_hash"] == validation.resolved.rng_context_hash
            assert "rng_context_hash" not in detail["cv"]
            assert _ingest(root, json.dumps(source).encode(), "uploads")[1]["status"] == "already_ingested"
            expected.append((validation.resolved.split_instance_hash, validation.resolved.cv_instance_hash))
        assert len(set(expected)) == 1
        assert store.count("split_instances") == store.count("cv_instances") == 1
        for table in ("split_instances", "cv_instances"):
            assert "rng_context_hash" not in {row["name"] for row in store.query(f"PRAGMA table_info({table})")}
        assert store.query("PRAGMA foreign_key_check") == []


def test_adopted_dimension_full_matrix(tmp_path):
    from nirs4all_benchmarks.ingestion import AtomicIngestionError, IngestionPolicy, ingest_exports_atomic
    from nirs4all_benchmarks.store import ArenaStore

    roots = (tmp_path / "a", tmp_path / "b")
    for root in roots:
        with ArenaStore(root):
            pass
    def post(root, payload):
        return _ingest(root, json.dumps(payload).encode(), "uploads")
    def snapshot(root):
        with ArenaStore(root, create=False) as store:
            tables = store.query("SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'")
            return ({r["name"]: store.count(r["name"]) for r in tables},
                    sorted(p.name for p in store.arrays_dir.iterdir()),
                    sorted(p.name for p in store.exports_dir.iterdir()))
    cases = (("split", "partition_summary", {"sizes": [2]}), ("split", "params", {"seed": 2}),
             ("cv", "n_folds", 3), ("cv", "n_repeats", 3), ("cv", "method", "different"),
             ("cv", "nested", True), ("cv", "within_train_only", True))
    for index, (kind, field, changed) in enumerate(cases):
        source = manifest(terminal="failed", execution_id=f"case-{index}")
        source["scores"]["observations"] = []
        key = "split_instance_hash" if kind == "split" else "cv_instance_hash"
        source[kind] = {key: f"{index + 1:064x}"}
        if kind == "cv":
            source[kind]["n_folds"] = 2
        status, accepted = post(roots[0], source)
        assert status == 200 and accepted["status"] == "committed"
        assert post(roots[0], source)[1]["status"] == "already_ingested"
        same = copy.deepcopy(source)
        same["execution"]["execution_id"] += "-same"
        same["rng"] = {"root_seed": index + 10}
        assert post(roots[0], same)[0] == 200
        conflict = copy.deepcopy(source)
        conflict["execution"]["execution_id"] += "-conflict"
        conflict[kind][field] = changed
        before = snapshot(roots[0])
        status, response = post(roots[0], conflict)
        assert status == 409 and response["code"] == "identity_dimension_conflict", (kind, field, response)
        assert snapshot(roots[0]) == before
        assert post(roots[1], conflict)[0] == 200
        assert snapshot(roots[0]) == before
        fresh = manifest(terminal="failed", execution_id=f"atomic-{index}")
        fresh["refit"] = {"strategy": f"atomic-{index}"}
        for pack in ([fresh, conflict], [conflict, fresh]):
            with (ArenaStore(roots[0], create=False) as store,
                  pytest.raises(AtomicIngestionError, match="identity_dimension_conflict")):
                ingest_exports_atomic(store, pack, policy=IngestionPolicy(collection_id="uploads"))
            assert snapshot(roots[0]) == before
    for index, kind in enumerate(("split", "cv")):
        for first_known in (False, True):
            source = manifest(terminal="failed", execution_id=f"known-{kind}-{first_known}")
            source["scores"]["observations"] = []
            key = "split_instance_hash" if kind == "split" else "cv_instance_hash"
            field = "partition_summary" if kind == "split" else "n_folds"
            value = {"sizes": [1]} if kind == "split" else 2
            source[kind] = {key: f"{100 + 2 * index + int(first_known):064x}"}
            if first_known:
                source[kind][field] = value
            assert post(roots[0], source)[0] == 200
            conflict = copy.deepcopy(source)
            conflict["execution"]["execution_id"] += "-other"
            if first_known:
                conflict[kind].pop(field)
            else:
                conflict[kind][field] = value
            before = snapshot(roots[0])
            assert post(roots[0], conflict)[0] == 409
            assert snapshot(roots[0]) == before
    derived = manifest(terminal="failed", execution_id="derived-then-adopted")
    resolved = validate_metadata(derived).resolved
    assert post(roots[0], derived)[0] == 200
    adopted = copy.deepcopy(derived)
    adopted["execution"]["execution_id"] += "-adopted"
    adopted["split"] = {"split_instance_hash": resolved.split_instance_hash}
    adopted["cv"] = {"cv_instance_hash": resolved.cv_instance_hash}
    assert post(roots[0], adopted)[0] == 200


def test_previous_seed_text_schema_refused_without_writes(tmp_path):
    import hashlib
    import sqlite3

    from nirs4all_benchmarks.store import ArenaStore, ArenaStoreRefusal
    from nirs4all_benchmarks.store.arena_store import _SCHEMA_SQL, ARENA_APPLICATION_ID

    old = _SCHEMA_SQL.replace(
        "    split_spec_hash      TEXT REFERENCES split_specs (split_spec_hash),\n",
        "    split_spec_hash      TEXT REFERENCES split_specs (split_spec_hash),\n    rng_context_hash     TEXT,\n",
    ).replace(
        "    cv_spec_hash              TEXT REFERENCES cv_specs (cv_spec_hash),\n",
        "    cv_spec_hash              TEXT REFERENCES cv_specs (cv_spec_hash),\n    rng_context_hash          TEXT,\n",
    )
    digest = "ce36b0074fe9e6727d58074aac0f822bac94e42b42297581b9a555ba05d12797"
    assert hashlib.sha256(old.encode()).hexdigest() == digest
    root = tmp_path / "old"
    root.mkdir()
    (root / "arrays").mkdir()
    (root / "exports").mkdir()
    db = root / "arena.sqlite"
    with sqlite3.connect(db) as conn:
        conn.executescript(old)
        conn.execute(f"PRAGMA application_id={ARENA_APPLICATION_ID}")
        conn.execute("PRAGMA user_version=3")
        conn.executemany("INSERT INTO arena_meta(key,value) VALUES (?,?)", (
            ("arena_schema_version", "3"), ("schema_fingerprint", digest), ("pseudonymization_salt", "0" * 32)))
    before = db.read_bytes()
    names = sorted(x.name for x in root.iterdir())
    with pytest.raises(ArenaStoreRefusal, match="arena_store_schema_fingerprint_mismatch"):
        ArenaStore(root, create=False)
    assert db.read_bytes() == before and sorted(x.name for x in root.iterdir()) == names


def test_generated_cv_schema_and_repeat_runtime_contract():
    from nirs4all_benchmarks.contract.arena_run_export import ArenaRunExport, CVBlock
    from nirs4all_benchmarks.contract.schema import ARENA_RUN_EXPORT_SCHEMA, validate_manifest

    generated = CVBlock.model_json_schema()
    checked = ARENA_RUN_EXPORT_SCHEMA["$defs"]["CVBlock"]
    assert generated == checked
    assert ArenaRunExport.model_json_schema()["$defs"]["CVBlock"] == checked
    assert CVBlock.model_validate({"n_repeats": 0}).n_repeats == 0
    for value in (None, 1):
        source = manifest(terminal="failed")
        source["cv"] = {"n_repeats": value}
        assert not validate_manifest(source)
        assert validate_metadata(source).status == "metadata_valid"
    source = manifest(terminal="failed")
    source["cv"] = {"n_repeats": 0}
    assert validate_manifest(source)
    result = validate_metadata(source)
    assert result.status == "rejected" and result.issues[0]["code"] == "fold_count_contradiction"
