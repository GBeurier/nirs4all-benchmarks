"""In-process tests for the isolated project front door."""

import json

from fastapi.testclient import TestClient
from nirs4all_access import ProjectPrincipal, issue_credential

from nirs4all_benchmarks.service.project import PROJECT_ROUTE_TABLE, create_project_app, provision_project
from test_arena_v2 import record


def test_auth_isolation_strict_json_and_metadata(tmp_path):
    for name in ("a", "b"):
        provision_project(tmp_path, name)
    credentials = []
    tokens = []
    for name in ("a", "b"):
        principal = ProjectPrincipal(name, "test", frozenset({"arena"}),
                                     frozenset({"arena.read", "arena.add"}))
        token, credential = issue_credential(principal)
        tokens.append(token)
        credentials.append(credential)
    application = create_project_app(tmp_path, credentials)
    routes = {(method, route.path) for route in application.project_apps["a"].routes for method in route.methods}
    assert routes == PROJECT_ROUTE_TABLE and len(routes) == 27
    client = TestClient(application)
    headers = {"Authorization": "Bearer " + tokens[0]}
    assert client.get("/api/overview").status_code == 401
    assert client.get("/docs", headers=headers).status_code == 404
    assert client.post("/api/ingest", headers={**headers, "Content-Type": "application/json"},
                       content=b'{"x":1,"x":2}').status_code == 422
    payload = record()
    payload["dataset"]["visibility"] = "private"
    payload["dataset"]["visibility_source"] = "local_policy"
    payload["dataset"]["dataset_card"] = {"description": "/client/metadata.parquet"}
    payload["provenance"] = {"extra": {"null": None}}
    payload["residuals"] = {"key": "sample_id", "pseudonymized": False,
        "publishable": dict.fromkeys(("y_true", "y_pred", "residual", "y_proba"), True),
        "inline": [{"sample_id": "shared-input-id", "scope": "cv", "partition": "validation",
                    "fold_id": "0", "y_true": 1.0, "y_pred": 0.5, "residual": 0.5}]}
    response = client.post("/api/ingest", headers=headers, json=payload)
    assert response.status_code == 200, response.text
    run = response.json()["execution_hash"]
    assert client.post("/api/ingest", headers=headers, json=payload).json()["status"] == "already_ingested"
    metadata = client.get(f"/api/run/{run}/metadata", headers=headers).json()
    assert metadata["submitted_metadata"]["metadata"]["dataset"]["dataset_card"] == payload["dataset"]["dataset_card"]
    assert metadata["submitted_metadata"]["metadata"]["provenance"] == payload["provenance"]
    other = {"Authorization": "Bearer " + tokens[1]}
    assert client.get(f"/api/run/{run}/metadata", headers=other).status_code == 404
    assert client.post("/api/ingest", headers=other, json=payload).json()["status"] == "committed"
    assert client.get(f"/api/run/{run}/metadata", headers=other).json()["arena_export_hash"] == response.json()["arena_export_hash"]
    a_rows = client.get(f"/api/run/{run}/residuals", headers=headers).json()
    b_rows = client.get(f"/api/run/{run}/residuals", headers=other).json()
    assert a_rows[0]["sample_id"] != b_rows[0]["sample_id"]
    detail = client.get(f"/api/run/{run}", headers=headers).json()
    assert "parquet_path" not in detail["residual_set"]
    assert detail['execution']['visibility'] == 'private'
    assert str(tmp_path) not in json.dumps(detail)
    for method, template in PROJECT_ROUTE_TABLE:
        if method != 'GET':
            continue
        route = template.replace('{execution_hash}', run)
        if route == '/api/compare':
            route += f'?a={run}&b={run}'
        text = client.get(route, headers=headers).text
        assert str(tmp_path) not in text and 'arrays/residuals_' not in text and tokens[0] not in text
    restarted = TestClient(create_project_app(tmp_path, credentials))
    assert restarted.get(f'/api/run/{run}/metadata', headers=headers).status_code == 200
    assert restarted.post('/api/ingest', headers=headers, json=payload).json()['status'] == 'already_ingested'
    assert str(tmp_path) not in json.dumps(metadata)


def test_rights_visibility_limits_and_fixed_errors(tmp_path):
    provision_project(tmp_path, "a")
    def issue(right):
        return issue_credential(ProjectPrincipal("a", "test", frozenset({"arena"}), frozenset({right})))
    read_token, read_record = issue("arena.read")
    add_token, add_record = issue("arena.add")
    client = TestClient(create_project_app(tmp_path, [read_record, add_record]))
    read = {"Authorization": "Bearer " + read_token}
    add = {"Authorization": "Bearer " + add_token}
    for method, path in PROJECT_ROUTE_TABLE:
        resolved = path.replace('{execution_hash}', 'a' * 64)
        if method == 'GET':
            assert client.request(method, resolved, headers=add).status_code == 403
        else:
            assert client.request(method, resolved, headers=read).status_code == 403
    assert client.get("/api/overview", headers=add).status_code == 403
    assert client.post("/api/ingest", headers=read, json={}).status_code == 403
    assert client.get("/api/runs?limit=0", headers=read).json() == {"code": "invalid_query"}
    assert client.get("/api/overview?token=secret", headers=read).json() == {"code": "selector_refused"}
    assert client.get("/api/overview/", headers=read).status_code == 404
    assert client.head("/api/overview", headers=read).status_code == 404
    assert client.options("/api/overview", headers=read).status_code == 404
    for source in (None, "unreported", "local_policy", "producer_reported"):
        public = record()
        if source is None:
            public["dataset"].pop("visibility_source")
        else:
            public["dataset"]["visibility_source"] = source
        assert client.post("/api/ingest", headers=add, json=public).json()["code"] == "public_visibility_unsupported"
    payload = record()
    payload["dataset"]["visibility"] = "private"
    payload["dataset"]["visibility_source"] = "local_policy"
    wrong_version = dict(payload, arena_export_schema_version=1)
    assert client.post("/api/ingest", headers=add, json=wrong_version).status_code == 422
    reference = dict(payload, residuals={"ref": "/client/never-open"})
    assert client.post("/api/ingest", headers=add, json=reference).status_code == 422
    payload["provenance"] = {"prov_jsonld": "x" * 65537}
    assert client.post("/api/ingest", headers=add, json=payload).status_code == 422
    assert client.get("/api/run/" + "0" * 64, headers=read).content == client.get(
        "/api/run/bad", headers=read).content


def test_early_refusals_never_read_body(tmp_path):
    import asyncio

    provision_project(tmp_path, "a")
    token, credential = issue_credential(ProjectPrincipal("a", "test", frozenset({"arena"}),
                                                        frozenset({"arena.read"})))
    add_token, add_credential = issue_credential(ProjectPrincipal("a", "writer", frozenset({"arena"}),
                                                                frozenset({"arena.add"})))
    app = create_project_app(tmp_path, [credential, add_credential])
    async def request(method, path, headers, query=b""):
        calls = []
        sent = []
        async def receive():
            calls.append(True)
            raise AssertionError("body was read")
        async def send(message):
            sent.append(message)
        await app({"type": "http", "method": method, "path": path, "headers": headers,
                   "query_string": query}, receive, send)
        assert not calls
        return sent[0]["status"]
    auth = [(b"authorization", ("Bearer " + token).encode())]
    assert asyncio.run(request("POST", "/api/ingest", [])) == 401
    assert asyncio.run(request("POST", "/api/ingest", auth)) == 403
    assert asyncio.run(request("POST", "/unknown", auth)) == 404
    assert asyncio.run(request("GET", "/api/overview", auth, b"project=b")) == 400
    add = [(b"authorization", ("Bearer " + add_token).encode())]
    assert asyncio.run(request("POST", "/api/ingest", add)) == 415
    assert asyncio.run(request("POST", "/api/ingest", [*add, (b"content-type", b"application/json"),
                                                               (b"content-length", b"8388609")])) == 413
    assert asyncio.run(request("GET", "/api/overview", auth + auth)) == 401


def test_terminal_states_remain_truthful(tmp_path):
    provision_project(tmp_path, "a")
    token, credential = issue_credential(ProjectPrincipal("a", "test", frozenset({"arena"}),
        frozenset({"arena.read", "arena.add"})))
    client = TestClient(create_project_app(tmp_path, [credential]))
    headers = {"Authorization": "Bearer " + token}
    for terminal, status in (("failed", "failed"), ("incomplete", "failed"), ("cancelled", "cancelled")):
        payload = record(execution_id=terminal)
        payload["dataset"]["visibility"] = "private"
        payload["dataset"]["visibility_source"] = "local_policy"
        payload["execution"].update(status=status, terminal_status=terminal)
        response = client.post("/api/ingest", headers=headers, json=payload)
        assert response.status_code == 200, response.text
        assert response.json()["terminal_status"] == terminal
        assert response.json()["scientific_verification"] == "not_performed"
        run = response.json()["execution_hash"]
        metadata = client.get(f"/api/run/{run}/metadata", headers=headers).json()
        assert metadata["terminal_status"] == terminal
        assert metadata['receipt']['scores']['observations'][0]['metric_value'] == 1.0
        assert metadata['receipt']['leakage_attestation']['oof_enforced'] is True


def test_cancelled_metadata_reads_hold_project_slots_until_workers_finish(tmp_path, monkeypatch):
    import asyncio
    import threading

    import nirs4all_benchmarks.service.project as project

    provision_project(tmp_path, "a")
    token, credential = issue_credential(ProjectPrincipal("a", "test", frozenset({"arena"}),
                                                        frozenset({"arena.read"})))
    app = create_project_app(tmp_path, [credential])
    release = threading.Event()
    mutex = threading.Lock()
    started = 0
    active = set()
    finished = set()
    extra_entered = threading.Event()
    def blocked(_root, _hash):
        nonlocal started
        with mutex:
            started += 1
            ordinal = started
            active.add(ordinal)
            if ordinal > 8:
                extra_entered.set()
        try:
            if not release.wait(5):
                raise RuntimeError("worker deadline")
        finally:
            with mutex:
                active.remove(ordinal)
                finished.add(ordinal)
        return None
    monkeypatch.setattr(project, "_metadata", blocked)

    async def run():
        scope = {"type": "http", "method": "GET", "path": "/api/run/" + "a" * 64 + "/metadata",
                 "headers": [(b"authorization", ("Bearer " + token).encode())], "query_string": b""}
        async def receive():
            raise AssertionError("unexpected body read")
        async def request():
            messages = []
            async def send(message):
                messages.append(message)
            await app(scope, receive, send)
            return messages[0]["status"]
        tasks = [asyncio.create_task(request()) for _ in range(8)]
        probe = None
        try:
            for _ in range(200):
                with mutex:
                    count = started
                if count == 8:
                    break
                await asyncio.sleep(0.005)
            assert count == 8
            for task in tasks:
                task.cancel()
            await asyncio.sleep(0.05)
            for task in tasks:
                task.cancel()
            await asyncio.sleep(0.05)
            probe = asyncio.create_task(request())
            for _ in range(100):
                if extra_entered.is_set() or probe.done():
                    break
                await asyncio.sleep(0.005)
            with mutex:
                observed = (started, sorted(active), sorted(finished))
            print(f"reader_observed started={observed[0]} active={observed[1]} finished={observed[2]} "
                  f"extra_entered={extra_entered.is_set()} pending_originals={sum(not t.done() for t in tasks)}",
                  flush=True)
            assert set(range(1, 9)).issubset(observed[1]) and not observed[2] and not release.is_set()
            assert not extra_entered.is_set() and observed[0] == 8
            assert all(not task.done() for task in tasks)
            assert probe.done() and probe.result() == 503
        finally:
            release.set()
            await asyncio.gather(*tasks, *([probe] if probe is not None else []), return_exceptions=True)
    asyncio.run(run())


def test_cancelled_writer_holds_its_slot_until_worker_finishes(tmp_path, monkeypatch):
    import asyncio
    import threading

    import nirs4all_benchmarks.service.project as project

    provision_project(tmp_path, 'a')
    token, credential = issue_credential(ProjectPrincipal('a', 'writer', frozenset({'arena'}),
                                                        frozenset({'arena.add'})))
    app = create_project_app(tmp_path, [credential])
    started, release = threading.Event(), threading.Event()
    second_entered = threading.Event()
    mutex = threading.Lock()
    entered = 0
    active = set()
    finished = set()
    def blocked(_root, _body, _collection):
        nonlocal entered
        with mutex:
            entered += 1
            ordinal = entered
            active.add(ordinal)
            started.set()
            if ordinal > 1:
                second_entered.set()
        try:
            assert release.wait(5)
        finally:
            with mutex:
                active.remove(ordinal)
                finished.add(ordinal)
        return 200, {'status': 'synthetic'}
    monkeypatch.setattr(project, '_ingest', blocked)
    async def run():
        scope = {'type': 'http', 'method': 'POST', 'path': '/api/ingest', 'query_string': b'',
                 'headers': [(b'authorization', ('Bearer ' + token).encode()),
                             (b'content-type', b'application/json')]}
        async def request():
            sent = []
            async def receive():
                return {'type': 'http.request', 'body': b'{}'}
            async def send(message):
                sent.append(message)
            await app(scope, receive, send)
            return sent[0]['status']
        first = asyncio.create_task(request())
        probe = None
        try:
            for _ in range(200):
                if started.is_set():
                    break
                await asyncio.sleep(0.005)
            assert started.is_set()
            first.cancel()
            await asyncio.sleep(0.05)
            first.cancel()
            await asyncio.sleep(0.05)
            probe = asyncio.create_task(request())
            for _ in range(100):
                if second_entered.is_set() or probe.done():
                    break
                await asyncio.sleep(0.005)
            with mutex:
                observed = (entered, sorted(active), sorted(finished))
            print(f"writer_observed started={observed[0]} active={observed[1]} finished={observed[2]} "
                  f"second_entered={second_entered.is_set()} first_pending={not first.done()}", flush=True)
            assert 1 in observed[1] and not observed[2] and not release.is_set()
            assert not second_entered.is_set() and observed[0] == 1
            assert not first.done()
            assert probe.done() and probe.result() == 503
        finally:
            release.set()
            await asyncio.gather(first, *([probe] if probe is not None else []), return_exceptions=True)
    asyncio.run(run())


def test_distinct_cards_source_replay_and_collision_precheck(tmp_path, monkeypatch):
    import copy

    import nirs4all_benchmarks.datasets as datasets
    from nirs4all_benchmarks.ingestion import validate_metadata
    from nirs4all_benchmarks.store import ArenaStore

    provision_project(tmp_path, "a")
    token, credential = issue_credential(ProjectPrincipal("a", "test", frozenset({"arena"}),
        frozenset({"arena.read", "arena.add"})))
    client = TestClient(create_project_app(tmp_path, [credential]))
    headers = {"Authorization": "Bearer " + token}
    first = record(execution_id="first")
    first["dataset"].update(visibility="private", visibility_source="local_policy",
        dataset_card={"name": "one", "citation_id": "unregistered", "extra": {"nested": None}},
        dataset_spec_ref="file:///client/origin.parquet")
    first["provenance"] = {"extra": {"nested": None}}
    second = copy.deepcopy(first)
    second["execution"]["execution_id"] = "second"
    second["dataset"]["dataset_card"] = {"name": "two", "visibility": "public", "license": "asserted"}
    one = client.post("/api/ingest", headers=headers, json=first)
    two = client.post("/api/ingest", headers=headers, json=second)
    assert one.status_code == two.status_code == 200
    for response, source in ((one, first), (two, second)):
        metadata = client.get('/api/run/' + response.json()["execution_hash"] + '/metadata', headers=headers).json()
        fragments = metadata["submitted_metadata"]["metadata"]
        assert fragments["dataset"]["dataset_card"] == source["dataset"]["dataset_card"]
        assert fragments["dataset"]["dataset_spec_ref"] == "file:///client/origin.parquet"
        assert "source_registry" not in fragments["dataset"]["dataset_card"]
        assert metadata["submitted_metadata"]["verification"] == "source_declared_unverified"
        assert str(tmp_path) not in json.dumps(metadata)
        assert "file:///client/origin.parquet" not in json.dumps(metadata["receipt"])
        assert client.post('/api/ingest', headers=headers, json=metadata['receipt']).status_code == 422
    null_card = copy.deepcopy(first)
    null_card["dataset"]["dataset_card"] = None
    assert validate_metadata(null_card).status == "rejected"
    omitted = copy.deepcopy(first)
    omitted.pop("provenance")
    null = copy.deepcopy(omitted)
    null["provenance"] = None
    assert validate_metadata(omitted).arena_export_hash == validate_metadata(null).arena_export_hash
    empty = copy.deepcopy(omitted)
    empty["provenance"] = {}
    null_field = copy.deepcopy(empty)
    null_field["provenance"] = {"prov_jsonld": None}
    assert validate_metadata(empty).arena_export_hash == validate_metadata(null_field).arena_export_hash
    with ArenaStore(tmp_path / 'projects' / 'a', create=False) as store:
        before = {table: store.count(table) for table in ('executions', 'metadata_receipts', 'dataset_cards')}
        first_row = store.get('submitted_metadata', 'arena_export_hash', one.json()['arena_export_hash'])
        batch = store.query_one('SELECT clean_report_json FROM ingestion_batches WHERE input_export_hash=?',
                                (one.json()['arena_export_hash'],))
        assert 'file:///client/origin.parquet' not in batch['clean_report_json']
    builder = datasets.build_dataset_card_row
    def collide(dataset, now):
        row = builder(dataset, now)
        if row is not None:
            row['dataset_card_hash'] = first_row['dataset_card_hash']
        return row
    monkeypatch.setattr(datasets, 'build_dataset_card_row', collide)
    third = copy.deepcopy(second)
    third['execution']['execution_id'] = 'third'
    third['dataset']['dataset_card'] = {'name': 'collision'}
    assert client.post('/api/ingest', headers=headers, json=third).status_code == 409
    with ArenaStore(tmp_path / 'projects' / 'a', create=False) as store:
        assert {table: store.count(table) for table in before} == before


def test_fixed_internal_error_never_echoes_server_or_request_data(tmp_path, monkeypatch, caplog):
    import nirs4all_benchmarks.service.project as project

    provision_project(tmp_path, "a")
    token, credential = issue_credential(ProjectPrincipal("a", "test", frozenset({"arena"}),
                                                        frozenset({"arena.read"})))
    client = TestClient(create_project_app(tmp_path, [credential]))
    def fail(_root, _hash):
        raise RuntimeError(f"raw exception {tmp_path} {token}")
    monkeypatch.setattr(project, "_metadata", fail)
    response = client.get('/api/run/' + 'a' * 64 + '/metadata?opaque=query-canary',
                          headers={'Authorization': 'Bearer ' + token})
    assert response.status_code == 500 and response.json() == {'code': 'internal_error'}
    combined = response.text + caplog.text
    assert str(tmp_path) not in combined and token not in combined and 'query-canary' not in combined


def test_bounded_stream_and_body_deadline(tmp_path, monkeypatch):
    import asyncio

    import nirs4all_benchmarks.service.project as project

    provision_project(tmp_path, 'a')
    token, credential = issue_credential(ProjectPrincipal('a', 'writer', frozenset({'arena'}),
                                                        frozenset({'arena.add'})))
    app = create_project_app(tmp_path, [credential])
    headers = [(b'authorization', ('Bearer ' + token).encode()), (b'content-type', b'application/json')]
    async def request(messages, extra=()):
        sent = []
        async def receive():
            return messages.pop(0)
        async def send(message):
            sent.append(message)
        await app({'type': 'http', 'method': 'POST', 'path': '/api/ingest',
                   'headers': headers + list(extra), 'query_string': b''}, receive, send)
        return sent[0]['status']
    chunks = [{'type': 'http.request', 'body': b'x' * (4 * 1024 * 1024), 'more_body': True},
              {'type': 'http.request', 'body': b'y' * (4 * 1024 * 1024 + 1), 'more_body': False}]
    assert asyncio.run(request(chunks)) == 413
    assert asyncio.run(request([{'type': 'http.request', 'body': b'{}'}],
                               [(b'content-length', b'3')])) == 400
    async def deadline(coro, timeout):
        assert timeout == 30
        coro.close()
        raise TimeoutError
    with monkeypatch.context() as patch:
        patch.setattr(project.asyncio, 'wait_for', deadline)
        assert asyncio.run(request([])) == 408


def test_startup_rejects_unprovisioned_symlinks_and_shared_database(tmp_path):
    import os

    import pytest

    token, credential = issue_credential(ProjectPrincipal('a', 'test', frozenset({'arena'}),
                                                        frozenset({'arena.read'})))
    del token
    bad = tmp_path / 'marker-first'
    (bad / 'projects' / 'a').mkdir(parents=True)
    (bad / 'projects' / 'a' / 'project.json').write_text('{"project_id":"a"}')
    with pytest.raises((OSError, ValueError)):
        create_project_app(bad, [credential])
    linked = tmp_path / 'linked'
    linked.mkdir()
    (linked / 'projects').symlink_to(bad / 'projects', target_is_directory=True)
    with pytest.raises(ValueError):
        create_project_app(linked, [credential])
    shared = tmp_path / 'shared'
    provision_project(shared, 'a')
    provision_project(shared, 'b')
    target = shared / 'projects' / 'b' / 'arena.sqlite'
    target.unlink()
    os.link(shared / 'projects' / 'a' / 'arena.sqlite', target)
    with pytest.raises(ValueError):
        create_project_app(shared, [credential])
