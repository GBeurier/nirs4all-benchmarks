"""Project-token local Arena service; operator-provisioned projects only."""
from __future__ import annotations

import asyncio
import json
import os
import re
import sqlite3
import stat
import tempfile
import threading
import time
from contextlib import contextmanager
from pathlib import Path
from typing import Any
from urllib.parse import parse_qsl

from nirs4all_benchmarks.store import ArenaStore, ArenaStoreRefusal, Queries

_HASH = re.compile(r"[0-9a-f]{64}")
_COLLECTION = re.compile(r"[a-z0-9][a-z0-9_-]{0,63}")
_READ_PATHS = frozenset('/api/' + p for p in (
    'healthz', 'unknown-context', 'overview', 'collections', 'datasets', 'pipelines', 'operators',
    'parameters', 'leaderboard', 'matrix', 'runs', 'operator-effect', 'parameter-effect', 'robustness',
    'facets', 'facet-values', 'pivot', 'parallel', 'planned', 'graph', 'composition', 'stats', 'compare'))
_RUN = re.compile(r'/api/run/([0-9a-f]{64})(?:/(residuals|metadata))?')
PROJECT_ROUTE_TABLE = frozenset({('GET', path) for path in _READ_PATHS} | {
    ('GET', '/api/run/{execution_hash}'), ('GET', '/api/run/{execution_hash}/residuals'),
    ('GET', '/api/run/{execution_hash}/metadata'), ('POST', '/api/ingest')})
_SELECTORS = frozenset(('token', 'access_token', 'api_key', 'project', 'project_id'))
_MAX_BODY = 8 * 1024 * 1024


async def _drain_cancelled(task: asyncio.Task[Any]) -> None:
    """Keep the slot until the worker finishes, even after repeated cancellation."""
    while not task.done():
        try:
            await asyncio.shield(task)
        except asyncio.CancelledError:
            continue
        except Exception:
            break
    if not task.cancelled():
        try:
            task.result()
        except Exception:
            return


def _principal(project_id: str) -> Any:
    from nirs4all_access import ProjectPrincipal
    return ProjectPrincipal(project_id, 'provisioner', frozenset({'arena'}), frozenset({'arena.read'}))


def provision_project(root: str | Path, project_id: str) -> None:
    """Provision a project store and publish its marker last."""
    _principal(project_id)
    root = Path(root).absolute()
    root.mkdir(mode=0o700, parents=True, exist_ok=True)
    if root.is_symlink() or not root.is_dir():
        raise ValueError('unsafe project root')
    projects = root / 'projects'
    projects.mkdir(mode=0o700, exist_ok=True)
    if projects.is_symlink() or not projects.is_dir():
        raise ValueError('unsafe projects directory')
    project = projects / project_id
    project.mkdir(mode=0o700, exist_ok=True)
    if project.is_symlink() or not project.is_dir():
        raise ValueError('unsafe project directory')
    marker = project / 'project.json'
    if marker.exists() or marker.is_symlink():
        raise ValueError('project already provisioned')
    with ArenaStore(project):
        pass
    data = json.dumps({'project_id': project_id}, sort_keys=True, separators=(',', ':')).encode() + b'\n'
    fd, temp = tempfile.mkstemp(prefix='.project-', dir=project)
    try:
        with os.fdopen(fd, 'wb') as f:
            os.fchmod(f.fileno(), 0o600)
            f.write(data)
            f.flush()
            os.fsync(f.fileno())
        os.link(temp, marker)
    finally:
        os.unlink(temp)


def _directory(path: Path) -> os.stat_result:
    info = path.lstat()
    if not stat.S_ISDIR(info.st_mode):
        raise ValueError('unsafe project component')
    return info


def _snapshot(root: Path) -> dict[str, Path]:
    _directory(root)
    projects = root / 'projects'
    _directory(projects)
    result: dict[str, Path] = {}
    seen_roots: set[tuple[int, int]] = set()
    seen_databases: set[tuple[int, int]] = set()
    for path in sorted(projects.iterdir()):
        _principal(path.name)
        info = _directory(path)
        key = (info.st_dev, info.st_ino)
        if key in seen_roots:
            raise ValueError('duplicate project root')
        seen_roots.add(key)
        marker = path / 'project.json'
        mi = marker.lstat()
        if not stat.S_ISREG(mi.st_mode) or json.loads(marker.read_text()) != {'project_id': path.name}:
            raise ValueError('invalid project marker')
        for name in ('arrays', 'exports'):
            _directory(path / name)
        db = (path / 'arena.sqlite').lstat()
        key = (db.st_dev, db.st_ino)
        if not stat.S_ISREG(db.st_mode) or db.st_nlink != 1 or key in seen_databases:
            raise ValueError('unsafe project database')
        seen_databases.add(key)
        with ArenaStore(path, create=False, busy_timeout_ms=1000):
            pass
        result[path.name] = path
    return result


def _limits(value: Any, path: tuple[str, ...] = (), depth: int = 0) -> None:
    if depth > 64:
        raise ValueError('depth')
    if isinstance(value, str) and len(value) > 65536:
        raise ValueError('string')
    if isinstance(value, dict):
        for k, v in value.items():
            _limits(k, path, depth + 1)
            _limits(v, (*path, k), depth + 1)
    elif isinstance(value, list):
        if len(value) > 4096 and path not in {('residuals', 'inline'), ('scores', 'observations')}:
            raise ValueError('array')
        for item in value:
            _limits(item, (*path, '[]'), depth + 1)


def _metadata(root: Path, execution_hash: str) -> dict[str, Any] | None:
    from nirs4all_benchmarks.identity import fingerprint
    from nirs4all_benchmarks.ingestion.metadata_bundle import read_stored_metadata
    from nirs4all_benchmarks.ingestion.submitted_metadata import binding_consistent
    with ArenaStore(root, create=False, busy_timeout_ms=1000) as store:
        execution = store.get('executions', 'execution_hash', execution_hash)
        if execution is None:
            return None
        export_hash = execution['arena_export_hash']
        row = store.get('submitted_metadata', 'arena_export_hash', export_hash)
        receipt = store.get('metadata_receipts', 'arena_export_hash', export_hash)
        if not row or not receipt or not binding_consistent(execution, row, receipt):
            raise RuntimeError('stored binding')
        receipt_json = json.loads(receipt['receipt_json'])
        if (fingerprint(receipt_json) != receipt['receipt_sha256']
                or receipt_json.get('source_export_hash') != export_hash):
            raise RuntimeError('stored receipt')
        receipt_json = read_stored_metadata(None, export_hash, stored_json=receipt['receipt_json'])
        if receipt_json.get('status') == 'metadata_unavailable':
            raise RuntimeError('stored receipt')
        if row['dataset_card_hash']:
            card = store.get('dataset_cards', 'dataset_card_hash', row['dataset_card_hash'])
            if not card or fingerprint(json.loads(card['card_json'])) != row['dataset_card_hash']:
                raise RuntimeError('stored card')
        fragments = json.loads(row['source_metadata_json'])
        context = fragments.pop('arena_context', None)
        return {'metadata_response_version': 1, 'arena_export_schema_version': 2,
                'execution_hash': execution_hash, 'arena_export_hash': export_hash,
                'terminal_status': receipt_json.get('metadata_validation', {}).get('terminal_status', 'unreported'),
                'receipt': receipt_json,
                'submitted_metadata': {'metadata': fragments, 'context': context,
                    'representation_kind': row['representation_kind'], 'representation_scope': 'first_accepted',
                    'verification': 'source_declared_unverified', 'dataset_card_hash': row['dataset_card_hash'],
                    'source_metadata_sha256': row['source_metadata_sha256'],
                    'normalized_provenance_sha256': row['normalized_provenance_sha256'],
                    'normalized_dataset_card_sha256': row['normalized_dataset_card_sha256']}}


def _ingest(root: Path, body: bytes, collection: str) -> tuple[int, dict[str, Any]]:
    from nirs4all_benchmarks.contract.strict_json import strict_loads
    from nirs4all_benchmarks.ingestion import IngestionPolicy, ingest_export, validate_metadata
    try:
        payload = strict_loads(body.decode('utf-8'))
        _limits(payload)
    except (ValueError, UnicodeError, RecursionError):
        return 422, {'code': 'invalid_export', 'message': 'Export was rejected'}
    validation = validate_metadata(payload)
    if validation.status != 'metadata_valid':
        code = validation.issues[0]['code'] if validation.issues else 'invalid_export'
        return 422, {'code': code, 'message': 'Export was rejected'}
    assert validation.model is not None
    if validation.model.dataset.visibility == 'public':
        return 422, {'code': 'public_visibility_unsupported', 'message': 'Export was rejected'}
    policy = IngestionPolicy(collection_id=collection, collection_kind='user_run_collection',
                             quarantine_on_leakage=False, store_export_bundle=True,
                             recompute_scores=False, bind_card_citation=False)
    with ArenaStore(root, create=False, busy_timeout_ms=1000) as store:
        result = ingest_export(store, payload, policy=policy, _validated=validation)
    if not result.ok:
        code = result.issues[0]['code'] if result.issues else 'invalid_export'
        if code in {'stored_batch_inconsistent', 'metadata_not_indexable'}:
            return 500, {'code': 'internal_error'}
        status = 409 if code in {'collection_conflict', 'execution_identity_conflict',
                                 'workspace_identity_collision', 'identity_dimension_conflict',
                                 'metric_unit_conflict'} else 422
        return status, {'code': code, 'message': 'Export was rejected'}
    return 200, {key: getattr(result, key) for key in ('status', 'validity_status', 'metadata_status',
        'scientific_verification', 'terminal_status', 'run_condition_hash', 'execution_hash', 'arena_export_hash')} | {
        'issues': [{'level': i['level'], 'code': i['code']} for i in result.issues]}


def create_project_app(root: str | Path, records: Any) -> Any:
    """Build a closed, authenticated ASGI application from startup snapshots."""
    from fastapi import FastAPI
    from fastapi.responses import JSONResponse
    from nirs4all_access import AccessError, ProjectTokenVerifier, parse_bearer

    from nirs4all_benchmarks.service.routes import register_routes

    records = tuple(records)
    if not any(not r.revoked and (r.expires_at is None or r.expires_at > time.time())
               and 'arena' in r.principal.services for r in records):
        raise ValueError('no active arena credentials')
    verifier = ProjectTokenVerifier(records)
    roots = _snapshot(Path(root).absolute())
    apps: dict[str, Any] = {}
    slots: dict[str, int] = dict.fromkeys(roots, 0)
    writers = {key: threading.Lock() for key in roots}
    for project_id, project_root in roots.items():
        @contextmanager
        def queries(path: Path = project_root):
            with ArenaStore(path, create=False, busy_timeout_ms=1000) as store:
                yield Queries(store)
        app = FastAPI(docs_url=None, redoc_url=None, openapi_url=None, redirect_slashes=False)
        app.router.redirect_slashes = False
        @app.exception_handler(ValueError)
        async def invalid_query(_request: Any, _exc: ValueError) -> Any:
            return JSONResponse(status_code=400, content={'code': 'invalid_query'})
        register_routes(app, queries, project=True)
        # These routes are handled by the authenticated front door; registration
        # keeps the project application's route inventory explicit and auditable.
        async def front_door_only() -> None:
            raise RuntimeError('front door required')
        app.add_api_route('/api/healthz', front_door_only, methods=['GET'])
        app.add_api_route('/api/run/{execution_hash}/metadata', front_door_only, methods=['GET'])
        app.add_api_route('/api/ingest', front_door_only, methods=['POST'])
        apps[project_id] = app

    async def service(scope: dict[str, Any], receive: Any, send: Any) -> None:
        if scope['type'] != 'http':
            return
        async def reply(status: int, payload: Any, headers: list[tuple[bytes, bytes]] | None = None) -> None:
            body = json.dumps(payload, separators=(',', ':'), ensure_ascii=False).encode()
            await send({'type': 'http.response.start', 'status': status, 'headers': [
                (b'content-type', b'application/json'), (b'content-length', str(len(body)).encode()),
                (b'cache-control', b'no-store'), (b'x-content-type-options', b'nosniff'), *(headers or [])]})
            await send({'type': 'http.response.body', 'body': body})
        raw_headers = scope.get('headers', [])
        auth = [v for k, v in raw_headers if k.lower() == b'authorization']
        try:
            try:
                authorization = auth[0].decode('ascii') if len(auth) == 1 else None
            except UnicodeDecodeError:
                authorization = None
            token = parse_bearer(authorization)
            verifier.verify(token, service='arena', required_rights=())
        except AccessError as exc:
            await reply(exc.status, {'code': exc.code, 'detail': exc.detail}, [(b'www-authenticate', b'Bearer')])
            return
        path, method = scope['path'], scope['method']
        match = _RUN.fullmatch(path)
        if method == 'GET' and (path in _READ_PATHS or match):
            right = 'arena.read'
        elif method == 'POST' and path == '/api/ingest':
            right = 'arena.add'
        else:
            await reply(404, {'code': 'not_found'})
            return
        try:
            principal = verifier.verify(token, service='arena', required_rights=(right,))
        except AccessError as exc:
            await reply(exc.status, {'code': exc.code, 'detail': exc.detail})
            return
        try:
            query = parse_qsl(scope.get('query_string', b'').decode('ascii'), keep_blank_values=True)
        except (ValueError, UnicodeDecodeError):
            await reply(400, {'code': 'invalid_query'})
            return
        if any(k.lower() in _SELECTORS for k, _ in query) or any(
            re.fullmatch(rb'x-.*-project.*', k.lower()) for k, _ in raw_headers
        ):
            await reply(400, {'code': 'selector_refused'})
            return
        project_id = principal.project_id
        if project_id not in roots:
            await reply(404, {'code': 'not_found'})
            return
        if path == '/api/compare':
            values = dict(query)
            if any(not _HASH.fullmatch(values.get(k, '')) for k in ('a', 'b')):
                await reply(404, {'code': 'not_found'})
                return
        if slots[project_id] >= 8:
            await reply(503, {'code': 'project_busy'}, [(b'retry-after', b'1')])
            return
        slots[project_id] += 1
        acquired = False
        try:
            if method == 'POST':
                params = dict(query)
                if 'as_release' in params:
                    await reply(400, {'code': 'release_not_supported'})
                    return
                collection = params.get('collection', 'uploads')
                if not _COLLECTION.fullmatch(collection):
                    await reply(400, {'code': 'invalid_query'})
                    return
                content_types = [v for k, v in raw_headers if k.lower() == b'content-type']
                if (len(content_types) != 1 or content_types[0].split(b';', 1)[0].strip().lower() != b'application/json'
                        or any(k.lower() == b'content-encoding' for k, _ in raw_headers)):
                    await reply(415, {'code': 'unsupported_media_type'})
                    return
                lengths = [v for k, v in raw_headers if k.lower() == b'content-length']
                if len(lengths) > 1 or (lengths and (not lengths[0].isdigit())):
                    await reply(400, {'code': 'invalid_request'})
                    return
                if lengths and int(lengths[0]) > _MAX_BODY:
                    await reply(413, {'code': 'body_too_large'})
                    return
                acquired = writers[project_id].acquire(blocking=False)
                if not acquired:
                    await reply(503, {'code': 'project_busy'}, [(b'retry-after', b'1')])
                    return
                async def read_body() -> bytes:
                    chunks = bytearray()
                    while True:
                        message = await receive()
                        if message['type'] != 'http.request':
                            raise ValueError('disconnected')
                        chunks.extend(message.get('body', b''))
                        if len(chunks) > _MAX_BODY:
                            raise OverflowError
                        if not message.get('more_body', False):
                            break
                    return bytes(chunks)
                try:
                    body = await asyncio.wait_for(read_body(), timeout=30)
                except TimeoutError:
                    await reply(408, {'code': 'body_timeout'})
                    return
                except OverflowError:
                    await reply(413, {'code': 'body_too_large'})
                    return
                except ValueError:
                    await reply(400, {'code': 'invalid_request'})
                    return
                if lengths and len(body) != int(lengths[0]):
                    await reply(400, {'code': 'invalid_request'})
                    return
                task = asyncio.create_task(asyncio.to_thread(_ingest, roots[project_id], body, collection))
                try:
                    status, result = await asyncio.shield(task)
                except asyncio.CancelledError:
                    await _drain_cancelled(task)
                    raise
                await reply(status, result)
                return
            if path == '/api/healthz':
                await reply(200, {'status': 'ok'})
                return
            if match and match.group(2) == 'metadata':
                metadata_task = asyncio.create_task(asyncio.to_thread(_metadata, roots[project_id], match.group(1)))
                try:
                    metadata_result = await asyncio.shield(metadata_task)
                except asyncio.CancelledError:
                    await _drain_cancelled(metadata_task)
                    raise
                await reply(200 if metadata_result is not None else 404,
                            metadata_result if metadata_result is not None else {'code': 'not_found'})
                return
            response: dict[str, Any] = {'body': bytearray()}
            async def inner_send(message: dict[str, Any]) -> None:
                if message['type'] == 'http.response.start':
                    response['status'] = message['status']
                    response['headers'] = message.get('headers', [])
                elif message['type'] == 'http.response.body':
                    response['body'].extend(message.get('body', b''))
            await apps[project_id](scope, receive, inner_send)
            status = response['status']
            if status in (404, 422):
                await reply(404 if status == 404 else 400,
                            {'code': 'not_found' if status == 404 else 'invalid_query'})
            elif status >= 500:
                await reply(500, {'code': 'internal_error'})
            else:
                headers = [(k, v) for k, v in response['headers'] if k.lower() not in (
                    b'server', b'etag', b'cache-control', b'x-content-type-options')]
                headers += [(b'cache-control', b'no-store'), (b'x-content-type-options', b'nosniff')]
                await send({'type': 'http.response.start', 'status': status, 'headers': headers})
                await send({'type': 'http.response.body', 'body': bytes(response['body'])})
        except ArenaStoreRefusal as exc:
            if exc.code == 'arena_store_busy':
                await reply(503, {'code': 'store_busy'}, [(b'retry-after', b'1')])
            else:
                await reply(500, {'code': 'internal_error'})
        except sqlite3.OperationalError as exc:
            if getattr(exc, 'sqlite_errorcode', 0) & 0xff in (5, 6):
                await reply(503, {'code': 'store_busy'}, [(b'retry-after', b'1')])
            else:
                await reply(500, {'code': 'internal_error'})
        except Exception:
            await reply(500, {'code': 'internal_error'})
        finally:
            if acquired:
                writers[project_id].release()
            slots[project_id] -= 1
    service.project_apps = apps  # type: ignore[attr-defined]
    return service
