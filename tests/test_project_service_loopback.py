"""Real loopback qualification; execute only in the separately admitted lane."""

import http.client
import json
import os
import socket
import sqlite3
import subprocess
import sys
import time
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

from nirs4all_access import ProjectPrincipal, issue_credential, write_credential_store

from nirs4all_benchmarks.service.project import provision_project
from test_arena_v2 import record


def test_owned_loopback_server_authentication_and_quiet_logs(tmp_path):
    assert os.environ.get("ARENA_PROJECT_LOOPBACK") == "1"
    assert "ARENA_TEST_SCRATCH" not in os.environ
    root = tmp_path / "arena"
    provision_project(root, "a")
    token, record = issue_credential(ProjectPrincipal("a", "loopback", frozenset({"arena"}),
                                                      frozenset({"arena.read", "arena.add"})))
    credentials = tmp_path / "credentials.json"
    write_credential_store(credentials, [record])
    with socket.socket() as probe:
        probe.bind(("127.0.0.1", 0))
        port = probe.getsockname()[1]
    env = os.environ.copy()
    env.update(WEB_CONCURRENCY="4", FORWARDED_ALLOW_IPS="*", PYTHONASYNCIODEBUG="1")
    command = [sys.executable, "-B", "-m", "nirs4all_benchmarks.cli", "serve-project", "--root", str(root),
               "--credentials", str(credentials), "--host", "127.0.0.1", "--port", str(port)]
    process = subprocess.Popen(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE, env=env)
    try:
        deadline = time.monotonic() + 10
        while True:
            try:
                connection = http.client.HTTPConnection("127.0.0.1", port, timeout=1)
                connection.request("GET", "/api/healthz?opaque=loopback-canary",
                                   headers={"Authorization": "Bearer " + token})
                response = connection.getresponse()
                body = response.read()
                break
            except OSError:
                if process.poll() is not None or time.monotonic() >= deadline:
                    raise
                time.sleep(0.05)
        assert response.status == 200 and body == b'{"status":"ok"}'
        assert response.getheader("Server") is None
        connection.close()
        connection = http.client.HTTPConnection("127.0.0.1", port, timeout=2)
        connection.request("POST", "/api/ingest", body=b"", headers={
            "Authorization": "Bearer " + token, "Content-Type": "application/json", "Content-Length": "8388609"})
        response = connection.getresponse()
        response.read()
        assert response.status == 413
        connection.close()
        connection = http.client.HTTPConnection("127.0.0.1", port, timeout=15)
        connection.request("POST", "/api/ingest", body=iter((b'x' * (4 * 1024 * 1024),
                                                               b'y' * (4 * 1024 * 1024 + 1))),
                           headers={"Authorization": "Bearer " + token, "Content-Type": "application/json",
                                    "Transfer-Encoding": "chunked"}, encode_chunked=True)
        response = connection.getresponse()
        response.read()
        assert response.status == 413
        connection.close()
        with socket.create_connection(("127.0.0.1", port), timeout=35) as client:
            stalled = ("POST /api/ingest HTTP/1.1\r\nHost: 127.0.0.1\r\n"
                       f"Authorization: Bearer {token}\r\nContent-Type: application/json\r\n"
                       "Content-Length: 2\r\n\r\n{")
            client.sendall(stalled.encode('ascii'))
            assert client.recv(4096).startswith(b"HTTP/1.1 408")
        with socket.create_connection(("127.0.0.1", port), timeout=2) as client:
            upgrade = ("GET /api/healthz HTTP/1.1\r\nHost: 127.0.0.1\r\nConnection: Upgrade\r\n"
                       "Upgrade: websocket\r\nSec-WebSocket-Version: 13\r\n"
                       "Sec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r\n"
                       f"Authorization: Bearer {token}\r\n\r\n")
            client.sendall(upgrade.encode('ascii'))
            assert not client.recv(4096).startswith(b"HTTP/1.1 101")
        children = (Path('/proc') / str(process.pid) / 'task' / str(process.pid) / 'children').read_text().strip()
        assert not children
    finally:
        process.terminate()
        stdout, stderr = process.communicate(timeout=15)
    assert token.encode() not in stdout + stderr
    assert b"loopback-canary" not in stdout + stderr


def test_external_project_write_lock_records_cross_project_latency(tmp_path, record_property):
    assert os.environ.get('ARENA_PROJECT_LOOPBACK') == '1'
    assert 'ARENA_TEST_SCRATCH' not in os.environ
    root = tmp_path / 'arena'
    records, tokens = [], []
    for name in ('a', 'b'):
        provision_project(root, name)
        token, credential = issue_credential(ProjectPrincipal(name, 'loopback', frozenset({'arena'}),
            frozenset({'arena.read', 'arena.add'})))
        records.append(credential)
        tokens.append(token)
    credentials = tmp_path / 'credentials.json'
    write_credential_store(credentials, records)
    with socket.socket() as probe:
        probe.bind(('127.0.0.1', 0))
        port = probe.getsockname()[1]
    command = [sys.executable, '-B', '-m', 'nirs4all_benchmarks.cli', 'serve-project', '--root', str(root),
               '--credentials', str(credentials), '--host', '127.0.0.1', '--port', str(port)]
    process = subprocess.Popen(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    def request(method, path, token, body=None):
        started = time.monotonic()
        connection = http.client.HTTPConnection('127.0.0.1', port, timeout=15)
        headers = {'Authorization': 'Bearer ' + token}
        if body is not None:
            headers['Content-Type'] = 'application/json'
        try:
            connection.request(method, path, body=body, headers=headers)
            response = connection.getresponse()
            response.read()
            return response.status, time.monotonic() - started
        finally:
            connection.close()
    db = None
    try:
        deadline = time.monotonic() + 10
        while True:
            try:
                assert request('GET', '/api/healthz', tokens[1])[0] == 200
                break
            except OSError:
                if process.poll() is not None or time.monotonic() >= deadline:
                    raise
                time.sleep(0.05)
        db = sqlite3.connect(root / 'projects' / 'a' / 'arena.sqlite')
        db.execute('BEGIN IMMEDIATE')
        payload = record()
        payload['dataset'].update(visibility='private', visibility_source='local_policy')
        with ThreadPoolExecutor(max_workers=2) as pool:
            writer = pool.submit(request, 'POST', '/api/ingest', tokens[0], json.dumps(payload).encode())
            reader = pool.submit(request, 'GET', '/api/overview', tokens[1])
            writer_status, writer_latency = writer.result(timeout=20)
            reader_status, reader_latency = reader.result(timeout=20)
        assert writer_status == 503 and reader_status == 200
        record_property('writer_seconds', writer_latency)
        record_property('reader_seconds', reader_latency)
    finally:
        if db is not None:
            db.rollback()
            db.close()
        process.terminate()
        stdout, stderr = process.communicate(timeout=15)
    assert all(token.encode() not in stdout + stderr for token in tokens)


def test_sigkill_before_commit_restarts_and_replays(tmp_path):
    assert os.environ.get('ARENA_PROJECT_LOOPBACK') == '1'
    assert 'ARENA_TEST_SCRATCH' not in os.environ
    root = tmp_path / 'arena'
    provision_project(root, 'a')
    token, credential = issue_credential(ProjectPrincipal('a', 'loopback', frozenset({'arena'}),
        frozenset({'arena.read', 'arena.add'})))
    credentials = tmp_path / 'credentials.json'
    write_credential_store(credentials, [credential])
    marker = tmp_path / 'before-commit'
    bootstrap = tmp_path / 'controlled_ingest.py'
    bootstrap.write_text(
        'import os,time\n'
        'from pathlib import Path\n'
        'from nirs4all_benchmarks.store.arena_store import ArenaStore\n'
        '_insert=ArenaStore.insert\n'
        'def controlled(self,table,row):\n'
        '    result=_insert(self,table,row)\n'
        '    if table=="submitted_metadata":\n'
        '        Path(os.environ["ARENA_TEST_MARKER"]).write_text("ready")\n'
        '        time.sleep(60)\n'
        '    return result\n'
        'ArenaStore.insert=controlled\n'
        'from nirs4all_benchmarks.cli import main\n'
        'main()\n', encoding='utf-8')
    with socket.socket() as probe:
        probe.bind(('127.0.0.1', 0))
        port = probe.getsockname()[1]
    options = ['serve-project', '--root', str(root), '--credentials', str(credentials),
               '--host', '127.0.0.1', '--port', str(port)]
    env = os.environ.copy()
    env['ARENA_TEST_MARKER'] = str(marker)
    process = subprocess.Popen([sys.executable, '-B', str(bootstrap), *options],
                               stdout=subprocess.PIPE, stderr=subprocess.PIPE, env=env)
    payload = record()
    payload['dataset'].update(visibility='private', visibility_source='local_policy')
    def post():
        connection = http.client.HTTPConnection('127.0.0.1', port, timeout=15)
        try:
            connection.request('POST', '/api/ingest', json.dumps(payload).encode(),
                               {'Authorization': 'Bearer ' + token, 'Content-Type': 'application/json'})
            response = connection.getresponse()
            return response.status, json.loads(response.read())
        finally:
            connection.close()
    def ready():
        deadline = time.monotonic() + 10
        while True:
            try:
                connection = http.client.HTTPConnection('127.0.0.1', port, timeout=1)
                connection.request('GET', '/api/healthz', headers={'Authorization': 'Bearer ' + token})
                response = connection.getresponse()
                response.read()
                connection.close()
                assert response.status == 200
                return
            except OSError:
                if process.poll() is not None or time.monotonic() >= deadline:
                    raise
                time.sleep(0.05)
    all_logs = b''
    try:
        ready()
        with ThreadPoolExecutor(max_workers=1) as pool:
            pending = pool.submit(post)
            deadline = time.monotonic() + 10
            while not marker.exists():
                if process.poll() is not None or time.monotonic() >= deadline:
                    raise AssertionError('ingestion did not reach controlled write')
                time.sleep(0.01)
            process.kill()
            stdout, stderr = process.communicate(timeout=10)
            all_logs += stdout + stderr
            try:
                pending.result(timeout=10)
            except (OSError, http.client.HTTPException):
                pass
        process = subprocess.Popen([sys.executable, '-B', '-m', 'nirs4all_benchmarks.cli', *options],
                                   stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        ready()
        assert post()[1]['status'] == 'committed'
        assert post()[1]['status'] == 'already_ingested'
    finally:
        if process.poll() is None:
            process.terminate()
        stdout, stderr = process.communicate(timeout=15)
        all_logs += stdout + stderr
    assert token.encode() not in all_logs and str(root).encode() not in all_logs


def test_real_server_inner_exception_is_fixed_and_quiet(tmp_path):
    assert os.environ.get('ARENA_PROJECT_LOOPBACK') == '1'
    assert 'ARENA_TEST_SCRATCH' not in os.environ
    root = tmp_path / 'arena'
    provision_project(root, 'a')
    token, credential = issue_credential(ProjectPrincipal('a', 'loopback', frozenset({'arena'}),
                                                        frozenset({'arena.read'})))
    credentials = tmp_path / 'credentials.json'
    write_credential_store(credentials, [credential])
    bootstrap = tmp_path / 'controlled_error.py'
    bootstrap.write_text(
        'import nirs4all_benchmarks.service.project as project\n'
        'def fail(root,execution_hash):\n'
        '    raise RuntimeError("raw-error-canary " + str(root))\n'
        'project._metadata=fail\n'
        'from nirs4all_benchmarks.cli import main\n'
        'main()\n', encoding='utf-8')
    with socket.socket() as probe:
        probe.bind(('127.0.0.1', 0))
        port = probe.getsockname()[1]
    command = [sys.executable, '-B', str(bootstrap), 'serve-project', '--root', str(root),
               '--credentials', str(credentials), '--host', '127.0.0.1', '--port', str(port)]
    process = subprocess.Popen(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    try:
        deadline = time.monotonic() + 10
        while True:
            try:
                connection = http.client.HTTPConnection('127.0.0.1', port, timeout=2)
                connection.request('GET', '/api/run/' + 'a' * 64 + '/metadata?opaque=query-canary',
                                   headers={'Authorization': 'Bearer ' + token})
                response = connection.getresponse()
                body = response.read()
                break
            except OSError:
                if process.poll() is not None or time.monotonic() >= deadline:
                    raise
                time.sleep(0.05)
        assert response.status == 500 and json.loads(body) == {'code': 'internal_error'}
    finally:
        process.terminate()
        stdout, stderr = process.communicate(timeout=15)
    combined = body + stdout + stderr
    assert all(value not in combined for value in (token.encode(), str(root).encode(), b'query-canary',
                                                    b'raw-error-canary'))
