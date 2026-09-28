"""Strict Web/WASM launcher for the selected Archive V2 replay surface.

The Python adapter never executes scientific code.  It verifies an explicit
Web source snapshot, strict product profile, Archive V2 and a closed JavaScript
runtime manifest, recreates that closure in a private directory, then starts
only the declared Node executable.  The child calls the selected Web aggregate
once and reports the exact multi-target result plus Methods ABI call counts.
"""

from __future__ import annotations

import hashlib
import importlib
import json
import math
import os
import re
import shutil
import site
import subprocess
import sys
import tarfile
import tempfile
import time
from pathlib import Path, PurePosixPath
from typing import Any, Literal

from pydantic import ValidationError

from nirs4all_benchmarks.process_guard import run_bounded
from nirs4all_benchmarks.qualification.contract import AdapterRequest, ComponentOutput

_SCENARIO_SCHEMA = "nirs4all.perf-scenario.v1"
_CLOSURE_SCHEMA = "nirs4all.web-wasm-runtime-closure.v1"
_OPERATION = "archive_v2_predict"
_CHILD_PROTOCOL = "nirs4all.web-wasm-child.v1"
_MAX_CHILD_OUTPUT = 2 * 1024 * 1024
_MAX_CLOSURE_FILES = 256
_MAX_CLOSURE_BYTES = 64 * 1024 * 1024
_MAX_PYTHON_FILES = 10_000
_FIT_SYMBOL_PATTERN = r"(?:^|_)(?:re)?fit(?:_|$)"
_ROLES = {
    "runtime_python",
    "python_runtime_closure",
    "runtime_node",
    "web_runtime_closure",
    "web_source_snapshot",
    "strict_profile",
    "archive_v2",
}
_REQUIRED_CLOSURE_ROLES = {
    "aggregate_package",
    "aggregate_index",
    "aggregate_archive_v2",
    "core_provenance",
    "native_package",
    "native_loader",
    "core_native_wasm",
    "methods_index",
    "methods_wasm",
}
_STRICT_PROFILE = {
    "contract": "nirs4all.web-runtime-profile.v1",
    "profile": "strict-wasm",
    "nativeWasmRequired": True,
    "jsBackendFallback": "forbid",
    "providerMatrixFallback": "forbid",
    "schedulerFallback": "forbid",
    "remoteComputeProvider": "forbid",
}
_PYTHON_DISTRIBUTIONS = {
    "annotated-types",
    "nirs4all-benchmarks",
    "pydantic",
    "pydantic-core",
    "typing-extensions",
    "typing-inspection",
}


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _below(path: Path, root: Path) -> bool:
    try:
        path.resolve().relative_to(root.resolve())
        return True
    except ValueError:
        return False


def _strict_scenario(request: AdapterRequest) -> dict[str, Any]:
    path = request.scenario.definition.path
    if _sha256(path) != request.scenario.definition.sha256:
        raise ValueError("scenario SHA-256 changed after qualification preflight")
    value = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(value, dict) or set(value) != {
        "schema_version",
        "scenario_id",
        "operation",
        "fallback_allowed",
        "prediction",
        "legacy_oracle",
    }:
        raise ValueError("scenario must contain only the finalized PERF-001 fields")
    if value["schema_version"] != _SCENARIO_SCHEMA:
        raise ValueError(f"scenario schema_version must be {_SCENARIO_SCHEMA!r}")
    if value["scenario_id"] != request.scenario.scenario_id:
        raise ValueError("scenario_id does not match the adapter request")
    if value["operation"] != _OPERATION:
        raise ValueError(f"scenario operation must be {_OPERATION!r}")
    if value["fallback_allowed"] is not False:
        raise ValueError("Web/WASM qualification requires fallback_allowed=false")
    prediction = value["prediction"]
    if not isinstance(prediction, dict) or set(prediction) != {"sample_ids", "x"}:
        raise ValueError("scenario prediction must contain only sample_ids and x")
    sample_ids = prediction["sample_ids"]
    matrix = prediction["x"]
    if (
        not isinstance(sample_ids, list)
        or not sample_ids
        or any(not isinstance(item, str) or not item for item in sample_ids)
        or len(set(sample_ids)) != len(sample_ids)
    ):
        raise ValueError("prediction sample_ids must be non-empty, unique strings")
    if not isinstance(matrix, list) or len(matrix) != len(sample_ids) or not matrix:
        raise ValueError("prediction x rows must align exactly with sample_ids")
    width: int | None = None
    for row in matrix:
        if not isinstance(row, list) or not row:
            raise ValueError("prediction x must be a non-empty rectangular matrix")
        if width is None:
            width = len(row)
        if len(row) != width or any(
            isinstance(item, bool) or not isinstance(item, (int, float)) or not math.isfinite(item) for item in row
        ):
            raise ValueError("prediction x must be a finite rectangular numeric matrix")
    if not isinstance(value["legacy_oracle"], dict):
        raise ValueError("scenario legacy_oracle must be an object")
    return value


def _artifact_paths(request: AdapterRequest) -> dict[str, Path]:
    artifacts = {artifact.role: artifact for artifact in request.artifacts}
    if set(artifacts) != _ROLES:
        raise ValueError(f"web_wasm requires exactly artifact roles {sorted(_ROLES)}; received {sorted(artifacts)}")
    paths: dict[str, Path] = {}
    for role, artifact in artifacts.items():
        if not artifact.path.is_file():
            raise ValueError(f"artifact {role!r} is not a regular file")
        if _sha256(artifact.path) != artifact.sha256:
            raise ValueError(f"artifact {role!r} SHA-256 changed after qualification preflight")
        paths[role] = artifact.path
    if not os.access(paths["runtime_node"], os.X_OK):
        raise ValueError("runtime_node artifact is not executable")
    return paths


def _python_runtime(path: Path, paths: dict[str, Path]) -> Path:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise ValueError(f"python_runtime_closure is not valid JSON: {exc}") from exc
    if not isinstance(value, dict) or set(value) != {
        "schema_version",
        "stage_root",
        "prefix",
        "runtime_python_sha256",
        "distributions",
    }:
        raise ValueError("python_runtime_closure contains unknown or missing fields")
    if value["schema_version"] != "nirs4all.web-wasm-python-runtime.v1":
        raise ValueError("python_runtime_closure has an unsupported schema")
    stage_root = Path(value["stage_root"])
    prefix = Path(value["prefix"])
    if not stage_root.is_absolute() or not prefix.is_absolute() or not _below(prefix, stage_root):
        raise ValueError("Python runtime prefix is not confined to the autonomous stage")
    for role, artifact_path in paths.items():
        if not _below(artifact_path, stage_root):
            raise ValueError(f"artifact {role!r} escapes the autonomous stage")
    runtime = paths["runtime_python"].resolve()
    if runtime != Path(sys.executable).resolve() or not _below(runtime, prefix):
        raise ValueError("launcher did not start the declared staged Python runtime")
    if _sha256(runtime) != value["runtime_python_sha256"]:
        raise ValueError("staged Python executable differs from its runtime closure")
    if sys.flags.isolated != 1 or site.ENABLE_USER_SITE is not False:
        raise ValueError("Web/WASM adapter must run with python -I and user-site disabled")
    user_site = Path(site.getusersitepackages()).resolve()
    if any(Path(item).resolve() == user_site for item in sys.path if item):
        raise ValueError("user-site leaked into the isolated adapter runtime")
    distributions = value["distributions"]
    if not isinstance(distributions, list) or len(distributions) != len(_PYTHON_DISTRIBUTIONS):
        raise ValueError("Python runtime distribution inventory is incomplete")
    names = {item.get("name") for item in distributions if isinstance(item, dict)}
    if names != _PYTHON_DISTRIBUTIONS:
        raise ValueError("Python runtime distribution set is not the bounded adapter closure")
    file_count = 0
    for distribution in distributions:
        if set(distribution) != {"name", "version", "wheel_path", "wheel_sha256", "files"}:
            raise ValueError("Python runtime distribution entry contains unknown or missing fields")
        wheel = Path(distribution["wheel_path"])
        if not _below(wheel, stage_root) or not wheel.is_file() or _sha256(wheel) != distribution["wheel_sha256"]:
            raise ValueError(f"Python wheel {distribution['name']!r} is missing or changed")
        files = distribution["files"]
        if not isinstance(files, list) or not files:
            raise ValueError(f"Python distribution {distribution['name']!r} has no installed file inventory")
        for item in files:
            if not isinstance(item, dict) or set(item) != {"path", "sha256"}:
                raise ValueError("Python installed-file entry contains unknown or missing fields")
            relative = PurePosixPath(item["path"])
            if relative.is_absolute() or ".." in relative.parts:
                raise ValueError("Python installed-file inventory contains an unsafe path")
            installed = prefix.joinpath(*relative.parts)
            if not installed.is_file() or _sha256(installed) != item["sha256"]:
                raise ValueError(f"installed Python bytes changed for {distribution['name']!r}")
            file_count += 1
            if file_count > _MAX_PYTHON_FILES:
                raise ValueError("Python runtime installed-file inventory exceeds its bound")
    for module_name in ("nirs4all_benchmarks", "pydantic", "typing_extensions"):
        module = importlib.import_module(module_name)
        location = getattr(module, "__file__", None)
        if location is None or not _below(Path(location), prefix):
            raise ValueError(f"imported {module_name} escapes the staged Python prefix")
    return stage_root


def _strict_profile(path: Path) -> None:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise ValueError(f"strict_profile is not valid JSON: {exc}") from exc
    if value != _STRICT_PROFILE:
        raise ValueError("strict_profile does not exactly forbid every Web fallback surface")


def _snapshot_member_bytes(source_snapshot: Path, member_name: str) -> bytes:
    try:
        with tarfile.open(source_snapshot, "r:") as snapshot:
            member = snapshot.getmember(member_name)
            handle = snapshot.extractfile(member) if member.isfile() else None
            if handle is None or member.size > _MAX_CLOSURE_BYTES:
                raise ValueError("snapshot member is not a bounded regular file")
            return handle.read()
    except (KeyError, OSError, tarfile.TarError) as exc:
        raise ValueError(f"source snapshot member is missing: {member_name}") from exc


def _closure_manifest(
    path: Path,
    request: AdapterRequest,
    source_snapshot: Path,
) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise ValueError(f"web_runtime_closure is not valid JSON: {exc}") from exc
    required = {
        "schema_version",
        "web_commit_sha",
        "web_version",
        "core_commit_sha",
        "aggregate_version",
        "source_snapshot_sha256",
        "entrypoint",
        "files",
    }
    if not isinstance(value, dict) or set(value) != required:
        raise ValueError("web_runtime_closure contains unknown or missing fields")
    if value["schema_version"] != _CLOSURE_SCHEMA:
        raise ValueError(f"web_runtime_closure schema_version must be {_CLOSURE_SCHEMA!r}")
    if value["web_commit_sha"] != request.declared_commit_sha:
        raise ValueError("declared Web commit differs from the runtime closure")
    if value["web_version"] != request.declared_version:
        raise ValueError("declared Web version differs from the runtime closure")
    if value["source_snapshot_sha256"] != _sha256(source_snapshot):
        raise ValueError("Web source snapshot identity differs from the runtime closure")
    for name in ("web_commit_sha", "core_commit_sha"):
        item = value[name]
        if not isinstance(item, str) or len(item) != 40 or any(char not in "0123456789abcdef" for char in item):
            raise ValueError(f"web_runtime_closure {name} is not a full lowercase Git SHA")
    if not isinstance(value["aggregate_version"], str) or not value["aggregate_version"]:
        raise ValueError("web_runtime_closure aggregate_version is missing")
    if value["entrypoint"] != "node_modules/nirs4all/src/index.js":
        raise ValueError("web_runtime_closure entrypoint is not the selected Web aggregate")
    files = value["files"]
    if not isinstance(files, list) or not files or len(files) > _MAX_CLOSURE_FILES:
        raise ValueError("web_runtime_closure file inventory is empty or exceeds its bound")
    destinations: set[str] = set()
    roles: list[str] = []
    total_bytes = 0
    checked: list[dict[str, Any]] = []
    for item in files:
        if not isinstance(item, dict) or set(item) != {
            "role",
            "path",
            "destination",
            "sha256",
            "snapshot_member",
        }:
            raise ValueError("web_runtime_closure file entry contains unknown or missing fields")
        role = item["role"]
        source = Path(item["path"])
        destination = PurePosixPath(item["destination"])
        digest = item["sha256"]
        snapshot_member = item["snapshot_member"]
        if not isinstance(role, str) or not role:
            raise ValueError("web_runtime_closure file role is invalid")
        if not source.is_absolute() or not source.is_file():
            raise ValueError(f"closure file {role!r} is not an absolute regular file")
        if destination.is_absolute() or not destination.parts or ".." in destination.parts:
            raise ValueError(f"closure file {role!r} has an unsafe destination")
        destination_text = destination.as_posix()
        if destination_text in destinations:
            raise ValueError(f"closure destination {destination_text!r} is duplicated")
        if not isinstance(digest, str) or len(digest) != 64 or _sha256(source) != digest:
            raise ValueError(f"closure file {role!r} SHA-256 changed before execution")
        snapshot_bytes: bytes | None = None
        if snapshot_member is not None:
            if not isinstance(snapshot_member, str) or not snapshot_member.startswith("web-app/"):
                raise ValueError(f"closure file {role!r} has an unsafe source snapshot member")
            snapshot_bytes = _snapshot_member_bytes(source_snapshot, snapshot_member)
            if hashlib.sha256(snapshot_bytes).hexdigest() != digest:
                raise ValueError(f"closure file {role!r} differs from the Web source snapshot")
        destinations.add(destination_text)
        roles.append(role)
        total_bytes += source.stat().st_size
        checked.append(
            {**item, "source": source, "destination_path": destination, "snapshot_bytes": snapshot_bytes}
        )
    if total_bytes > _MAX_CLOSURE_BYTES:
        raise ValueError("web_runtime_closure exceeds the 64 MiB extracted bound")
    missing = sorted(_REQUIRED_CLOSURE_ROLES - set(roles))
    if missing:
        raise ValueError(f"web_runtime_closure is missing required roles: {missing}")
    for unique_role in _REQUIRED_CLOSURE_ROLES:
        if roles.count(unique_role) != 1:
            raise ValueError(f"web_runtime_closure role {unique_role!r} must occur exactly once")
    by_role = {item["role"]: item for item in checked if item["role"] in _REQUIRED_CLOSURE_ROLES}
    provenance = by_role["core_provenance"]["source"].read_text(encoding="utf-8")
    match = re.search(r"nirs4all-core` commit\s+`([0-9a-f]{40})`", provenance)
    if match is None or match.group(1) != value["core_commit_sha"]:
        raise ValueError("Core provenance does not bind the declared Core commit")
    if by_role["core_native_wasm"]["sha256"] not in provenance:
        raise ValueError("Core provenance does not bind the declared native WASM")
    return value, checked


def _child_environment() -> dict[str, str]:
    allowed = ("LANG", "LC_ALL", "LC_CTYPE", "TZ", "SYSTEMROOT", "WINDIR")
    return {name: os.environ[name] for name in allowed if name in os.environ}


def _archive_matches_snapshot(source_snapshot: Path, archive_path: Path) -> None:
    member_name = "web-app/src/engine/fixtures/archive-v2/multitarget-pls.n4a"
    try:
        with tarfile.open(source_snapshot, "r:") as snapshot:
            member = snapshot.getmember(member_name)
            handle = snapshot.extractfile(member) if member.isfile() else None
            if handle is None:
                raise ValueError("Archive V2 snapshot member is not a regular file")
            snapshot_digest = hashlib.sha256(handle.read()).hexdigest()
    except (KeyError, OSError, tarfile.TarError) as exc:
        raise ValueError("Archive V2 is absent from the selected Web source snapshot") from exc
    if snapshot_digest != _sha256(archive_path):
        raise ValueError("staged Archive V2 differs from the selected Web source snapshot")


_CHILD_SCRIPT = r'''
import { readFile } from 'node:fs/promises';
import { pathToFileURL } from 'node:url';

const chunks = [];
for await (const chunk of process.stdin) chunks.push(chunk);
const payload = JSON.parse(Buffer.concat(chunks).toString('utf8'));
if (payload.protocol !== 'nirs4all.web-wasm-child.v1') throw new Error('invalid Web/WASM child protocol');
const aggregate = await import(pathToFileURL(payload.entrypoint).href);
if (typeof aggregate.replayMethodsArchiveV2 !== 'function') {
  throw new Error('selected Web aggregate does not expose Archive V2 replay');
}
const methods = await aggregate.loadMethodsWasm();
await methods.loadModule();
const module = methods.getModule();
const originalCcall = module.ccall.bind(module);
const calls = new Map();
module.ccall = (symbol, ...args) => {
  calls.set(symbol, (calls.get(symbol) ?? 0) + 1);
  return originalCcall(symbol, ...args);
};
try {
  const archive = new Uint8Array(await readFile(payload.archive));
  const prediction = payload.scenario.prediction;
  const rows = prediction.x.length;
  const cols = prediction.x[0].length;
  const started = performance.now();
  const result = await aggregate.replayMethodsArchiveV2(archive, {
    X: prediction.x,
    rows,
    cols,
    sampleIds: prediction.sample_ids,
  });
  const predictionMs = performance.now() - started;
  const required = {
    n4m_serialization_inspect: 1,
    n4m_model_import_from_buffer: 1,
    n4m_model_predict_alloc: 1,
    n4m_model_destroy: 1,
    n4m_array_free: 1,
    n4m_context_create: 1,
    n4m_context_destroy: 1,
  };
  for (const [symbol, count] of Object.entries(required)) {
    if ((calls.get(symbol) ?? 0) !== count) throw new Error(`${symbol} call count was not exactly ${count}`);
  }
  const fitPattern = new RegExp(payload.fitSymbolPattern);
  const forbiddenFitCalls = [...calls.entries()].filter(
    ([symbol, count]) => count > 0 && fitPattern.test(symbol),
  );
  const fitCalls = forbiddenFitCalls.reduce((total, [, count]) => total + count, 0);
  if (forbiddenFitCalls.length > 0) throw new Error(`fit/refit call observed: ${JSON.stringify(forbiddenFitCalls)}`);
  if (result.schema !== 'nirs4all.core.archive-v2-replay.v1'
      || result.engine !== 'nirs4all-methods-wasm'
      || result.fallback !== false
      || result.archiveSha256 !== payload.archiveSha256
      || JSON.stringify(result.sampleIds) !== JSON.stringify(prediction.sample_ids)
      || result.rows !== rows
      || result.cols !== result.targetNames.length
      || result.data.length !== result.rows * result.cols
      || result.data.some((value) => !Number.isFinite(value))) {
    throw new Error('selected Web aggregate returned inconsistent replay identity, shape, or fallback metadata');
  }
  process.stdout.write(JSON.stringify({
    protocol: payload.protocol,
    values: result.data,
    targetNames: result.targetNames,
    sampleIds: result.sampleIds,
    archiveSha256: result.archiveSha256,
    engine: result.engine,
    fallback: result.fallback,
    calls: Object.fromEntries(calls),
    fitCalls,
    predictionMs,
  }));
} finally {
  module.ccall = originalCcall;
}
'''


def adapt(request: AdapterRequest) -> ComponentOutput:
    """Execute the exact selected Web aggregate and finalize its observations."""
    component: Literal["web_wasm"] = "web_wasm"
    if request.component != component:
        raise ValueError(f"web_wasm launcher refuses component {request.component!r}")
    started = time.perf_counter()
    scenario = _strict_scenario(request)
    paths = _artifact_paths(request)
    _python_runtime(paths["python_runtime_closure"], paths)
    _strict_profile(paths["strict_profile"])
    closure, files = _closure_manifest(paths["web_runtime_closure"], request, paths["web_source_snapshot"])
    _archive_matches_snapshot(paths["web_source_snapshot"], paths["archive_v2"])
    archive_sha256 = _sha256(paths["archive_v2"])

    with tempfile.TemporaryDirectory(prefix="n4a-web-wasm-") as cwd_text:
        cwd = Path(cwd_text)
        for item in files:
            destination = cwd.joinpath(*item["destination_path"].parts)
            destination.parent.mkdir(parents=True, exist_ok=True)
            if item["snapshot_bytes"] is None:
                shutil.copyfile(item["source"], destination)
            else:
                destination.write_bytes(item["snapshot_bytes"])
            if _sha256(destination) != item["sha256"]:
                raise ValueError(f"closure file {item['role']!r} changed while materializing the runtime")
        entrypoint = cwd / closure["entrypoint"]
        if not entrypoint.is_file():
            raise ValueError("web_runtime_closure did not materialize its declared entrypoint")
        payload = {
            "protocol": _CHILD_PROTOCOL,
            "entrypoint": str(entrypoint),
            "archive": str(paths["archive_v2"]),
            "archiveSha256": archive_sha256,
            "fitSymbolPattern": _FIT_SYMBOL_PATTERN,
            "scenario": scenario,
        }
        process = run_bounded(
            [str(paths["runtime_node"]), "--input-type=module", "--eval", _CHILD_SCRIPT],
            input_text=json.dumps(payload, sort_keys=True, separators=(",", ":"), allow_nan=False),
            cwd=cwd,
            env=_child_environment(),
            timeout=300,
            max_output_bytes=_MAX_CHILD_OUTPUT,
        )
    if len(process.stdout.encode()) > _MAX_CHILD_OUTPUT or len(process.stderr.encode()) > _MAX_CHILD_OUTPUT:
        raise ValueError("Web/WASM runtime output exceeded the 2 MiB limit")
    if process.returncode != 0:
        lines = process.stderr.strip().splitlines()
        detail = " | ".join(lines[-8:]) if lines else "no diagnostic"
        raise ValueError(f"Web/WASM runtime failed with code {process.returncode}: {detail[:500]}")
    try:
        result = json.loads(process.stdout)
    except json.JSONDecodeError as exc:
        raise ValueError("Web/WASM runtime did not return valid JSON") from exc
    if not isinstance(result, dict) or result.get("protocol") != _CHILD_PROTOCOL:
        raise ValueError("Web/WASM runtime did not finalize the child protocol")
    values = result.get("values")
    targets = result.get("targetNames")
    calls = result.get("calls")
    if (
        result.get("archiveSha256") != archive_sha256
        or result.get("engine") != "nirs4all-methods-wasm"
        or result.get("fallback") is not False
        or result.get("sampleIds") != scenario["prediction"]["sample_ids"]
    ):
        raise ValueError("Web/WASM child output changed archive, engine, fallback, or sample identity")
    if (
        not isinstance(values, list)
        or not isinstance(targets, list)
        or not targets
        or any(not isinstance(target, str) or not target for target in targets)
        or len(set(targets)) != len(targets)
    ):
        raise ValueError("Web/WASM runtime omitted predictions or target identities")
    if not isinstance(calls, dict) or calls.get("n4m_model_predict_alloc") != 1:
        raise ValueError("Web/WASM runtime did not report exactly one Methods prediction")
    fit_calls = result.get("fitCalls")
    if isinstance(fit_calls, bool) or not isinstance(fit_calls, int) or fit_calls != 0:
        raise ValueError("Web/WASM runtime reported a fit/refit call")
    rows = len(scenario["prediction"]["sample_ids"])
    if len(values) != rows * len(targets):
        raise ValueError("Web/WASM runtime returned a prediction vector with the wrong shape")
    if any(
        isinstance(value, bool) or not isinstance(value, (int, float)) or not math.isfinite(value)
        for value in values
    ):
        raise ValueError("Web/WASM runtime returned non-finite predictions")
    observations = {
        f"prediction.{target}": [float(values[row * len(targets) + index]) for row in range(rows)]
        for index, target in enumerate(targets)
    }
    return ComponentOutput(
        component=component,
        scenario_id=request.scenario.scenario_id,
        version=request.declared_version,
        commit_sha=request.declared_commit_sha,
        completed=True,
        fallback_used=False,
        observations=observations,
        metrics={
            "prediction.samples": float(rows),
            "prediction.targets": float(len(targets)),
            "prediction.values": float(len(values)),
            "runtime.fit_calls": float(fit_calls),
            "runtime.predict_calls": float(calls["n4m_model_predict_alloc"]),
        },
        timings_ms={
            "runtime.predict": float(result["predictionMs"]),
            "adapter.total": (time.perf_counter() - started) * 1000.0,
        },
    )


def web_wasm_main() -> None:
    """Run the selected Web/WASM adapter."""
    try:
        request = AdapterRequest.model_validate_json(sys.stdin.read())
        output = adapt(request)
    except (OSError, subprocess.SubprocessError, ValueError, ValidationError) as exc:
        print(str(exc), file=sys.stderr)
        raise SystemExit(2) from exc
    sys.stdout.write(output.model_dump_json())


if __name__ == "__main__":
    web_wasm_main()
