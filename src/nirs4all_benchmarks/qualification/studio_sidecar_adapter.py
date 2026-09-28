"""Execute PERF-001 through Studio's native Archive V2 product route."""

from __future__ import annotations

import hashlib
import http.client
import json
import math
import os
import platform
import selectors
import site
import stat
import subprocess
import sys
import tarfile
import tempfile
import threading
import time
from dataclasses import dataclass
from pathlib import Path, PurePosixPath
from typing import Any, Literal

from pydantic import ValidationError

from nirs4all_benchmarks.qualification.contract import AdapterRequest, ComponentOutput

STUDIO_COMMIT = "6b0545771b7bb14e127ca4842d1a56e9b007ee42"
CORE_COMMIT = "bc001e53f41cee6eb5e6f1bf6b363db25cf750ee"
IO_COMMIT = "f41967d53b355b10951b5658af1dddb6bd926e3d"
STUDIO_SOURCE_SHA256 = "a49af896955b70de0ae25cdbe8d347dfd11de71a067981e70b52b16a045413f3"
SIDECAR_BINARY_SHA256 = "40b4aaac3f597ffba0680cdf0caf133250a65d0159fc5e6c4f191720c31e4ac1"
PACKAGED_CONTRACT_SHA256 = "23b6b7830aae3b0e485a5e723168abb71aa7fdb480b062a07c926e70d1829c7c"
METHODS_LIBRARY_SHA256 = "847f3b23986d9f74d392169bf559c49a38ca915492286e61ada10c31f72bfdf0"
METHODS_VERSION = "1.0.10+abi.2.2.0"
ARCHIVE_SHA256 = "994252030ff80129d0431995bae53eb473082f05825b65714379262b72af13fa"
ARCHIVE_ID = "archive:cb6a51b825bf5de858d8b190591f77e806d866b23dd6b27ee93b2aabbdee5363"
SCENARIO_SHA256 = "a25ae04d4a60198f1bd3f8423533991c3b09943e2cd3f33d3d6441f1b84c1f51"
WORKSPACE_ID = "workspace-a"
ARCHIVE_REF = "models/perf001-multitarget.n4a"
EXPECTED_TARGETS = ["protein", "moisture"]
EXPECTED_X = [[1.5, 0.5], [3.5, 1.5]]
EXPECTED_VALUES = [[1.6363636363636365, 13.272727272727273], [2.4999999999999996, 15.0]]
EXPECTED_PROVENANCE = {
    "executor": f"nirs4all-core@0.3.24+libn4m-abi-2.2:{METHODS_LIBRARY_SHA256}",
    "archive_ref": ARCHIVE_REF,
    "workspace_id": WORKSPACE_ID,
}
_ROLES = {
    "runtime_python",
    "adapter_runtime_closure",
    "runtime_sidecar",
    "studio_runtime_closure",
    "studio_source_snapshot",
    "studio_packaged_runtime_contract",
    "methods_library",
    "archive_v2",
}
MAX_JSON = 16 * 1024 * 1024


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _regular(path: Path, maximum: int, label: str) -> int:
    try:
        metadata = path.lstat()
    except FileNotFoundError as exc:
        raise ValueError(f"{label} must be a non-symlink regular file") from exc
    if path.is_symlink() or not stat.S_ISREG(metadata.st_mode) or not 0 < metadata.st_size <= maximum:
        raise ValueError(f"{label} must be a bounded non-symlink regular file")
    return metadata.st_size


def _installed_file(path: Path, maximum: int, label: str) -> int:
    try:
        metadata = path.lstat()
    except FileNotFoundError as exc:
        raise ValueError(f"{label} must be a non-symlink regular file") from exc
    if path.is_symlink() or not stat.S_ISREG(metadata.st_mode) or metadata.st_size > maximum:
        raise ValueError(f"{label} must be a bounded non-symlink regular file")
    return metadata.st_size


def _json(path: Path, maximum: int = MAX_JSON) -> Any:
    _regular(path, maximum, str(path))
    return json.loads(path.read_bytes())


def _below(path: Path, root: Path) -> bool:
    try:
        path.resolve().relative_to(root.resolve())
        return True
    except ValueError:
        return False


def _scenario(request: AdapterRequest) -> dict[str, Any]:
    path = request.scenario.definition.path
    _regular(path, 2 * 1024 * 1024, "scenario definition")
    if request.scenario.definition.sha256 != SCENARIO_SHA256 or _sha256(path) != SCENARIO_SHA256:
        raise ValueError("scenario SHA-256 changed after qualification preflight")
    value = _json(path)
    required = {"schema_version", "scenario_id", "operation", "fallback_allowed", "prediction", "legacy_oracle"}
    if not isinstance(value, dict) or set(value) != required:
        raise ValueError("scenario fields differ from PERF-001")
    if (
        value["schema_version"] != "nirs4all.perf-scenario.v1"
        or value["scenario_id"] != request.scenario.scenario_id
        or value["operation"] != "archive_v2_predict"
        or value["fallback_allowed"] is not False
    ):
        raise ValueError("Studio requires the finalized no-fallback Archive V2 scenario")
    if value.get("legacy_oracle", {}).get("training", {}).get("target_names") != EXPECTED_TARGETS:
        raise ValueError("Studio requires the protein/moisture multi-target identity")
    prediction = value["prediction"]
    if (
        not isinstance(prediction, dict)
        or set(prediction) != {"sample_ids", "x"}
        or prediction["sample_ids"] != ["predict.0", "predict.1"]
        or prediction["x"] != EXPECTED_X
        or not isinstance(prediction["x"], list)
        or len(prediction["x"]) != 2
    ):
        raise ValueError("prediction witness identity changed")
    width = len(prediction["x"][0]) if isinstance(prediction["x"][0], list) else 0
    for row in prediction["x"]:
        if not isinstance(row, list) or len(row) != width or width == 0:
            raise ValueError("prediction X must be rectangular")
        if any(isinstance(item, bool) or not isinstance(item, (int, float)) or not math.isfinite(item) for item in row):
            raise ValueError("prediction X must be finite")
    return value


def _paths(request: AdapterRequest) -> dict[str, Path]:
    artifacts = {artifact.role: artifact for artifact in request.artifacts}
    if set(artifacts) != _ROLES:
        raise ValueError(f"studio_sidecar requires exactly artifact roles {sorted(_ROLES)}")
    limits = {
        "runtime_python": 16 * 1024 * 1024,
        "adapter_runtime_closure": MAX_JSON,
        "runtime_sidecar": 32 * 1024 * 1024,
        "studio_runtime_closure": MAX_JSON,
        "studio_source_snapshot": 64 * 1024 * 1024,
        "studio_packaged_runtime_contract": 64 * 1024,
        "methods_library": 64 * 1024 * 1024,
        "archive_v2": 64 * 1024 * 1024,
    }
    result = {}
    for role, artifact in artifacts.items():
        _regular(artifact.path, limits[role], f"artifact {role!r}")
        if _sha256(artifact.path) != artifact.sha256:
            raise ValueError(f"artifact {role!r} SHA-256 changed")
        result[role] = artifact.path
    if any(not os.access(result[role], os.X_OK) for role in ("runtime_python", "runtime_sidecar")):
        raise ValueError("declared runtime is not executable")
    return result


def _adapter_closure(path: Path, paths: dict[str, Path]) -> Path:
    value = _json(path)
    required = {"schema_version", "stage_root", "prefix", "runtime_python_sha256", "distributions"}
    if (
        not isinstance(value, dict)
        or set(value) != required
        or value["schema_version"] != "nirs4all.studio-gate-adapter-runtime.v1"
    ):
        raise ValueError("adapter runtime closure fields differ")
    stage = Path(value["stage_root"])
    prefix = Path(value["prefix"])
    runtime = paths["runtime_python"].resolve()
    if (
        not stage.is_absolute()
        or not _below(prefix, stage)
        or any(not _below(item, stage) for item in paths.values())
        or runtime != Path(sys.executable).resolve()
        or not _below(runtime, prefix)
        or _sha256(runtime) != value["runtime_python_sha256"]
        or sys.flags.isolated != 1
        or site.ENABLE_USER_SITE is not False
    ):
        raise ValueError("adapter runtime is not isolated inside its declared stage")
    names: set[str] = set()
    total = 0
    count = 0
    for distribution in value["distributions"]:
        if not isinstance(distribution, dict) or set(distribution) != {"name", "version", "files"}:
            raise ValueError("adapter distribution inventory is malformed")
        name = distribution["name"]
        if not isinstance(name, str) or not name or name in names:
            raise ValueError("adapter distribution identity is invalid")
        names.add(name)
        for item in distribution["files"]:
            if not isinstance(item, dict) or set(item) != {"path", "sha256"}:
                raise ValueError("adapter installed-file entry is malformed")
            relative = PurePosixPath(item["path"])
            if relative.is_absolute() or ".." in relative.parts:
                raise ValueError("adapter installed-file path is unsafe")
            installed = prefix.joinpath(*relative.parts)
            total += _installed_file(installed, 256 * 1024 * 1024, f"installed file for {name}")
            count += 1
            if total > 2 * 1024 * 1024 * 1024 or count > 20_000 or _sha256(installed) != item["sha256"]:
                raise ValueError("adapter installed-file inventory differs")
    if "nirs4all" in names or not {"nirs4all-benchmarks", "pydantic"}.issubset(names):
        raise ValueError("adapter closure must contain the harness but no scientific nirs4all runtime")
    return stage


def _tree_digest(entries: list[tuple[str, str]]) -> str:
    digest = hashlib.sha256()
    for name, member_digest in sorted(entries):
        digest.update(f"{name}\0{member_digest}\n".encode())
    return digest.hexdigest()


def _studio_closure(path: Path, paths: dict[str, Path], request: AdapterRequest) -> None:
    value = _json(path)
    required = {
        "schema_version",
        "platform",
        "arch",
        "studio_commit_sha",
        "studio_version",
        "core_commit_sha",
        "io_commit_sha",
        "sidecar_binary_sha256",
        "source_snapshot_sha256",
        "packaged_runtime_contract_sha256",
        "methods_library_sha256",
        "methods_library_size",
        "methods_version",
        "methods_needed",
        "methods_required_glibc",
        "methods_required_glibcxx",
        "portability_claim",
        "archive_v2_sha256",
        "source_members",
        "vendored_core_member_count",
        "vendored_core_tree_sha256",
    }
    if (
        not isinstance(value, dict)
        or set(value) != required
        or value["schema_version"] != "nirs4all.studio-gate-runtime.v2"
    ):
        raise ValueError("Studio runtime closure fields differ")
    expected = {
        "platform": "linux",
        "arch": "x86_64",
        "studio_commit_sha": STUDIO_COMMIT,
        "core_commit_sha": CORE_COMMIT,
        "io_commit_sha": IO_COMMIT,
        "sidecar_binary_sha256": SIDECAR_BINARY_SHA256,
        "source_snapshot_sha256": STUDIO_SOURCE_SHA256,
        "packaged_runtime_contract_sha256": PACKAGED_CONTRACT_SHA256,
        "methods_library_sha256": METHODS_LIBRARY_SHA256,
        "methods_version": METHODS_VERSION,
        "methods_needed": ["libc.so.6", "libgcc_s.so.1", "libm.so.6", "libstdc++.so.6"],
        "methods_required_glibc": "GLIBC_2.35",
        "methods_required_glibcxx": "GLIBCXX_3.4.29",
        "portability_claim": "local-linux-x86_64-only-not-manylinux",
        "archive_v2_sha256": ARCHIVE_SHA256,
    }
    if (
        sys.platform != "linux"
        or platform.machine() not in {"x86_64", "AMD64"}
        or any(value[k] != v for k, v in expected.items())
    ):
        raise ValueError("Studio runtime closure differs from selected identities")
    if value["studio_commit_sha"] != request.declared_commit_sha or value["studio_version"] != request.declared_version:
        raise ValueError("declared Studio identity differs from closure")
    direct = {
        "runtime_sidecar": "sidecar_binary_sha256",
        "studio_source_snapshot": "source_snapshot_sha256",
        "studio_packaged_runtime_contract": "packaged_runtime_contract_sha256",
        "methods_library": "methods_library_sha256",
        "archive_v2": "archive_v2_sha256",
    }
    if any(_sha256(paths[role]) != value[field] for role, field in direct.items()):
        raise ValueError("Studio product artifact differs from closure")
    if _regular(paths["methods_library"], 64 * 1024 * 1024, "Methods library") != value["methods_library_size"]:
        raise ValueError("Methods library size differs")
    contract = _json(paths["studio_packaged_runtime_contract"], 64 * 1024)
    methods = contract.get("methods_library", {})
    if (
        contract.get("schema") != "nirs4all.studio-packaged-runtime.v1"
        or contract.get("product_backend") != "rust-sidecar"
        or methods.get("mode") != "bundled-required"
        or methods.get("member")
        != {"path": "native/libn4m.so", "sha256": METHODS_LIBRARY_SHA256, "size": value["methods_library_size"]}
        or methods.get("abi") != {"major": 2, "minor": 2}
    ):
        raise ValueError("packaged runtime contract does not bind Methods ABI 2.2")
    source_members = value["source_members"]
    if not isinstance(source_members, list) or not source_members or len(source_members) > 512:
        raise ValueError("source member inventory is invalid")
    core_entries = []
    with tarfile.open(paths["studio_source_snapshot"], "r:") as archive:
        members = archive.getmembers()
        if archive.pax_headers.get("comment") != STUDIO_COMMIT or len(members) > 4096:
            raise ValueError("source archive identity or bounds changed")
        if sum(member.size for member in members) > 64 * 1024 * 1024:
            raise ValueError("source archive exceeds content bound")
        for member in members:
            if not (member.isfile() or member.isdir()) or member.size > 8 * 1024 * 1024:
                raise ValueError("source archive contains unsafe member")
        for item in source_members:
            member = archive.getmember(item["path"])
            handle = archive.extractfile(member)
            if handle is None or hashlib.sha256(handle.read()).hexdigest() != item["sha256"]:
                raise ValueError(f"source member changed: {item['path']}")
        for member in members:
            if member.isfile() and member.name.startswith("sidecar/vendor/nirs4all-core-bc001e5/"):
                handle = archive.extractfile(member)
                if handle is None:
                    raise ValueError("vendored Core member unreadable")
                core_entries.append((member.name, hashlib.sha256(handle.read()).hexdigest()))
    if (
        len(core_entries) != value["vendored_core_member_count"]
        or _tree_digest(core_entries) != value["vendored_core_tree_sha256"]
    ):
        raise ValueError("vendored Core tree differs from closure")


def _prepare(root: Path, archive: Path) -> tuple[Path, str]:
    config = root / "config"
    workspace = root / "workspace"
    ref = ARCHIVE_REF
    destination = workspace / "exports" / ref
    config.mkdir()
    destination.parent.mkdir(parents=True)
    destination.write_bytes(archive.read_bytes())
    if _sha256(destination) != ARCHIVE_SHA256:
        raise ValueError("workspace archive changed during copy")
    config.joinpath("app_settings.json").write_text(
        json.dumps(
            {
                "linked_workspaces": [
                    {
                        "id": WORKSPACE_ID,
                        "path": str(workspace),
                        "name": "PERF-001",
                        "is_active": True,
                        "linked_at": "2026-09-02T12:00:00Z",
                        "last_scanned": None,
                        "discovered": {"runs_count": 0},
                    }
                ]
            }
        ),
        encoding="utf-8",
    )
    return config, ref


def _http(port: int, method: str, path: str, value: Any | None = None) -> tuple[int, dict[str, Any]]:
    body = None if value is None else json.dumps(value, separators=(",", ":")).encode()
    connection = http.client.HTTPConnection("127.0.0.1", port, timeout=15)
    try:
        connection.request(
            method, path, body=body, headers={} if body is None else {"Content-Type": "application/json"}
        )
        response = connection.getresponse()
        payload = response.read(2 * 1024 * 1024 + 1)
        if len(payload) > 2 * 1024 * 1024:
            raise ValueError("Studio response exceeded bound")
        decoded = json.loads(payload)
        if not isinstance(decoded, dict):
            raise ValueError("Studio response is not an object")
        return response.status, decoded
    finally:
        connection.close()


def _ready(process: subprocess.Popen[str]) -> int:
    if process.stdout is None:
        raise ValueError("sidecar stdout missing")
    selector = selectors.DefaultSelector()
    selector.register(process.stdout, selectors.EVENT_READ)
    if not selector.select(timeout=15):
        raise ValueError("sidecar did not publish readiness within 15 seconds")
    line = process.stdout.readline()
    if not line:
        stderr = ""
        if process.stderr is not None:
            stderr = process.stderr.read(4096).strip()
        raise ValueError(f"sidecar exited before readiness: status={process.poll()}, stderr={stderr!r}")
    prefix = "STUDIO_SIDECAR_READY "
    if not line.startswith(prefix):
        raise ValueError(f"sidecar readiness prefix changed: {line.strip()!r}")
    value = json.loads(line.removeprefix(prefix))
    if not isinstance(value, dict) or not isinstance(value.get("port"), int):
        raise ValueError("sidecar readiness malformed")
    return value["port"]


def _descendants(process_id: int) -> set[int]:
    result: set[int] = set()
    pending = [process_id]
    while pending:
        parent = pending.pop()
        try:
            task_root = Path(f"/proc/{parent}/task")
            children = [
                child
                for task in task_root.iterdir()
                for child in (task / "children").read_text(encoding="ascii").split()
            ]
        except FileNotFoundError as error:
            if Path(f"/proc/{parent}").exists():
                raise ValueError(f"cannot inspect live sidecar process {parent}") from error
            children = []
        for value in children:
            child = int(value)
            if child not in result:
                result.add(child)
                pending.append(child)
    return result


class _Isolation:
    def __init__(self, process_id: int, scratch: Path, baseline: set[str]) -> None:
        self.process_id, self.scratch, self.baseline = process_id, scratch, baseline
        self.children: set[int] = set()
        self.additions: set[str] = set()
        self.stop = threading.Event()
        self.error: Exception | None = None
        self.thread = threading.Thread(target=self._run, daemon=True)

    def _sample(self) -> None:
        self.children.update(_descendants(self.process_id))
        self.additions.update({item.name for item in self.scratch.iterdir()} - self.baseline)

    def _run(self) -> None:
        while not self.stop.wait(0.001):
            try:
                self._sample()
            except Exception as error:
                self.error = error
                return

    def __enter__(self) -> _Isolation:
        self._sample()
        self.thread.start()
        return self

    def __exit__(self, *_args: object) -> None:
        self.stop.set()
        self.thread.join(timeout=1)
        self._sample()
        if self.error is not None:
            raise ValueError(f"route isolation sampling failed: {self.error}") from self.error
        if self.children or self.additions:
            raise ValueError(f"route spawned children or scratch: {sorted(self.children)}, {sorted(self.additions)}")


@dataclass(frozen=True)
class _Evidence:
    response: dict[str, Any]
    elapsed_ms: float


def _execute(paths: dict[str, Path], scenario: dict[str, Any]) -> _Evidence:
    with tempfile.TemporaryDirectory(prefix="n4a-studio-gate-") as root_text:
        root = Path(root_text)
        scratch = root / "tmp"
        scratch.mkdir()
        config, ref = _prepare(root, paths["archive_v2"])
        environment = {
            "LANG": os.environ.get("LANG", "C.UTF-8"),
            "HOME": str(root),
            "TMPDIR": str(scratch),
            "TMP": str(scratch),
            "TEMP": str(scratch),
            "NIRS4ALL_CONFIG": str(config),
            "NIRS4ALL_RUNTIME_MODE": "release",
            "NIRS4ALL_RUNTIME_KIND": "rust_sidecar",
        }
        process = subprocess.Popen(
            [str(paths["runtime_sidecar"]), "--host", "127.0.0.1", "--port", "0"],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            cwd=root,
            env=environment,
        )
        try:
            port = _ready(process)
            code, capabilities = _http(port, "GET", "/sidecar/v1/capabilities")
            features = capabilities.get("features", {})
            if (
                code != 200
                or capabilities.get("python_plugin_host") != "unconfigured"
                or features.get("native_archive_v2_prediction") is not True
                or features.get("implicit_python_http_fallback") is not False
                or features.get("python_plugin_execution") is not False
            ):
                raise ValueError("native Archive V2 capability policy changed")
            if _descendants(process.pid):
                raise ValueError("sidecar has a child before the route")
            baseline = {item.name for item in scratch.iterdir()}
            if len(baseline) != 1 or not next(iter(baseline)).startswith("nirs4all-core-libn4m-"):
                raise ValueError("startup did not retain exactly one attested Core snapshot")
            payload = {
                "schema_version": 1,
                "operation": "archive_v2_predict",
                "workspace_id": WORKSPACE_ID,
                "archive": {"ref": ref, "sha256": ARCHIVE_SHA256},
                "input": {
                    "kind": "array",
                    "sample_ids": scenario["prediction"]["sample_ids"],
                    "x": scenario["prediction"]["x"],
                    "expected_target_names": EXPECTED_TARGETS,
                },
                "execution": {"engine": "core_rust_methods", "allow_fallback": False},
            }
            started = time.perf_counter()
            with _Isolation(process.pid, scratch, baseline):
                code, response = _http(port, "POST", "/api/predict/archive-v2", payload)
            elapsed = (time.perf_counter() - started) * 1000
            if code != 200:
                raise ValueError(f"native Archive V2 returned HTTP {code}: {response}")
            if _descendants(process.pid) or {item.name for item in scratch.iterdir()} != baseline:
                raise ValueError("route left a child or changed scratch baseline")
            return _Evidence(response, elapsed)
        finally:
            process.terminate()
            try:
                process.wait(timeout=5)
            except subprocess.TimeoutExpired:
                process.kill()
                process.wait(timeout=5)


def _response(value: dict[str, Any], scenario: dict[str, Any], request: AdapterRequest) -> None:
    required = {
        "schema_version",
        "operation",
        "archive_id",
        "archive_sha256",
        "engine",
        "fallback_used",
        "sample_ids",
        "target_names",
        "values",
        "provenance",
    }
    if set(value) != required:
        raise ValueError("response fields differ")
    if (
        value["schema_version"] != 1
        or value["operation"] != "archive_v2_predict"
        or value["archive_id"] != ARCHIVE_ID
        or value["archive_sha256"] != ARCHIVE_SHA256
        or value["engine"] != "core_rust_methods"
        or value["fallback_used"] is not False
        or value["sample_ids"] != scenario["prediction"]["sample_ids"]
        or value["target_names"] != EXPECTED_TARGETS
        or value["provenance"] != EXPECTED_PROVENANCE
    ):
        raise ValueError("response identity, provenance, engine, fallback, sample, or target order changed")
    if not isinstance(value["values"], list) or len(value["values"]) != 2:
        raise ValueError("response matrix shape changed")
    for observed_row, expected_row in zip(value["values"], EXPECTED_VALUES, strict=True):
        if not isinstance(observed_row, list) or len(observed_row) != 2:
            raise ValueError("response matrix shape changed")
        for observed, expected in zip(observed_row, expected_row, strict=True):
            if (
                isinstance(observed, bool)
                or not isinstance(observed, (int, float))
                or not math.isfinite(observed)
                or float(observed) != expected
            ):
                raise ValueError(f"prediction {observed!r} differs from Core witness {expected!r}")


def adapt(request: AdapterRequest) -> ComponentOutput:
    """Execute and validate the selected Studio product route."""
    component: Literal["studio_sidecar"] = "studio_sidecar"
    if request.component != component:
        raise ValueError(f"studio_sidecar refuses component {request.component!r}")
    started = time.perf_counter()
    scenario = _scenario(request)
    paths = _paths(request)
    _adapter_closure(paths["adapter_runtime_closure"], paths)
    _studio_closure(paths["studio_runtime_closure"], paths, request)
    evidence = _execute(paths, scenario)
    _response(evidence.response, scenario, request)
    if {role: path.resolve() for role, path in _paths(request).items()} != {
        role: path.resolve() for role, path in paths.items()
    }:
        raise ValueError("Studio artifacts changed during execution")
    values = evidence.response["values"]
    return ComponentOutput(
        component=component,
        scenario_id=request.scenario.scenario_id,
        version=request.declared_version,
        commit_sha=request.declared_commit_sha,
        completed=True,
        fallback_used=False,
        observations={
            "prediction.protein": [float(row[0]) for row in values],
            "prediction.moisture": [float(row[1]) for row in values],
        },
        metrics={
            "runtime.product_route_calls": 1.0,
            "runtime.native_archive_v2_prediction_calls": 1.0,
            "runtime.python_children": 0.0,
            "runtime.route_scratch_entries": 0.0,
            "runtime.core_attested_snapshot_entries": 1.0,
            "runtime.comparable_prediction_values": 4.0,
        },
        timings_ms={
            "runtime.product_execution": evidence.elapsed_ms,
            "adapter.total": (time.perf_counter() - started) * 1000,
        },
    )


def studio_sidecar_main() -> None:
    """Run the selected Studio sidecar adapter."""
    try:
        output = adapt(AdapterRequest.model_validate_json(sys.stdin.read()))
    except (OSError, subprocess.SubprocessError, ValueError, ValidationError) as exc:
        print(str(exc), file=sys.stderr)
        raise SystemExit(2) from exc
    sys.stdout.write(output.model_dump_json())


if __name__ == "__main__":
    studio_sidecar_main()
