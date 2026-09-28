"""Strict launcher for the direct Core/Rust PERF-001 lane.

The launcher process never imports a selected scientific runtime itself.  It
verifies explicit artifacts, then starts the content-addressed Python
executable in isolated mode.  The child verifies that the installed
distributions are byte-for-byte the declared wheels and live below its own
runtime prefix before performing one Archive V2 prediction.
"""

from __future__ import annotations

import base64
import csv
import hashlib
import io
import json
import math
import os
import stat
import subprocess
import sys
import time
from email.parser import Parser
from pathlib import Path, PurePosixPath, PureWindowsPath
from typing import Any, Literal
from zipfile import BadZipFile, ZipFile, ZipInfo

from pydantic import ValidationError

from nirs4all_benchmarks.process_guard import run_bounded
from nirs4all_benchmarks.qualification.contract import AdapterRequest, ComponentOutput

_SCENARIO_SCHEMA = "nirs4all.perf-scenario.v1"
_OPERATION = "archive_v2_predict"
_CHILD_PROTOCOL = "nirs4all.scientific-child.v1"
_MAX_CHILD_OUTPUT = 2 * 1024 * 1024
_MAX_SCENARIO_BYTES = 1024 * 1024

_CORE_ROLES = {
    "runtime_python",
    "core_wheel",
    "pyyaml_wheel",
    "archive_v2",
    "methods_library",
}


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _read_regular_bounded(path: Path, *, label: str, max_bytes: int) -> bytes:
    """Read one stable regular file identity without following a path swap."""
    try:
        before = path.lstat()
    except OSError as exc:
        raise ValueError(f"{label} is not a regular non-symlink file") from exc
    if not stat.S_ISREG(before.st_mode):
        raise ValueError(f"{label} is not a regular non-symlink file")
    if before.st_size > max_bytes:
        raise ValueError(f"{label} exceeds the {max_bytes}-byte limit")
    try:
        with path.open("rb") as handle:
            opened = os.fstat(handle.fileno())
            if not stat.S_ISREG(opened.st_mode) or (opened.st_dev, opened.st_ino) != (before.st_dev, before.st_ino):
                raise ValueError(f"{label} identity changed before read")
            payload = handle.read(max_bytes + 1)
            after_read = os.fstat(handle.fileno())
        after_path = path.lstat()
    except OSError as exc:
        raise ValueError(f"{label} could not be read safely") from exc
    if len(payload) > max_bytes:
        raise ValueError(f"{label} exceeds the {max_bytes}-byte limit")
    stable_fields = ("st_dev", "st_ino", "st_size", "st_mtime_ns", "st_ctime_ns")
    if any(getattr(opened, field) != getattr(after_read, field) for field in stable_fields):
        raise ValueError(f"{label} changed during read")
    if (after_path.st_dev, after_path.st_ino) != (opened.st_dev, opened.st_ino):
        raise ValueError(f"{label} path identity changed during read")
    if len(payload) != opened.st_size:
        raise ValueError(f"{label} length changed during read")
    return payload


def _strict_scenario(request: AdapterRequest) -> dict[str, Any]:
    path = request.scenario.definition.path
    payload = _read_regular_bounded(path, label="scenario definition", max_bytes=_MAX_SCENARIO_BYTES)
    if hashlib.sha256(payload).hexdigest() != request.scenario.definition.sha256:
        raise ValueError("scenario SHA-256 changed after qualification preflight")
    value = json.loads(payload.decode("utf-8"))
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
        raise ValueError("PERF-001 scenario must explicitly forbid fallback")
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


def _expected_target_names(scenario: dict[str, Any]) -> list[str]:
    oracle = scenario["legacy_oracle"]
    training = oracle.get("training")
    target_names = training.get("target_names") if isinstance(training, dict) else None
    if (
        not isinstance(target_names, list)
        or not target_names
        or any(not isinstance(item, str) or not item for item in target_names)
        or len(set(target_names)) != len(target_names)
    ):
        raise ValueError("scenario legacy_oracle training must declare unique target_names")
    return target_names


def _artifact_paths(request: AdapterRequest) -> dict[str, Path]:
    artifacts = {artifact.role: artifact for artifact in request.artifacts}
    if len(request.artifacts) != len(artifacts) or set(artifacts) != _CORE_ROLES:
        raise ValueError(
            f"core_rust requires exactly artifact roles {sorted(_CORE_ROLES)}; received {sorted(artifacts)}"
        )
    paths: dict[str, Path] = {}
    for role, artifact in artifacts.items():
        try:
            mode = artifact.path.lstat().st_mode
        except OSError as exc:
            raise ValueError(f"artifact {role!r} is not a regular non-symlink file") from exc
        if not stat.S_ISREG(mode):
            raise ValueError(f"artifact {role!r} is not a regular non-symlink file")
        if _sha256(artifact.path) != artifact.sha256:
            raise ValueError(f"artifact {role!r} SHA-256 changed after qualification preflight")
        paths[role] = artifact.path
    if not os.access(paths["runtime_python"], os.X_OK):
        raise ValueError("runtime_python artifact is not executable")
    return paths


def _runtime_layout(runtime_python: Path) -> tuple[Path, Path]:
    runtime_root = runtime_python.parent.parent
    if runtime_python.parent.name == "Scripts":
        candidates = [runtime_root / "Lib" / "site-packages"]
    elif runtime_python.parent.name == "bin":
        candidates = sorted(runtime_root.glob("lib/python*/site-packages"))
    else:
        candidates = []
    candidates = [candidate for candidate in candidates if candidate.is_dir()]
    if len(candidates) != 1:
        raise ValueError("runtime_python must bind exactly one isolated site-packages directory")
    site_packages = candidates[0]
    for label, path in (("runtime root", runtime_root), ("site-packages", site_packages)):
        try:
            mode = path.lstat().st_mode
        except OSError as exc:
            raise ValueError(f"{label} is unavailable") from exc
        if not stat.S_ISDIR(mode):
            raise ValueError(f"{label} must be a non-symlink directory")
    return runtime_root, site_packages


def _urlsafe_digest(value: str) -> str:
    try:
        return base64.urlsafe_b64decode(value + "=" * (-len(value) % 4)).hex()
    except ValueError as exc:
        raise ValueError("wheel RECORD contains an invalid SHA-256 digest") from exc


def _safe_wheel_member(member: str) -> PurePosixPath:
    pure = PurePosixPath(member)
    if (
        not member
        or "\x00" in member
        or "\\" in member
        or pure.is_absolute()
        or ".." in pure.parts
        or PureWindowsPath(member).drive
    ):
        raise ValueError(f"wheel contains unsafe member path {member!r}")
    return pure


def _wheel_contract(path: Path, expected_name: str) -> dict[str, Any]:
    """Extract the byte identities the isolated runtime must prove installed."""
    try:
        with ZipFile(path) as archive:
            infos = archive.infolist()
            names = [info.filename for info in infos]
            if len(names) != len(set(names)):
                raise ValueError("wheel ZIP contains duplicate member names")
            file_infos: dict[str, ZipInfo] = {}
            for info in infos:
                _safe_wheel_member(info.filename)
                if info.flag_bits & 0x1:
                    raise ValueError(f"wheel member {info.filename!r} is encrypted")
                mode = info.external_attr >> 16
                kind = stat.S_IFMT(mode)
                if info.is_dir():
                    if kind not in (0, stat.S_IFDIR):
                        raise ValueError(f"wheel member {info.filename!r} has an invalid directory type")
                else:
                    if kind not in (0, stat.S_IFREG):
                        raise ValueError(f"wheel member {info.filename!r} is not a regular file")
                    file_infos[info.filename] = info
            metadata_names = [name for name in file_infos if name.endswith(".dist-info/METADATA")]
            record_names = [name for name in file_infos if name.endswith(".dist-info/RECORD")]
            if len(metadata_names) != 1 or len(record_names) != 1:
                raise ValueError("wheel must contain exactly one METADATA and one RECORD")
            metadata = Parser().parsestr(archive.read(metadata_names[0]).decode("utf-8"))
            normalized = metadata["Name"].lower().replace("_", "-")
            if normalized != expected_name:
                raise ValueError(f"wheel distribution must be {expected_name!r}, got {metadata['Name']!r}")
            version = metadata["Version"]
            if not version:
                raise ValueError("wheel METADATA has no Version")
            files: dict[str, str] = {}
            recorded: set[str] = set()
            for row in csv.reader(io.StringIO(archive.read(record_names[0]).decode("utf-8"))):
                if len(row) != 3:
                    raise ValueError("wheel RECORD row must have exactly three columns")
                member, digest, size = row
                _safe_wheel_member(member)
                if member in recorded:
                    raise ValueError(f"wheel RECORD contains duplicate member {member!r}")
                recorded.add(member)
                if member == record_names[0]:
                    if digest or size:
                        raise ValueError("wheel RECORD self-entry must be unhashed and unsized")
                    continue
                member_info = file_infos.get(member)
                if member_info is None:
                    raise ValueError(f"wheel RECORD member {member!r} is absent from the ZIP")
                if not digest.startswith("sha256="):
                    raise ValueError(f"wheel member {member!r} is not SHA-256 addressed")
                if not size.isascii() or not size.isdecimal() or int(size) != member_info.file_size:
                    raise ValueError(f"wheel member {member!r} size differs from RECORD")
                expected_digest = _urlsafe_digest(digest.removeprefix("sha256="))
                if hashlib.sha256(archive.read(member_info)).hexdigest() != expected_digest:
                    raise ValueError(f"wheel member {member!r} bytes differ from RECORD")
                files[member] = expected_digest
            if set(file_infos) != recorded:
                unrecorded = sorted(set(file_infos) - recorded)
                raise ValueError(f"wheel ZIP inventory differs from RECORD; unrecorded={unrecorded}")
            files[record_names[0]] = hashlib.sha256(archive.read(record_names[0])).hexdigest()
            if not files:
                raise ValueError("wheel RECORD contains no hashed files")
    except (BadZipFile, KeyError, UnicodeDecodeError) as exc:
        raise ValueError(f"invalid wheel {path.name!r}: {exc}") from exc
    return {"name": expected_name, "version": version, "files": files}


def _child_environment() -> dict[str, str]:
    allowed = ("LANG", "LC_ALL", "LC_CTYPE", "TZ", "SYSTEMROOT", "WINDIR")
    environment = {name: os.environ[name] for name in allowed if name in os.environ}
    environment["PYTHONNOUSERSITE"] = "1"
    environment["PYTHONDONTWRITEBYTECODE"] = "1"
    return environment


_CHILD_SCRIPT = r"""
import hashlib, importlib, json, math, os, stat, sys, time
from pathlib import Path

def sha256(path):
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()

def below(path, root):
    try:
        path.relative_to(root)
        return True
    except ValueError:
        return False

def verify_runtime_inventory(payload):
    root = Path(payload["runtime_root"])
    site = Path(payload["site_packages"])
    if root.is_symlink() or site.is_symlink() or not root.is_dir() or not site.is_dir():
        raise RuntimeError("declared runtime layout is not a regular directory tree")
    root = root.resolve()
    site = site.resolve()
    if not below(site, root):
        raise RuntimeError("declared site-packages escapes the runtime root")
    expected = {}
    for contract in payload["wheels"]:
        for relative, digest in contract["files"].items():
            if relative in expected:
                raise RuntimeError("wheel contracts overlap at %s" % relative)
            expected[relative] = digest
    actual = set()
    for directory, names, files in os.walk(site, followlinks=False):
        directory_path = Path(directory)
        for name in names:
            path = directory_path / name
            if path.is_symlink() or not stat.S_ISDIR(path.lstat().st_mode):
                raise RuntimeError("installed runtime contains a non-directory or symlink directory")
        for name in files:
            path = directory_path / name
            if path.is_symlink() or not stat.S_ISREG(path.lstat().st_mode):
                raise RuntimeError("installed runtime contains a non-regular or symlink file")
            actual.add(path.relative_to(site).as_posix())
    if actual != set(expected):
        raise RuntimeError(
            "installed runtime inventory differs from declared wheels: missing=%s extra=%s"
            % (sorted(set(expected) - actual), sorted(actual - set(expected)))
        )
    for relative, digest in expected.items():
        installed = site / relative
        if sha256(installed) != digest:
            raise RuntimeError("installed runtime bytes differ from declared wheel member %s" % relative)
    return root, site

def import_below_site(name, site):
    module = importlib.import_module(name)
    location = getattr(module, "__file__", None)
    if location is None or not below(Path(location).resolve(), site):
        raise RuntimeError("imported %s escapes the explicit site-packages" % name)
    return module

def direct_core(payload, site):
    core = import_below_site("nirs4all_core", site)
    if "dag_ml" in sys.modules:
        raise RuntimeError("direct Core matrix lane imported forbidden Python dag_ml")
    prediction = payload["prediction"]
    outcome = core.predict_methods_archive_v2_matrix(
        payload["archive_v2"],
        prediction["sample_ids"],
        prediction["x"],
        payload["expected_target_names"],
        methods_library_path=payload["methods_library"],
        methods_library_sha256=payload["digests"]["methods_library"],
        request_id="request:nirs4all.perf001",
        outcome_id="outcome:nirs4all.perf001",
        run_id="run:nirs4all.perf001",
        diagnostics={"qualification": "PERF-001", "fallback_allowed": False},
    )
    if "dag_ml" in sys.modules:
        raise RuntimeError("direct Core matrix lane imported forbidden Python dag_ml")
    if not isinstance(outcome, dict) or outcome.get("phase") != "PREDICT":
        raise RuntimeError("Core matrix API returned an invalid prediction outcome")
    outputs = outcome.get("outputs")
    if not isinstance(outputs, list) or len(outputs) != 1:
        raise RuntimeError("Core matrix API must return exactly one output")
    predictions = outputs[0].get("predictions") if isinstance(outputs[0], dict) else None
    if not isinstance(predictions, list) or len(predictions) != 1:
        raise RuntimeError("Core matrix API must return exactly one prediction block")
    block = predictions[0]
    if not isinstance(block, dict) or block.get("sample_ids") != prediction["sample_ids"]:
        raise RuntimeError("Core prediction sample identities are not aligned")
    if block.get("target_names") != payload["expected_target_names"]:
        raise RuntimeError("Core prediction target identities are not aligned")
    return block.get("values"), block.get("target_names")

payload = json.load(sys.stdin)
if payload.get("protocol") != "nirs4all.scientific-child.v1":
    raise RuntimeError("invalid scientific child protocol")
if not sys.flags.isolated or not sys.flags.no_site or not sys.flags.dont_write_bytecode:
    raise RuntimeError("scientific child requires -I -S -B")
for role in ("archive_v2", "methods_library"):
    if sha256(Path(payload[role])) != payload["digests"][role]:
        raise RuntimeError("%s digest changed before scientific execution" % role)
_runtime_root, site_packages = verify_runtime_inventory(payload)
if str(site_packages) in sys.path:
    raise RuntimeError("site-packages was active before closed inventory verification")
sys.path.insert(0, str(site_packages))
versions = {contract["name"]: contract["version"] for contract in payload["wheels"]}
started = time.perf_counter()
if payload["mode"] != "core_rust":
    raise RuntimeError("unsupported scientific mode")
values, target_names = direct_core(payload, site_packages)
elapsed_ms = (time.perf_counter() - started) * 1000.0
if not isinstance(target_names, list) or not target_names or len(set(target_names)) != len(target_names):
    raise RuntimeError("runtime returned ambiguous target names")
if not isinstance(values, list) or len(values) != len(payload["prediction"]["sample_ids"]):
    raise RuntimeError("runtime returned a prediction matrix with the wrong height")
if any(not isinstance(row, list) or len(row) != len(target_names) for row in values):
    raise RuntimeError("runtime returned a prediction matrix with the wrong width")
if any(
    isinstance(value, bool) or not isinstance(value, (int, float)) or not math.isfinite(value)
    for row in values for value in row
):
    raise RuntimeError("runtime returned non-finite predictions")
json.dump({
    "protocol": "nirs4all.scientific-child.v1",
    "values": values,
    "target_names": target_names,
    "versions": versions,
    "prediction_ms": elapsed_ms,
}, sys.stdout, sort_keys=True, allow_nan=False)
"""


def adapt(request: AdapterRequest) -> ComponentOutput:
    """Execute the strict direct Core lane and finalize the adapter protocol."""
    component: Literal["core_rust"] = "core_rust"
    if request.component != component:
        raise ValueError(
            "core_rust launcher refuses component "
            f"{request.component!r}; python_oracle is reserved for the selected legacy engine"
        )
    started = time.perf_counter()
    paths = _artifact_paths(request)
    scenario = _strict_scenario(request)
    expected_target_names = _expected_target_names(scenario)
    runtime_root, site_packages = _runtime_layout(paths["runtime_python"])
    wheels = [
        _wheel_contract(paths["core_wheel"], "nirs4all-core"),
        _wheel_contract(paths["pyyaml_wheel"], "pyyaml"),
    ]
    declared_runtime_version = wheels[0]["version"]
    if request.declared_version != declared_runtime_version:
        raise ValueError(
            f"declared component version {request.declared_version!r} differs from wheel {declared_runtime_version!r}"
        )
    payload = {
        "protocol": _CHILD_PROTOCOL,
        "mode": component,
        "prediction": scenario["prediction"],
        "expected_target_names": expected_target_names,
        "runtime_root": str(runtime_root),
        "site_packages": str(site_packages),
        "archive_v2": str(paths["archive_v2"]),
        "methods_library": str(paths["methods_library"]),
        "digests": {
            "archive_v2": _sha256(paths["archive_v2"]),
            "methods_library": _sha256(paths["methods_library"]),
        },
        "wheels": wheels,
    }
    process = run_bounded(
        [str(paths["runtime_python"]), "-I", "-S", "-B", "-c", _CHILD_SCRIPT],
        input_text=json.dumps(payload, sort_keys=True, separators=(",", ":"), allow_nan=False),
        env=_child_environment(),
        timeout=300,
        max_output_bytes=_MAX_CHILD_OUTPUT,
    )
    if len(process.stdout.encode()) > _MAX_CHILD_OUTPUT or len(process.stderr.encode()) > _MAX_CHILD_OUTPUT:
        raise ValueError("scientific runtime output exceeded the 2 MiB limit")
    if process.returncode != 0:
        detail = process.stderr.strip().splitlines()[-1] if process.stderr.strip() else "no diagnostic"
        raise ValueError(f"scientific runtime failed with code {process.returncode}: {detail[:500]}")
    result = json.loads(process.stdout)
    if not isinstance(result, dict) or result.get("protocol") != _CHILD_PROTOCOL:
        raise ValueError("scientific runtime did not finalize the child protocol")
    values = result.get("values")
    target_names = result.get("target_names")
    if not isinstance(values, list) or not isinstance(target_names, list):
        raise ValueError("scientific runtime omitted predictions or target names")
    observations = {
        f"prediction.{target}": [float(row[index]) for row in values] for index, target in enumerate(target_names)
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
            "prediction.samples": float(len(values)),
            "prediction.targets": float(len(target_names)),
            "prediction.values": float(sum(len(row) for row in values)),
        },
        timings_ms={
            "runtime.predict": float(result["prediction_ms"]),
            "adapter.total": (time.perf_counter() - started) * 1000.0,
        },
    )


def _main() -> None:
    try:
        request = AdapterRequest.model_validate_json(sys.stdin.read())
        output = adapt(request)
    except (OSError, subprocess.SubprocessError, ValueError, ValidationError) as exc:
        print(str(exc), file=sys.stderr)
        raise SystemExit(2) from exc
    sys.stdout.write(output.model_dump_json())


def core_rust_main() -> None:
    """Run the direct nirs4all-core/Rust adapter."""
    _main()
