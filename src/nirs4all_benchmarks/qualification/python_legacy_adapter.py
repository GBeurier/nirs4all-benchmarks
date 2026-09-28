"""Strict Python oracle for the explicit historical ``engine='legacy'`` lane.

The launcher verifies a closed wheel set, then invokes a content-addressed
CPython virtual environment in isolated mode.  The child proves every installed
wheel member against RECORD before importing nirs4all and executing the frozen
PERF-001 fit/predict scenario.  No sibling checkout, PYTHONPATH, network lookup,
native facade, or fallback path participates in the oracle.
"""

from __future__ import annotations

import json
import math
import os
import stat
import subprocess
import sys
import time
from pathlib import Path, PurePosixPath
from typing import Any, Literal

from pydantic import ValidationError

from nirs4all_benchmarks.process_guard import run_bounded
from nirs4all_benchmarks.qualification.contract import AdapterRequest, ComponentOutput
from nirs4all_benchmarks.qualification.scientific_runtime_adapter import (
    _child_environment,
    _sha256,
    _strict_scenario,
    _wheel_contract,
)

_CLOSURE_SCHEMA = "nirs4all.python-legacy-wheel-closure.v1"
_CHILD_PROTOCOL = "nirs4all.python-legacy-child.v1"
_MAX_CHILD_OUTPUT = 2 * 1024 * 1024
_COMMIT = "c8b5fd5bf847ce26f78008b9abd00fa54f790825"


def _normalized_name(value: str) -> str:
    return value.strip().lower().replace("_", "-").replace(".", "-")


def _finite_matrix(value: Any, *, field: str, rows: int | None = None) -> list[list[float]]:
    if not isinstance(value, list) or not value or (rows is not None and len(value) != rows):
        raise ValueError(f"{field} must be a non-empty matrix with the required row count")
    width: int | None = None
    for row in value:
        if not isinstance(row, list) or not row:
            raise ValueError(f"{field} must be a non-empty rectangular matrix")
        width = len(row) if width is None else width
        if len(row) != width or any(
            isinstance(item, bool) or not isinstance(item, (int, float)) or not math.isfinite(item) for item in row
        ):
            raise ValueError(f"{field} must be a finite rectangular numeric matrix")
    return value


def _runtime_site(runtime: Path) -> Path:
    root = runtime.parent.parent
    if runtime.is_symlink() or not stat.S_ISREG(runtime.lstat().st_mode) or root.is_symlink():
        raise ValueError("runtime_python must be a regular file in a non-symlink runtime root")
    candidates = [root / "lib/python3.11/site-packages", root / "Lib/site-packages"]
    available: list[Path] = []
    for candidate in candidates:
        if not candidate.is_dir():
            continue
        relative = candidate.relative_to(root)
        current = root
        for part in relative.parts:
            current /= part
            metadata = current.lstat()
            if current.is_symlink() or not stat.S_ISDIR(metadata.st_mode):
                raise ValueError("runtime_python site-packages path contains a symlink or non-directory ancestor")
        if not candidate.resolve().is_relative_to(root.resolve()):
            raise ValueError("runtime_python site-packages escapes the runtime root")
        available.append(candidate)
    if len(available) != 1:
        raise ValueError("runtime_python must belong to one explicit CPython 3.11 site-packages tree")
    return available[0]


def _verify_runtime_tree(runtime: Path, contracts: list[dict[str, Any]]) -> None:
    site = _runtime_site(runtime)
    expected: dict[PurePosixPath, str] = {}
    expected_directories: set[PurePosixPath] = set()
    for contract in contracts:
        for relative_text, digest in contract["files"].items():
            relative = PurePosixPath(relative_text)
            if relative.suffix == ".pth" or "__pycache__" in relative.parts or relative.suffix == ".pyc":
                raise ValueError(f"wheel closure contains forbidden ambient runtime member {relative_text!r}")
            if relative in expected:
                raise ValueError(f"wheel closure overlaps at installed member {relative_text!r}")
            expected[relative] = digest
            expected_directories.update(relative.parents[:-1])

    actual_files: set[PurePosixPath] = set()
    actual_directories: set[PurePosixPath] = set()
    for directory, names, files in os.walk(site, followlinks=False):
        directory_path = Path(directory)
        relative_directory = PurePosixPath(directory_path.relative_to(site).as_posix())
        if relative_directory != PurePosixPath("."):
            actual_directories.add(relative_directory)
        for name in names:
            path = directory_path / name
            metadata = path.lstat()
            if path.is_symlink() or not stat.S_ISDIR(metadata.st_mode):
                raise ValueError("Python legacy runtime contains a symlink or non-directory entry")
        for name in files:
            path = directory_path / name
            relative = PurePosixPath(path.relative_to(site).as_posix())
            metadata = path.lstat()
            if path.is_symlink() or not stat.S_ISREG(metadata.st_mode):
                raise ValueError(f"Python legacy runtime member {str(relative)!r} is a symlink or non-regular file")
            if relative.suffix == ".pth" or "__pycache__" in relative.parts or relative.suffix == ".pyc":
                raise ValueError(f"Python legacy runtime contains forbidden ambient member {str(relative)!r}")
            expected_digest = expected.get(relative)
            if expected_digest is None:
                raise ValueError(f"Python legacy runtime contains undeclared member {str(relative)!r}")
            if _sha256(path) != expected_digest:
                raise ValueError(f"Python legacy runtime member {str(relative)!r} differs from wheel RECORD")
            actual_files.add(relative)
    if actual_files != set(expected):
        missing = sorted(str(path) for path in set(expected) - actual_files)
        raise ValueError(f"Python legacy runtime omits wheel RECORD members: {missing}")
    if actual_directories != expected_directories:
        extra = sorted(str(path) for path in actual_directories - expected_directories)
        missing = sorted(str(path) for path in expected_directories - actual_directories)
        raise ValueError(f"Python legacy runtime directory inventory differs: extra={extra}, missing={missing}")


def _legacy_scenario(scenario: dict[str, Any]) -> dict[str, Any]:
    value = scenario["legacy_oracle"]
    if set(value) != {"engine", "allow_fallback", "random_state", "splitter", "model", "training"}:
        raise ValueError("legacy_oracle must contain only the finalized legacy PERF-001 fields")
    if value["engine"] != "legacy":
        raise ValueError("Python oracle requires the literal engine='legacy'; native and dag-ml are refused")
    if value["allow_fallback"] is not False:
        raise ValueError("Python oracle must explicitly forbid fallback")
    if value["random_state"] != 12345:
        raise ValueError("Python oracle random_state must be the finalized value 12345")
    if value["splitter"] != {
        "class": "sklearn.model_selection.KFold",
        "n_splits": 3,
        "shuffle": True,
        "random_state": 12345,
    }:
        raise ValueError("Python oracle splitter differs from the finalized deterministic KFold")
    if value["model"] != {
        "class": "sklearn.cross_decomposition.PLSRegression",
        "n_components": 1,
        "scale": True,
        "max_iter": 500,
        "tol": 1e-6,
    }:
        raise ValueError("Python oracle model differs from the finalized deterministic PLSRegression")
    training = value["training"]
    if not isinstance(training, dict) or set(training) != {"sample_ids", "target_names", "x", "y"}:
        raise ValueError("legacy_oracle training must contain only sample_ids, target_names, x and y")
    sample_ids = training["sample_ids"]
    target_names = training["target_names"]
    if sample_ids != [f"fit.{index}" for index in range(6)]:
        raise ValueError("Python oracle training sample_ids differ from the finalized six-row witness")
    if target_names != ["protein", "moisture"]:
        raise ValueError("Python oracle target_names differ from the finalized multi-target witness")
    x = _finite_matrix(training["x"], field="legacy_oracle.training.x", rows=6)
    y = _finite_matrix(training["y"], field="legacy_oracle.training.y", rows=6)
    if any(len(row) != 2 for row in x) or any(len(row) != 2 for row in y):
        raise ValueError("Python oracle training witness must have two features and two targets")
    return value


def _closure(request: AdapterRequest) -> tuple[Path, list[dict[str, Any]]]:
    artifacts = {artifact.role: artifact for artifact in request.artifacts}
    if "runtime_python" not in artifacts or "wheelhouse_manifest" not in artifacts:
        raise ValueError("python_oracle requires runtime_python and wheelhouse_manifest artifacts")
    for role, artifact in artifacts.items():
        if not artifact.path.is_file():
            raise ValueError(f"artifact {role!r} is not a regular file")
        if _sha256(artifact.path) != artifact.sha256:
            raise ValueError(f"artifact {role!r} SHA-256 changed after qualification preflight")
    runtime = artifacts["runtime_python"].path
    if not os.access(runtime, os.X_OK):
        raise ValueError("runtime_python artifact is not executable")

    manifest_path = artifacts["wheelhouse_manifest"].path
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    if not isinstance(manifest, dict) or set(manifest) != {
        "schema_version",
        "python_commit_sha",
        "engine",
        "fallback_allowed",
        "runtime",
        "wheels",
        "build_sources",
    }:
        raise ValueError("wheelhouse_manifest has an unknown or missing field")
    if manifest["schema_version"] != _CLOSURE_SCHEMA:
        raise ValueError(f"wheelhouse_manifest schema must be {_CLOSURE_SCHEMA!r}")
    if manifest["python_commit_sha"] != request.declared_commit_sha or request.declared_commit_sha != _COMMIT:
        raise ValueError("wheelhouse_manifest is not for the selected Python commit")
    if manifest["engine"] != "legacy" or manifest["fallback_allowed"] is not False:
        raise ValueError("wheelhouse_manifest must select only engine='legacy' with fallback forbidden")
    if manifest["runtime"] != {"implementation": "CPython", "major_minor": "3.11"}:
        raise ValueError("wheelhouse_manifest runtime must be the selected CPython 3.11")

    wheels = manifest["wheels"]
    sources = manifest["build_sources"]
    if not isinstance(wheels, list) or not wheels or not isinstance(sources, list):
        raise ValueError("wheelhouse_manifest must contain a non-empty wheels list and a build_sources list")
    declared_roles = {"runtime_python", "wheelhouse_manifest"}
    contracts: list[dict[str, Any]] = []
    for entry in wheels:
        if not isinstance(entry, dict) or set(entry) != {"role", "filename", "sha256", "distribution", "version"}:
            raise ValueError("wheelhouse_manifest wheel entry has an unknown or missing field")
        role = entry["role"]
        if not isinstance(role, str) or not role.startswith("wheel.") or role in declared_roles:
            raise ValueError("wheelhouse_manifest wheel roles must be unique and start with 'wheel.'")
        declared_roles.add(role)
        selected_artifact = artifacts.get(role)
        if (
            selected_artifact is None
            or selected_artifact.path.name != entry["filename"]
            or selected_artifact.sha256 != entry["sha256"]
        ):
            raise ValueError(f"wheel artifact {role!r} does not match wheelhouse_manifest")
        expected_name = _normalized_name(entry["distribution"])
        contract = _wheel_contract(selected_artifact.path, expected_name)
        if contract["version"] != entry["version"]:
            raise ValueError(f"wheel artifact {role!r} version differs from wheelhouse_manifest")
        contracts.append(contract)
    for entry in sources:
        if not isinstance(entry, dict) or set(entry) != {"role", "filename", "sha256", "builds_wheel"}:
            raise ValueError("wheelhouse_manifest build source has an unknown or missing field")
        role = entry["role"]
        if not isinstance(role, str) or not role.startswith("source.") or role in declared_roles:
            raise ValueError("wheelhouse_manifest source roles must be unique and start with 'source.'")
        declared_roles.add(role)
        selected_source = artifacts.get(role)
        if (
            selected_source is None
            or selected_source.path.name != entry["filename"]
            or selected_source.sha256 != entry["sha256"]
        ):
            raise ValueError(f"source artifact {role!r} does not match wheelhouse_manifest")
        if entry["builds_wheel"] not in {item["role"] for item in wheels}:
            raise ValueError(f"source artifact {role!r} names an unknown built wheel")
    if set(artifacts) != declared_roles:
        raise ValueError(
            "python_oracle artifacts must exactly match the closed wheelhouse manifest; "
            f"unexpected={sorted(set(artifacts) - declared_roles)}, missing={sorted(declared_roles - set(artifacts))}"
        )
    nirs4all_contracts = [item for item in contracts if item["name"] == "nirs4all"]
    if len(nirs4all_contracts) != 1:
        raise ValueError("wheelhouse_manifest must contain exactly one nirs4all wheel")
    if nirs4all_contracts[0]["version"] != request.declared_version:
        raise ValueError("declared component version differs from the nirs4all wheel")
    return runtime, contracts


_CHILD_SCRIPT = r"""
import collections, contextlib, hashlib, importlib, importlib.metadata, io, json, math, os
import site, stat, sys, tempfile, time
from pathlib import Path, PurePosixPath

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
    site_packages = Path(payload["site_packages"])
    if (not root.is_absolute() or not site_packages.is_absolute() or root.is_symlink() or site_packages.is_symlink()
            or not root.is_dir() or not site_packages.is_dir()):
        raise RuntimeError("declared Python runtime layout is not an absolute regular directory tree")
    unresolved_root = root
    unresolved_site = site_packages
    relative_site = unresolved_site.relative_to(unresolved_root)
    current = unresolved_root
    for part in relative_site.parts:
        current = current / part
        if current.is_symlink() or not stat.S_ISDIR(current.lstat().st_mode):
            raise RuntimeError("declared site-packages path contains a symlink or non-directory ancestor")
    root = root.resolve()
    site_packages = site_packages.resolve()
    if not below(site_packages, root):
        raise RuntimeError("declared site-packages escapes the Python runtime root")
    expected = {}
    expected_directories = set()
    for contract in payload["wheels"]:
        for relative_text, digest in contract["files"].items():
            relative = PurePosixPath(relative_text)
            if relative.suffix == ".pth" or "__pycache__" in relative.parts or relative.suffix == ".pyc":
                raise RuntimeError("wheel closure contains an ambient runtime member")
            if relative in expected:
                raise RuntimeError("wheel contracts overlap at %s" % relative_text)
            expected[relative] = digest
            expected_directories.update(relative.parents[:-1])
    actual = set()
    actual_directories = set()
    for directory, names, files in os.walk(site_packages, followlinks=False):
        directory_path = Path(directory)
        relative_directory = PurePosixPath(directory_path.relative_to(site_packages).as_posix())
        if relative_directory != PurePosixPath("."):
            actual_directories.add(relative_directory)
        for name in names:
            path = directory_path / name
            if path.is_symlink() or not stat.S_ISDIR(path.lstat().st_mode):
                raise RuntimeError("installed runtime contains a symlink or non-directory entry")
        for name in files:
            path = directory_path / name
            relative = PurePosixPath(path.relative_to(site_packages).as_posix())
            if path.is_symlink() or not stat.S_ISREG(path.lstat().st_mode):
                raise RuntimeError("installed runtime contains a symlink or non-regular file")
            if relative.suffix == ".pth" or "__pycache__" in relative.parts or relative.suffix == ".pyc":
                raise RuntimeError("installed runtime contains an ambient runtime member")
            if relative not in expected or sha256(path) != expected[relative]:
                raise RuntimeError("installed runtime member differs from the declared wheel RECORD")
            actual.add(relative)
    if actual != set(expected) or actual_directories != expected_directories:
        raise RuntimeError("installed runtime inventory differs from the declared wheel closure")
    return root, site_packages

def verify_wheel(contract):
    distribution = importlib.metadata.distribution(contract["name"])
    if distribution.version != contract["version"]:
        raise RuntimeError("installed %s version differs from declared wheel" % contract["name"])
    installed_hashes = collections.Counter()
    for relative in distribution.files or ():
        installed = Path(distribution.locate_file(relative)).resolve()
        if not below(installed, RUNTIME_ROOT):
            raise RuntimeError("installed %s escapes the explicit runtime prefix" % contract["name"])
        if installed.is_file() and "__pycache__" not in installed.parts and installed.suffix != ".pyc":
            installed_hashes[sha256(installed)] += 1
    expected_hashes = collections.Counter(contract["files"].values())
    missing = expected_hashes - installed_hashes
    if missing:
        raise RuntimeError("installed %s bytes differ from the declared wheel RECORD" % contract["name"])
    return distribution

def import_below_prefix(name):
    module = importlib.import_module(name)
    location = getattr(module, "__file__", None)
    if location is None or not below(Path(location).resolve(), RUNTIME_ROOT):
        raise RuntimeError("imported %s escapes the explicit runtime prefix" % name)
    return module

payload = json.load(sys.stdin)
if payload.get("protocol") != "nirs4all.python-legacy-child.v1" or payload.get("mode") != "legacy":
    raise RuntimeError("invalid Python legacy child protocol or mode")
if (not sys.flags.isolated or not sys.flags.no_site or not sys.flags.dont_write_bytecode
        or site.ENABLE_USER_SITE not in (None, False) or "PYTHONPATH" in os.environ or "N4A_ENGINE" in os.environ):
    raise RuntimeError("Python legacy child must run with -I -S -B without PYTHONPATH or N4A_ENGINE")

RUNTIME_ROOT, SITE_PACKAGES = verify_runtime_inventory(payload)
sys.path.insert(0, str(SITE_PACKAGES))
distributions = {contract["name"]: verify_wheel(contract) for contract in payload["wheels"]}
packaging = import_below_prefix("packaging.requirements")
markers = import_below_prefix("packaging.markers")
closed_names = set(distributions)
for name, distribution in distributions.items():
    for raw in distribution.requires or ():
        requirement = packaging.Requirement(raw)
        environment = markers.default_environment()
        environment["extra"] = ""
        if requirement.marker is not None and not requirement.marker.evaluate(environment):
            continue
        dependency_name = requirement.name.lower().replace("_", "-").replace(".", "-")
        if dependency_name not in closed_names:
            raise RuntimeError("wheel closure omits mandatory dependency %s required by %s" % (dependency_name, name))
        if distributions[dependency_name].version not in requirement.specifier:
            raise RuntimeError("wheel closure version violates %s required by %s" % (raw, name))

np = import_below_prefix("numpy")
nirs4all = import_below_prefix("nirs4all")
joblib = import_below_prefix("joblib")
data_module = import_below_prefix("nirs4all.data")
cross = import_below_prefix("sklearn.cross_decomposition")
selection = import_below_prefix("sklearn.model_selection")
legacy = payload["scenario"]["legacy_oracle"]
training = legacy["training"]
dataset = data_module.SpectroDataset("perf001_legacy")
dataset.add_samples(np.asarray(training["x"], dtype=float), indexes={"partition": "train"})
dataset.add_targets(np.asarray(training["y"], dtype=float))
if str(getattr(dataset.task_type, "value", dataset.task_type)) != "regression":
    raise RuntimeError("frozen legacy witness was not detected as regression")

started = time.perf_counter()
captured_stdout, captured_stderr = io.StringIO(), io.StringIO()
with tempfile.TemporaryDirectory(prefix="n4a-python-legacy-") as workspace:
    with contextlib.redirect_stdout(captured_stdout), contextlib.redirect_stderr(captured_stderr):
        result = nirs4all.run(
            [
                selection.KFold(n_splits=3, shuffle=True, random_state=12345),
                {"model": cross.PLSRegression(n_components=1, scale=True, max_iter=500, tol=1e-6)},
            ],
            dataset,
            verbose=0,
            save_artifacts=True,
            save_charts=False,
            plots_visible=False,
            random_state=12345,
            workspace_path=workspace,
            engine="legacy",
            allow_fallback=False,
        )
        model_path = result.export_model(Path(workspace) / "oracle.joblib")
        model = joblib.load(model_path)
        values = np.asarray(model.predict(np.asarray(payload["scenario"]["prediction"]["x"], dtype=float)), dtype=float)
elapsed_ms = (time.perf_counter() - started) * 1000.0
if captured_stdout.tell() > 2 * 1024 * 1024 or captured_stderr.tell() > 2 * 1024 * 1024:
    raise RuntimeError("legacy runtime diagnostic output exceeded the 2 MiB limit")
if bool(result._is_dagml_engine()) or getattr(result, "_rt_diagnostics", None):
    raise RuntimeError("Python oracle reached a native/fallback result instead of explicit legacy")
if values.shape != (len(payload["scenario"]["prediction"]["sample_ids"]), len(training["target_names"])):
    raise RuntimeError("legacy runtime returned a prediction matrix with the wrong shape")
if not np.isfinite(values).all():
    raise RuntimeError("legacy runtime returned non-finite predictions")
json.dump({
    "protocol": "nirs4all.python-legacy-child.v1",
    "engine": "legacy",
    "fallback_used": False,
    "values": values.tolist(),
    "target_names": training["target_names"],
    "prediction_ms": elapsed_ms,
    "wheel_count": len(distributions),
}, sys.stdout, sort_keys=True, allow_nan=False)
"""


def adapt(request: AdapterRequest) -> ComponentOutput:
    """Run the content-addressed, explicit Python legacy oracle."""
    component: Literal["python_oracle"] = "python_oracle"
    if request.component != component:
        raise ValueError("Python legacy launcher only accepts the python_oracle component")
    started = time.perf_counter()
    scenario = _strict_scenario(request)
    _legacy_scenario(scenario)
    runtime, wheels = _closure(request)
    _verify_runtime_tree(runtime, wheels)
    site_packages = _runtime_site(runtime)
    payload = {
        "protocol": _CHILD_PROTOCOL,
        "mode": "legacy",
        "scenario": scenario,
        "wheels": wheels,
        "runtime_root": str(runtime.parent.parent.resolve()),
        "site_packages": str(site_packages.resolve()),
    }
    process = run_bounded(
        [str(runtime), "-I", "-S", "-B", "-c", _CHILD_SCRIPT],
        input_text=json.dumps(payload, sort_keys=True, separators=(",", ":"), allow_nan=False),
        env=_child_environment(),
        timeout=600,
        max_output_bytes=_MAX_CHILD_OUTPUT,
    )
    _verify_runtime_tree(runtime, wheels)
    if len(process.stdout.encode()) > _MAX_CHILD_OUTPUT or len(process.stderr.encode()) > _MAX_CHILD_OUTPUT:
        raise ValueError("Python legacy runtime output exceeded the 2 MiB limit")
    if process.returncode != 0:
        detail = process.stderr.strip().splitlines()[-1] if process.stderr.strip() else "no diagnostic"
        raise ValueError(f"Python legacy runtime failed with code {process.returncode}: {detail[:500]}")
    result = json.loads(process.stdout)
    if (
        not isinstance(result, dict)
        or result.get("protocol") != _CHILD_PROTOCOL
        or result.get("engine") != "legacy"
        or result.get("fallback_used") is not False
    ):
        raise ValueError("Python legacy runtime did not finalize the explicit legacy child protocol")
    values = result.get("values")
    target_names = result.get("target_names")
    if not isinstance(values, list) or not isinstance(target_names, list):
        raise ValueError("Python legacy runtime omitted predictions or target names")
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
            "runtime.verified_wheels": float(result["wheel_count"]),
        },
        timings_ms={
            "runtime.fit_predict": float(result["prediction_ms"]),
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


def python_oracle_main() -> None:
    """Run the explicit historical Python oracle adapter."""
    _main()
