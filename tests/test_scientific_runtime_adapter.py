"""PERF-001 launchers use only explicit, content-addressed runtime artifacts."""

from __future__ import annotations

import base64
import csv
import hashlib
import io
import json
import stat
import subprocess
import sys
from pathlib import Path
from zipfile import ZIP_DEFLATED, ZipFile, ZipInfo

import pytest

from nirs4all_benchmarks.qualification.contract import AdapterRequest
from nirs4all_benchmarks.qualification.scientific_runtime_adapter import adapt


def _sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _record_digest(payload: bytes) -> str:
    return base64.urlsafe_b64encode(hashlib.sha256(payload).digest()).rstrip(b"=").decode()


def _wheel(tmp_path: Path, distribution: str, package: str, version: str, source: str) -> Path:
    normalized = distribution.replace("-", "_")
    dist_info = f"{normalized}-{version}.dist-info"
    members = {
        f"{package}/__init__.py": source.encode(),
        f"{dist_info}/METADATA": (f"Metadata-Version: 2.1\nName: {distribution}\nVersion: {version}\n").encode(),
        f"{dist_info}/WHEEL": b"Wheel-Version: 1.0\nGenerator: qualification-test\nRoot-Is-Purelib: true\nTag: py3-none-any\n",
    }
    rows = [[name, f"sha256={_record_digest(payload)}", str(len(payload))] for name, payload in members.items()]
    rows.append([f"{dist_info}/RECORD", "", ""])
    record = io.StringIO()
    csv.writer(record, lineterminator="\n").writerows(rows)
    members[f"{dist_info}/RECORD"] = record.getvalue().encode()
    path = tmp_path / f"{normalized}-{version}-py3-none-any.whl"
    with ZipFile(path, "w", compression=ZIP_DEFLATED) as archive:
        for name, payload in members.items():
            archive.writestr(name, payload)
    return path


def _add_recorded_member(path: Path, member: str, payload: bytes, mode: int) -> None:
    with ZipFile(path) as archive:
        entries = [(info, archive.read(info)) for info in archive.infolist()]
    record_name = next(info.filename for info, _payload in entries if info.filename.endswith(".dist-info/RECORD"))
    record_payload = next(payload for info, payload in entries if info.filename == record_name)
    rows = list(csv.reader(io.StringIO(record_payload.decode())))
    self_index = next(index for index, row in enumerate(rows) if row[0] == record_name)
    rows.insert(self_index, [member, f"sha256={_record_digest(payload)}", str(len(payload))])
    record = io.StringIO()
    csv.writer(record, lineterminator="\n").writerows(rows)
    hostile = ZipInfo(member)
    hostile.create_system = 3
    hostile.external_attr = mode << 16
    with ZipFile(path, "w", compression=ZIP_DEFLATED) as archive:
        for info, existing in entries:
            if info.filename != record_name:
                archive.writestr(info, existing)
        archive.writestr(hostile, payload)
        archive.writestr(record_name, record.getvalue().encode())


def _rewrite_record_field(path: Path, member: str, column: int, value: str) -> None:
    with ZipFile(path) as archive:
        entries = [(info, archive.read(info)) for info in archive.infolist()]
    record_name = next(info.filename for info, _payload in entries if info.filename.endswith(".dist-info/RECORD"))
    record_payload = next(payload for info, payload in entries if info.filename == record_name)
    rows = list(csv.reader(io.StringIO(record_payload.decode())))
    row = next(row for row in rows if row[0] == member)
    row[column] = value
    record = io.StringIO()
    csv.writer(record, lineterminator="\n").writerows(rows)
    with ZipFile(path, "w", compression=ZIP_DEFLATED) as archive:
        for info, payload in entries:
            if info.filename != record_name:
                archive.writestr(info, payload)
        archive.writestr(record_name, record.getvalue().encode())


_CORE_SOURCE = """
import hashlib
import sys
from pathlib import Path

import yaml

def read_portable_predictor_package_v2(*_args, **_kwargs):
    raise AssertionError("launcher must not read or compose the replay package")

def replay_methods_archive_v2(*_args, **_kwargs):
    raise AssertionError("launcher must not call the manual replay API")

def predict_methods_archive_v2_matrix(
    archive, sample_ids, x, expected_target_names, *,
    methods_library_path, methods_library_sha256,
    request_id, outcome_id, run_id, warnings=(), diagnostics=None,
):
    if "dag_ml" in sys.modules:
        raise AssertionError("launcher imported Python dag_ml")
    if not Path(archive).is_file():
        raise AssertionError("archive identity was not forwarded")
    actual = hashlib.sha256(Path(methods_library_path).read_bytes()).hexdigest()
    if actual != methods_library_sha256:
        raise AssertionError("Methods identity was not forwarded exactly")
    if expected_target_names != ["protein", "moisture"]:
        raise AssertionError("target identity was not forwarded")
    if request_id != "request:nirs4all.perf001":
        raise AssertionError("request identity drift")
    if outcome_id != "outcome:nirs4all.perf001" or run_id != "run:nirs4all.perf001":
        raise AssertionError("outcome identity drift")
    if warnings != () or diagnostics != {"qualification": "PERF-001", "fallback_allowed": False}:
        raise AssertionError("closed matrix diagnostics drift")
    values = [[sum(row), sum(row) * 2.0] for row in x]
    return {
        "phase": "PREDICT",
        "outputs": [{
            "predictions": [{
                "sample_ids": sample_ids,
                "target_names": expected_target_names,
                "values": values,
            }],
        }],
    }
"""


@pytest.fixture
def scientific_artifacts(tmp_path: Path) -> dict[str, Path]:
    runtime = tmp_path / "runtime"
    base_python = Path(sys.base_prefix) / (
        "python.exe" if sys.platform == "win32"
        else f"bin/python{sys.version_info.major}.{sys.version_info.minor}"
    )
    subprocess.run([str(base_python), "-m", "venv", "--without-pip", "--copies", str(runtime)], check=True)
    python = runtime / ("Scripts/python.exe" if sys.platform == "win32" else "bin/python")
    site = runtime / (
        "Lib/site-packages"
        if sys.platform == "win32"
        else f"lib/python{sys.version_info.major}.{sys.version_info.minor}/site-packages"
    )
    wheels = {
        "core_wheel": _wheel(tmp_path, "nirs4all-core", "nirs4all_core", "0.3.24", _CORE_SOURCE),
        "pyyaml_wheel": _wheel(tmp_path, "PyYAML", "yaml", "6.0.2", ""),
    }
    for wheel in wheels.values():
        with ZipFile(wheel) as archive:
            archive.extractall(site)
    scenario = tmp_path / "scenario.json"
    scenario.write_text(
        json.dumps(
            {
                "schema_version": "nirs4all.perf-scenario.v1",
                "scenario_id": "perf001-multitarget",
                "operation": "archive_v2_predict",
                "fallback_allowed": False,
                "prediction": {
                    "sample_ids": ["predict.0", "predict.1"],
                    "x": [[1.5, 0.5], [3.5, 1.5]],
                },
                "legacy_oracle": {"training": {"target_names": ["protein", "moisture"]}},
            }
        ),
        encoding="utf-8",
    )
    archive = tmp_path / "selected.n4a"
    archive.write_bytes(b"content-addressed archive witness")
    library = tmp_path / "libn4m.so"
    library.write_bytes(b"content-addressed methods witness")
    return {
        "runtime_python": python,
        "scenario": scenario,
        "archive_v2": archive,
        "methods_library": library,
        **wheels,
    }


def _request(paths: dict[str, Path], component: str = "core_rust") -> AdapterRequest:
    roles = [
        "runtime_python",
        "core_wheel",
        "pyyaml_wheel",
        "archive_v2",
        "methods_library",
    ]
    version = "0.3.24"
    return AdapterRequest.model_validate(
        {
            "component": component,
            "declared_version": version,
            "declared_commit_sha": "1" * 40,
            "scenario": {
                "scenario_id": "perf001-multitarget",
                "definition": {"path": paths["scenario"], "sha256": _sha256(paths["scenario"])},
            },
            "tolerances": {"absolute": 1e-12, "relative": 1e-9},
            "artifacts": [{"role": role, "path": paths[role], "sha256": _sha256(paths[role])} for role in roles],
        }
    )


def _site_packages(paths: dict[str, Path]) -> Path:
    runtime = paths["runtime_python"].parent.parent
    return runtime / (
        "Lib/site-packages"
        if sys.platform == "win32"
        else f"lib/python{sys.version_info.major}.{sys.version_info.minor}/site-packages"
    )


def test_core_launcher_produces_finalized_observations(
    scientific_artifacts: dict[str, Path], monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("PYTHONPATH", "/unselected/sibling/checkout")

    core = adapt(_request(scientific_artifacts))

    assert core.completed is True
    assert core.fallback_used is False
    assert core.observations == {
        "prediction.protein": [2.0, 5.0],
        "prediction.moisture": [4.0, 10.0],
    }
    assert core.metrics["prediction.values"] == 4.0
    assert core.timings_ms["runtime.predict"] >= 0


def test_core_launcher_never_executes_site_bootstrap_before_inventory(
    scientific_artifacts: dict[str, Path], tmp_path: Path
) -> None:
    marker = tmp_path / "site-bootstrap-executed"
    site = _site_packages(scientific_artifacts)
    (site / "sitecustomize.py").write_text(
        f"from pathlib import Path\nPath({str(marker)!r}).write_text('sitecustomize')\n"
        "raise RuntimeError('sitecustomize executed')\n",
        encoding="utf-8",
    )
    (site / "hostile.pth").write_text(
        f"import pathlib; pathlib.Path({str(marker)!r}).write_text('pth')\n",
        encoding="utf-8",
    )

    with pytest.raises(ValueError, match="runtime inventory differs"):
        adapt(_request(scientific_artifacts))
    assert not marker.exists()


def test_core_launcher_refuses_extra_installed_file(scientific_artifacts: dict[str, Path]) -> None:
    (_site_packages(scientific_artifacts) / "unexpected-runtime-member.txt").write_text(
        "not wheel-owned", encoding="utf-8"
    )

    with pytest.raises(ValueError, match="runtime inventory differs"):
        adapt(_request(scientific_artifacts))


def test_core_launcher_refuses_wheel_without_public_matrix_api(
    scientific_artifacts: dict[str, Path], tmp_path: Path
) -> None:
    wheel = _wheel(
        tmp_path,
        "nirs4all-core",
        "nirs4all_core",
        "0.3.24",
        "def replay_methods_archive_v2(*_args, **_kwargs):\n    return {}\n",
    )
    site = _site_packages(scientific_artifacts)
    with ZipFile(wheel) as archive:
        archive.extractall(site)
    scientific_artifacts["core_wheel"] = wheel

    with pytest.raises(ValueError, match="predict_methods_archive_v2_matrix"):
        adapt(_request(scientific_artifacts))


def test_launcher_refuses_declared_fallback_before_execution(scientific_artifacts: dict[str, Path]) -> None:
    scenario = scientific_artifacts["scenario"]
    value = json.loads(scenario.read_text(encoding="utf-8"))
    value["fallback_allowed"] = True
    scenario.write_text(json.dumps(value), encoding="utf-8")
    request = _request(scientific_artifacts, "core_rust")

    with pytest.raises(ValueError, match="explicitly forbid fallback"):
        adapt(request)


def test_launcher_refuses_missing_explicit_artifact_role(scientific_artifacts: dict[str, Path]) -> None:
    request = _request(scientific_artifacts, "core_rust")
    request.artifacts = [artifact for artifact in request.artifacts if artifact.role != "methods_library"]

    with pytest.raises(ValueError, match="requires exactly artifact roles"):
        adapt(request)


def test_launcher_refuses_obsolete_dag_ml_wheel_role(scientific_artifacts: dict[str, Path]) -> None:
    request = _request(scientific_artifacts, "core_rust")
    request.artifacts.append(request.artifacts[1].model_copy(update={"role": "dag_ml_wheel"}))

    with pytest.raises(ValueError, match="requires exactly artifact roles"):
        adapt(request)


def test_launcher_refuses_duplicate_expected_artifact_role(scientific_artifacts: dict[str, Path]) -> None:
    request = _request(scientific_artifacts, "core_rust")
    request.artifacts.append(request.artifacts[0].model_copy())

    with pytest.raises(ValueError, match="requires exactly artifact roles"):
        adapt(request)


def test_launcher_refuses_symlink_scenario(scientific_artifacts: dict[str, Path], tmp_path: Path) -> None:
    target = scientific_artifacts["scenario"]
    link = tmp_path / "scenario-link.json"
    link.symlink_to(target)
    scientific_artifacts["scenario"] = link

    with pytest.raises(ValueError, match="scenario definition is not a regular non-symlink file"):
        adapt(_request(scientific_artifacts))


def test_launcher_refuses_oversize_scenario(scientific_artifacts: dict[str, Path]) -> None:
    scientific_artifacts["scenario"].write_bytes(b" " * (1024 * 1024 + 1))

    with pytest.raises(ValueError, match="scenario definition exceeds the 1048576-byte limit"):
        adapt(_request(scientific_artifacts))


def test_launcher_refuses_unrecorded_wheel_member(scientific_artifacts: dict[str, Path]) -> None:
    with ZipFile(scientific_artifacts["core_wheel"], "a", compression=ZIP_DEFLATED) as archive:
        archive.writestr("nirs4all_core/unrecorded.py", b"raise RuntimeError('injected')\n")

    with pytest.raises(ValueError, match="inventory differs from RECORD"):
        adapt(_request(scientific_artifacts))


def test_launcher_refuses_duplicate_wheel_member(scientific_artifacts: dict[str, Path]) -> None:
    with (
        pytest.warns(UserWarning, match="Duplicate name"),
        ZipFile(scientific_artifacts["core_wheel"], "a", compression=ZIP_DEFLATED) as archive,
    ):
        archive.writestr("nirs4all_core/__init__.py", b"raise RuntimeError('duplicate')\n")

    with pytest.raises(ValueError, match="duplicate member names"):
        adapt(_request(scientific_artifacts))


def test_launcher_refuses_recorded_wheel_symlink(scientific_artifacts: dict[str, Path]) -> None:
    _add_recorded_member(
        scientific_artifacts["core_wheel"],
        "nirs4all_core/linked.py",
        b"outside.py",
        stat.S_IFLNK | 0o777,
    )

    with pytest.raises(ValueError, match="not a regular file"):
        adapt(_request(scientific_artifacts))


@pytest.mark.parametrize(
    ("column", "value", "message"),
    [
        (1, f"sha256={_record_digest(b'forged')}", "bytes differ from RECORD"),
        (2, "999999", "size differs from RECORD"),
    ],
)
def test_launcher_refuses_false_wheel_record_identity(
    scientific_artifacts: dict[str, Path], column: int, value: str, message: str
) -> None:
    _rewrite_record_field(
        scientific_artifacts["core_wheel"],
        "nirs4all_core/__init__.py",
        column,
        value,
    )

    with pytest.raises(ValueError, match=message):
        adapt(_request(scientific_artifacts))


@pytest.mark.parametrize("member", ["C:/escape.py", "nirs4all_core\\escape.py"])
def test_launcher_refuses_unsafe_recorded_wheel_path(scientific_artifacts: dict[str, Path], member: str) -> None:
    _add_recorded_member(
        scientific_artifacts["core_wheel"],
        member,
        b"raise RuntimeError('escape')\n",
        stat.S_IFREG | 0o644,
    )

    with pytest.raises(ValueError, match="unsafe member path"):
        adapt(_request(scientific_artifacts))


@pytest.mark.parametrize(
    "role",
    ["runtime_python", "core_wheel", "pyyaml_wheel", "archive_v2", "methods_library"],
)
def test_launcher_refuses_symlink_artifact(scientific_artifacts: dict[str, Path], tmp_path: Path, role: str) -> None:
    target = scientific_artifacts[role]
    link = tmp_path / f"{role}-link"
    link.symlink_to(target)
    scientific_artifacts[role] = link

    with pytest.raises(ValueError, match="regular non-symlink file"):
        adapt(_request(scientific_artifacts))


def test_launcher_refuses_non_regular_artifact(scientific_artifacts: dict[str, Path], tmp_path: Path) -> None:
    request = _request(scientific_artifacts)
    archive = next(artifact for artifact in request.artifacts if artifact.role == "archive_v2")
    archive.path = tmp_path

    with pytest.raises(ValueError, match="regular non-symlink file"):
        adapt(request)


def test_launcher_refuses_component_version_that_differs_from_wheel(scientific_artifacts: dict[str, Path]) -> None:
    request = _request(scientific_artifacts)
    request.declared_version = "9.9.9"

    with pytest.raises(ValueError, match="differs from wheel"):
        adapt(request)


def test_core_launcher_forbids_native_facade_masquerading_as_python_oracle(
    scientific_artifacts: dict[str, Path],
) -> None:
    request = _request(scientific_artifacts, "python_oracle")

    with pytest.raises(ValueError, match="reserved for the selected legacy engine"):
        adapt(request)


def test_launcher_refuses_installed_bytes_that_differ_from_wheel(scientific_artifacts: dict[str, Path]) -> None:
    runtime = scientific_artifacts["runtime_python"].parent.parent
    installed = runtime / (
        "Lib/site-packages/nirs4all_core/__init__.py"
        if sys.platform == "win32"
        else f"lib/python{sys.version_info.major}.{sys.version_info.minor}/site-packages/nirs4all_core/__init__.py"
    )
    installed.write_text("raise RuntimeError('tampered')\n", encoding="utf-8")

    with pytest.raises(ValueError, match="bytes differ from declared wheel"):
        adapt(_request(scientific_artifacts))


def test_launcher_refuses_non_executable_runtime(scientific_artifacts: dict[str, Path], tmp_path: Path) -> None:
    inert = tmp_path / "runtime-python"
    inert.write_bytes(b"not executable")
    inert.chmod(0o600)
    scientific_artifacts["runtime_python"] = inert

    with pytest.raises(ValueError, match="not executable"):
        adapt(_request(scientific_artifacts))
