"""The Python oracle is explicit legacy and closed over verified wheels."""

from __future__ import annotations

import base64
import csv
import hashlib
import io
import json
import subprocess
import sys
import venv
from pathlib import Path
from typing import Any
from zipfile import ZIP_DEFLATED, ZipFile

import pytest

from nirs4all_benchmarks.qualification.contract import AdapterRequest
from nirs4all_benchmarks.qualification import python_legacy_adapter
from nirs4all_benchmarks.qualification.python_legacy_adapter import _verify_runtime_tree, adapt
from nirs4all_benchmarks.qualification.scientific_runtime_adapter import _wheel_contract

_COMMIT = "c8b5fd5bf847ce26f78008b9abd00fa54f790825"


def _sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _record_digest(payload: bytes) -> str:
    return base64.urlsafe_b64encode(hashlib.sha256(payload).digest()).rstrip(b"=").decode()


def _wheel(tmp_path: Path) -> Path:
    dist_info = "nirs4all-0.10.3.dist-info"
    members = {
        "nirs4all/__init__.py": b"__version__ = '0.10.3'\n",
        f"{dist_info}/METADATA": b"Metadata-Version: 2.1\nName: nirs4all\nVersion: 0.10.3\n",
        f"{dist_info}/WHEEL": (
            b"Wheel-Version: 1.0\nGenerator: qualification-test\nRoot-Is-Purelib: true\nTag: py3-none-any\n"
        ),
    }
    rows = [[name, f"sha256={_record_digest(payload)}", str(len(payload))] for name, payload in members.items()]
    rows.append([f"{dist_info}/RECORD", "", ""])
    record = io.StringIO()
    csv.writer(record, lineterminator="\n").writerows(rows)
    members[f"{dist_info}/RECORD"] = record.getvalue().encode()
    path = tmp_path / "nirs4all-0.10.3-py3-none-any.whl"
    with ZipFile(path, "w", compression=ZIP_DEFLATED) as archive:
        for name, payload in members.items():
            archive.writestr(name, payload)
    return path


def _scenario() -> dict[str, Any]:
    epsilon = 1e-9
    y1 = [epsilon, 1.0, 2.0 - 2.0 * epsilon, 3.0, 4.0 + epsilon, 5.0]
    return {
        "schema_version": "nirs4all.perf-scenario.v1",
        "scenario_id": "perf001-multitarget-archive-v2",
        "operation": "archive_v2_predict",
        "fallback_allowed": False,
        "prediction": {
            "sample_ids": ["predict.0", "predict.1"],
            "x": [[1.5, 0.5], [3.5, 1.5]],
        },
        "legacy_oracle": {
            "engine": "legacy",
            "allow_fallback": False,
            "random_state": 12345,
            "splitter": {
                "class": "sklearn.model_selection.KFold",
                "n_splits": 3,
                "shuffle": True,
                "random_state": 12345,
            },
            "model": {
                "class": "sklearn.cross_decomposition.PLSRegression",
                "n_components": 1,
                "scale": True,
                "max_iter": 500,
                "tol": 1e-6,
            },
            "training": {
                "sample_ids": [f"fit.{index}" for index in range(6)],
                "target_names": ["protein", "moisture"],
                "x": [[0.0, 1.0], [1.0, 0.0], [2.0, 1.0], [3.0, 0.0], [4.0, 1.0], [5.0, 0.0]],
                "y": [[value, 10.0 + 2.0 * value] for value in y1],
            },
        },
    }


@pytest.fixture
def legacy_artifacts(tmp_path: Path) -> dict[str, Path]:
    runtime = tmp_path / "runtime"
    venv.EnvBuilder(with_pip=False, clear=True).create(runtime)
    python = runtime / ("Scripts/python.exe" if sys.platform == "win32" else "bin/python")
    wheel = _wheel(tmp_path)
    site_packages = runtime / ("Lib/site-packages" if sys.platform == "win32" else "lib/python3.11/site-packages")
    with ZipFile(wheel) as archive:
        archive.extractall(site_packages)
    scenario = tmp_path / "scenario.json"
    scenario.write_text(json.dumps(_scenario()), encoding="utf-8")
    closure = tmp_path / "closure.json"
    closure.write_text(
        json.dumps(
            {
                "schema_version": "nirs4all.python-legacy-wheel-closure.v1",
                "python_commit_sha": _COMMIT,
                "engine": "legacy",
                "fallback_allowed": False,
                "runtime": {"implementation": "CPython", "major_minor": "3.11"},
                "wheels": [
                    {
                        "role": "wheel.nirs4all",
                        "filename": wheel.name,
                        "sha256": _sha256(wheel),
                        "distribution": "nirs4all",
                        "version": "0.10.3",
                    }
                ],
                "build_sources": [],
            }
        ),
        encoding="utf-8",
    )
    return {"runtime_python": python, "wheel.nirs4all": wheel, "wheelhouse_manifest": closure, "scenario": scenario}


def _request(paths: dict[str, Path]) -> AdapterRequest:
    return AdapterRequest.model_validate(
        {
            "component": "python_oracle",
            "declared_version": "0.10.3",
            "declared_commit_sha": _COMMIT,
            "scenario": {
                "scenario_id": "perf001-multitarget-archive-v2",
                "definition": {"path": paths["scenario"], "sha256": _sha256(paths["scenario"])},
            },
            "tolerances": {"absolute": 1e-12, "relative": 1e-9},
            "artifacts": [
                {"role": role, "path": path, "sha256": _sha256(path)}
                for role, path in paths.items()
                if role != "scenario"
            ],
        }
    )


def _completed_process(*_args: Any, **kwargs: Any) -> subprocess.CompletedProcess[str]:
    payload = json.loads(kwargs["input_text"])
    assert payload["mode"] == "legacy"
    assert payload["scenario"]["legacy_oracle"]["engine"] == "legacy"
    assert kwargs["env"].get("PYTHONPATH") is None
    assert kwargs["env"].get("N4A_ENGINE") is None
    assert _args[0][1:5] == ["-I", "-S", "-B", "-c"]
    assert Path(payload["runtime_root"]).is_absolute()
    assert Path(payload["site_packages"]).is_absolute()
    result = {
        "protocol": "nirs4all.python-legacy-child.v1",
        "engine": "legacy",
        "fallback_used": False,
        "values": [[1.6363636364, 13.2727272727], [2.5, 15.0]],
        "target_names": ["protein", "moisture"],
        "prediction_ms": 1.25,
        "wheel_count": 1,
    }
    return subprocess.CompletedProcess(_args[0], 0, json.dumps(result), "")


def test_python_oracle_finalizes_only_explicit_legacy(
    legacy_artifacts: dict[str, Path], monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("PYTHONPATH", "/unselected/source/checkout")
    monkeypatch.setenv("N4A_ENGINE", "native")
    monkeypatch.setattr(python_legacy_adapter, "run_bounded", _completed_process)

    output = adapt(_request(legacy_artifacts))

    assert output.completed is True
    assert output.fallback_used is False
    assert output.observations["prediction.protein"] == [1.6363636364, 2.5]
    assert output.metrics["runtime.verified_wheels"] == 1.0


@pytest.mark.parametrize(
    ("field", "value", "message"),
    [
        ("engine", "native", "literal engine='legacy'"),
        ("engine", "dag-ml", "literal engine='legacy'"),
        ("allow_fallback", True, "explicitly forbid fallback"),
    ],
)
def test_python_oracle_refuses_native_and_fallback_before_execution(
    legacy_artifacts: dict[str, Path],
    monkeypatch: pytest.MonkeyPatch,
    field: str,
    value: object,
    message: str,
) -> None:
    scenario = legacy_artifacts["scenario"]
    document = json.loads(scenario.read_text(encoding="utf-8"))
    document["legacy_oracle"][field] = value
    scenario.write_text(json.dumps(document), encoding="utf-8")
    monkeypatch.setattr(python_legacy_adapter, "run_bounded", lambda *_args, **_kwargs: pytest.fail("runtime must not start"))

    with pytest.raises(ValueError, match=message):
        adapt(_request(legacy_artifacts))


def test_python_oracle_refuses_tampered_wheel_before_execution(
    legacy_artifacts: dict[str, Path], monkeypatch: pytest.MonkeyPatch
) -> None:
    request = _request(legacy_artifacts)
    legacy_artifacts["wheel.nirs4all"].write_bytes(b"tampered")
    monkeypatch.setattr(python_legacy_adapter, "run_bounded", lambda *_args, **_kwargs: pytest.fail("runtime must not start"))

    with pytest.raises(ValueError, match="SHA-256 changed"):
        adapt(request)


def test_python_oracle_refuses_source_checkout_artifact(
    legacy_artifacts: dict[str, Path], tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    checkout = tmp_path / "nirs4all-source-checkout.py"
    checkout.write_text("raise RuntimeError('must never import')\n", encoding="utf-8")
    request = _request(legacy_artifacts)
    request.artifacts.append(
        request.artifacts[0].model_copy(
            update={"role": "source_checkout", "path": checkout, "sha256": _sha256(checkout)}
        )
    )
    monkeypatch.setattr(python_legacy_adapter, "run_bounded", lambda *_args, **_kwargs: pytest.fail("runtime must not start"))

    with pytest.raises(ValueError, match="exactly match the closed wheelhouse manifest"):
        adapt(request)


def _site(paths: dict[str, Path]) -> Path:
    runtime = paths["runtime_python"].parent.parent
    return runtime / ("Lib/site-packages" if sys.platform == "win32" else "lib/python3.11/site-packages")


@pytest.mark.parametrize(
    ("relative", "payload", "message"),
    [
        ("ambient.py", b"raise RuntimeError\n", "undeclared member"),
        ("ambient.pth", b"/unselected/source\n", "forbidden ambient member"),
        ("package/__pycache__/ambient.cpython-311.pyc", b"pyc", "forbidden ambient member"),
    ],
)
def test_python_oracle_refuses_ambient_runtime_members_before_execution(
    legacy_artifacts: dict[str, Path],
    monkeypatch: pytest.MonkeyPatch,
    relative: str,
    payload: bytes,
    message: str,
) -> None:
    path = _site(legacy_artifacts) / relative
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(payload)
    monkeypatch.setattr(python_legacy_adapter, "run_bounded", lambda *_args, **_kwargs: pytest.fail("runtime must not start"))

    with pytest.raises(ValueError, match=message):
        adapt(_request(legacy_artifacts))


@pytest.mark.skipif(sys.platform == "win32", reason="symlink creation is not unprivileged on Windows")
@pytest.mark.parametrize("kind", ["file", "directory"])
def test_python_oracle_refuses_runtime_symlinks_before_execution(
    legacy_artifacts: dict[str, Path], monkeypatch: pytest.MonkeyPatch, kind: str
) -> None:
    site_packages = _site(legacy_artifacts)
    if kind == "file":
        (site_packages / "ambient-link").symlink_to(site_packages / "nirs4all/__init__.py")
    else:
        (site_packages / "ambient-dir").symlink_to(site_packages / "nirs4all", target_is_directory=True)
    monkeypatch.setattr(python_legacy_adapter, "run_bounded", lambda *_args, **_kwargs: pytest.fail("runtime must not start"))

    with pytest.raises(ValueError, match="symlink"):
        adapt(_request(legacy_artifacts))


@pytest.mark.skipif(sys.platform == "win32", reason="the POSIX runtime layout is not used on Windows")
def test_python_oracle_refuses_symlink_in_active_site_path(
    legacy_artifacts: dict[str, Path], monkeypatch: pytest.MonkeyPatch
) -> None:
    runtime_root = legacy_artifacts["runtime_python"].parent.parent
    library = runtime_root / "lib"
    real_library = runtime_root / "real-lib"
    library.rename(real_library)
    library.symlink_to(real_library, target_is_directory=True)
    monkeypatch.setattr(python_legacy_adapter, "run_bounded", lambda *_args, **_kwargs: pytest.fail("runtime must not start"))

    with pytest.raises(ValueError, match="symlink or non-directory ancestor"):
        adapt(_request(legacy_artifacts))


def test_python_oracle_refuses_installed_member_tamper_before_execution(
    legacy_artifacts: dict[str, Path], monkeypatch: pytest.MonkeyPatch
) -> None:
    (_site(legacy_artifacts) / "nirs4all/__init__.py").write_bytes(b"tampered\n")
    monkeypatch.setattr(python_legacy_adapter, "run_bounded", lambda *_args, **_kwargs: pytest.fail("runtime must not start"))

    with pytest.raises(ValueError, match="differs from wheel RECORD"):
        adapt(_request(legacy_artifacts))


def test_python_oracle_refuses_path_digest_swap_before_execution(
    legacy_artifacts: dict[str, Path], monkeypatch: pytest.MonkeyPatch
) -> None:
    site_packages = _site(legacy_artifacts)
    metadata = site_packages / "nirs4all-0.10.3.dist-info/METADATA"
    wheel = site_packages / "nirs4all-0.10.3.dist-info/WHEEL"
    metadata_payload = metadata.read_bytes()
    metadata.write_bytes(wheel.read_bytes())
    wheel.write_bytes(metadata_payload)
    monkeypatch.setattr(python_legacy_adapter, "run_bounded", lambda *_args, **_kwargs: pytest.fail("runtime must not start"))

    with pytest.raises(ValueError, match="differs from wheel RECORD"):
        adapt(_request(legacy_artifacts))


def test_python_oracle_refuses_inter_wheel_path_collision(legacy_artifacts: dict[str, Path]) -> None:
    wheel = legacy_artifacts["wheel.nirs4all"]
    contract = _wheel_contract(wheel, "nirs4all")

    with pytest.raises(ValueError, match="overlaps at installed member"):
        _verify_runtime_tree(legacy_artifacts["runtime_python"], [contract, contract])


def test_python_oracle_refuses_runtime_mutation_after_child(
    legacy_artifacts: dict[str, Path], monkeypatch: pytest.MonkeyPatch
) -> None:
    def mutate(*args: Any, **kwargs: Any) -> subprocess.CompletedProcess[str]:
        (_site(legacy_artifacts) / "post-run-extra.py").write_bytes(b"ambient\n")
        return _completed_process(*args, **kwargs)

    monkeypatch.setattr(python_legacy_adapter, "run_bounded", mutate)

    with pytest.raises(ValueError, match="undeclared member"):
        adapt(_request(legacy_artifacts))
