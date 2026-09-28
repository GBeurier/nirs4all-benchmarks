#!/usr/bin/env python3
"""Build an autonomous, content-addressed Web/WASM qualification stage."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import shlex
import shutil
import subprocess
import tarfile
from email.parser import Parser
from pathlib import Path, PurePosixPath
from zipfile import ZipFile

PYTHON_DISTRIBUTIONS = {
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


def _git(repo: Path, *args: str) -> str:
    return subprocess.run(
        ["git", "-C", str(repo), *args],
        check=True,
        capture_output=True,
        text=True,
    ).stdout.strip()


def _normal_name(value: str) -> str:
    return value.lower().replace("_", "-").replace(".", "-")


def _wheel_identity(path: Path) -> tuple[str, str]:
    with ZipFile(path) as wheel:
        metadata_names = [name for name in wheel.namelist() if name.endswith(".dist-info/METADATA")]
        if len(metadata_names) != 1:
            raise ValueError(f"wheel {path.name!r} does not contain exactly one METADATA")
        metadata = Parser().parsestr(wheel.read(metadata_names[0]).decode("utf-8"))
    name = metadata["Name"]
    version = metadata["Version"]
    if not name or not version:
        raise ValueError(f"wheel {path.name!r} has incomplete identity metadata")
    return _normal_name(name), version


def _copy(source: Path, destination: Path) -> Path:
    source = source.resolve()
    if not source.is_file():
        raise ValueError(f"missing stage input: {source}")
    destination.parent.mkdir(parents=True, exist_ok=True)
    shutil.copyfile(source, destination)
    if _sha256(source) != _sha256(destination):
        raise ValueError(f"copy changed bytes for {source}")
    return destination


def _snapshot_file(snapshot: tarfile.TarFile, member_name: str, destination: Path) -> Path:
    try:
        member = snapshot.getmember(member_name)
        handle = snapshot.extractfile(member) if member.isfile() else None
    except (KeyError, tarfile.TarError) as exc:
        raise ValueError(f"source snapshot member is missing: {member_name}") from exc
    if handle is None:
        raise ValueError(f"source snapshot member is not a regular file: {member_name}")
    destination.parent.mkdir(parents=True, exist_ok=True)
    destination.write_bytes(handle.read())
    return destination


def _closure_item(
    role: str,
    source: Path,
    destination: str,
    *,
    snapshot_member: str | None,
) -> dict[str, str | None]:
    return {
        "role": role,
        "path": str(source.resolve()),
        "destination": destination,
        "sha256": _sha256(source),
        "snapshot_member": snapshot_member,
    }


_INVENTORY_SCRIPT = r'''
import hashlib, importlib.metadata, json, site, sys
from pathlib import Path

prefix = Path(sys.prefix).resolve()
if sys.flags.isolated != 1 or site.ENABLE_USER_SITE is not False:
    raise RuntimeError("inventory interpreter is not isolated")

def below(path, root):
    try:
        path.relative_to(root)
        return True
    except ValueError:
        return False

def sha256(path):
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()

result = {}
for name in json.loads(sys.stdin.read()):
    distribution = importlib.metadata.distribution(name)
    files = []
    for item in distribution.files or []:
        installed = Path(distribution.locate_file(item)).resolve()
        if installed.suffix == ".pyc" or not installed.is_file():
            continue
        if not below(installed, prefix):
            raise RuntimeError("installed distribution escapes prefix: %s" % name)
        files.append({"path": installed.relative_to(prefix).as_posix(), "sha256": sha256(installed)})
    if not files:
        raise RuntimeError("installed distribution has no files: %s" % name)
    result[name] = {"version": distribution.version, "files": sorted(files, key=lambda item: item["path"])}
json.dump(result, sys.stdout, sort_keys=True)
'''


def _python_stage(
    stage_root: Path,
    adapter_python: Path,
    adapter_wheel: Path,
    dependency_wheels: list[Path],
) -> tuple[Path, Path, list[dict[str, object]]]:
    wheelhouse = stage_root / "python-wheelhouse"
    wheelhouse.mkdir(parents=True)
    source_wheels = [adapter_wheel, *dependency_wheels]
    staged_by_name: dict[str, tuple[Path, str]] = {}
    for source in source_wheels:
        destination = _copy(source, wheelhouse / source.name)
        name, version = _wheel_identity(destination)
        if name in staged_by_name:
            raise ValueError(f"duplicate Python distribution wheel: {name}")
        staged_by_name[name] = (destination, version)
    if set(staged_by_name) != PYTHON_DISTRIBUTIONS:
        raise ValueError(
            f"Python wheel closure must be exactly {sorted(PYTHON_DISTRIBUTIONS)}; "
            f"received {sorted(staged_by_name)}"
        )

    prefix = stage_root / "python-prefix"
    subprocess.run(
        [str(adapter_python.resolve()), "-I", "-m", "venv", "--copies", str(prefix)],
        check=True,
        env={},
    )
    runtime_python = prefix / ("Scripts/python.exe" if os.name == "nt" else "bin/python")
    subprocess.run(
        [
            str(runtime_python),
            "-I",
            "-m",
            "pip",
            "install",
            "--no-index",
            "--no-deps",
            "--disable-pip-version-check",
            *[str(staged_by_name[name][0]) for name in sorted(staged_by_name)],
        ],
        check=True,
        env={},
    )
    inventory_process = subprocess.run(
        [str(runtime_python), "-I", "-c", _INVENTORY_SCRIPT],
        input=json.dumps(sorted(PYTHON_DISTRIBUTIONS)),
        capture_output=True,
        text=True,
        check=True,
        env={},
    )
    installed = json.loads(inventory_process.stdout)
    distributions: list[dict[str, object]] = []
    for name in sorted(PYTHON_DISTRIBUTIONS):
        wheel, version = staged_by_name[name]
        if installed[name]["version"] != version:
            raise ValueError(f"installed Python version differs from wheel for {name}")
        distributions.append(
            {
                "name": name,
                "version": version,
                "wheel_path": str(wheel.resolve()),
                "wheel_sha256": _sha256(wheel),
                "files": installed[name]["files"],
            }
        )
    runtime_manifest = stage_root / "manifests/python-runtime-closure.v1.json"
    runtime_manifest.parent.mkdir(parents=True, exist_ok=True)
    runtime_contract = {
        "schema_version": "nirs4all.web-wasm-python-runtime.v1",
        "stage_root": str(stage_root),
        "prefix": str(prefix),
        "runtime_python_sha256": _sha256(runtime_python),
        "distributions": distributions,
    }
    runtime_manifest.write_text(
        f"{json.dumps(runtime_contract, indent=2)}\n",
        encoding="utf-8",
    )
    return runtime_python, runtime_manifest, distributions


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--web-repo", type=Path, required=True)
    parser.add_argument("--web-commit", required=True)
    parser.add_argument("--core-commit", required=True)
    parser.add_argument("--methods-package-json", type=Path, required=True)
    parser.add_argument("--yaml-package-root", type=Path, required=True)
    parser.add_argument("--scenario", type=Path, required=True)
    parser.add_argument("--strict-profile", type=Path, required=True)
    parser.add_argument("--node-runtime", type=Path, required=True)
    parser.add_argument("--adapter-python", type=Path, required=True)
    parser.add_argument("--adapter-wheel", type=Path, required=True)
    parser.add_argument("--dependency-wheel", type=Path, action="append", default=[])
    parser.add_argument("--output-dir", type=Path, required=True)
    args = parser.parse_args()

    web_repo = args.web_repo.resolve()
    if _git(web_repo, "rev-parse", "HEAD") != args.web_commit:
        raise ValueError("Web checkout HEAD does not match --web-commit")
    if _git(web_repo, "status", "--porcelain=v1"):
        raise ValueError("Web checkout must be clean before staging evidence")
    stage_root = args.output_dir.resolve()
    if stage_root.exists() and any(stage_root.iterdir()):
        raise ValueError("output directory must be absent or empty")
    stage_root.mkdir(parents=True, exist_ok=True)
    artifacts = stage_root / "artifacts"
    manifests = stage_root / "manifests"
    closure_sources = stage_root / "closure-sources"
    bin_dir = stage_root / "bin"
    for directory in (artifacts, manifests, closure_sources, bin_dir):
        directory.mkdir(parents=True, exist_ok=True)

    source_snapshot = artifacts / f"nirs4all-web-{args.web_commit}.tar"
    subprocess.run(
        ["git", "-C", str(web_repo), "archive", "--format=tar", f"--output={source_snapshot}", args.web_commit],
        check=True,
    )
    scenario = _copy(args.scenario, artifacts / "perf001-scenario.v1.json")
    profile = _copy(args.strict_profile, artifacts / "nirs4all-web-profile.v1.json")
    runtime_node = _copy(args.node_runtime, bin_dir / "node")
    runtime_node.chmod(0o755)

    source_items = [
        ("aggregate_package", "web-app/vendor/nirs4all/package.json", "node_modules/nirs4all/package.json"),
        ("core_provenance", "web-app/vendor/nirs4all/PROVENANCE.md", "node_modules/nirs4all/PROVENANCE.md"),
        ("aggregate_index", "web-app/vendor/nirs4all/src/index.js", "node_modules/nirs4all/src/index.js"),
        ("runtime_dependency", "web-app/vendor/nirs4all/src/execution.js", "node_modules/nirs4all/src/execution.js"),
        (
            "aggregate_archive_v2",
            "web-app/vendor/nirs4all/src/archive-v2.js",
            "node_modules/nirs4all/src/archive-v2.js",
        ),
        ("native_package", "web-app/vendor/nirs4all/native/package.json", "node_modules/nirs4all/native/package.json"),
        (
            "native_loader",
            "web-app/vendor/nirs4all/native/nirs4all_core_wasm_native.js",
            "node_modules/nirs4all/native/nirs4all_core_wasm_native.js",
        ),
        (
            "core_native_wasm",
            "web-app/vendor/nirs4all/native/nirs4all_core_wasm_native_bg.wasm",
            "node_modules/nirs4all/native/nirs4all_core_wasm_native_bg.wasm",
        ),
    ]
    method_members = _git(
        web_repo,
        "ls-tree",
        "-r",
        "--name-only",
        args.web_commit,
        "web-app/src/engine/wasm/methods",
    ).splitlines()
    for member in method_members:
        name = PurePosixPath(member).name
        if not (name.endswith(".js") or name == "n4m.wasm"):
            continue
        role = "methods_index" if name == "index.js" else "methods_wasm" if name == "n4m.wasm" else "runtime_dependency"
        source_items.append((role, member, f"node_modules/@nirs4all/methods/dist/{name}"))

    closure: list[dict[str, str | None]] = []
    with tarfile.open(source_snapshot, "r:") as snapshot:
        web_package_path = _snapshot_file(snapshot, "web-app/package.json", artifacts / "web-package.json")
        archive_member = "web-app/src/engine/fixtures/archive-v2/multitarget-pls.n4a"
        archive_v2 = _snapshot_file(snapshot, archive_member, artifacts / "multitarget-pls.n4a")
        for role, member, destination in source_items:
            staged = _snapshot_file(snapshot, member, closure_sources / member)
            closure.append(_closure_item(role, staged, destination, snapshot_member=member))

    methods_package = _copy(args.methods_package_json, closure_sources / "external/methods-package.json")
    closure.append(
        _closure_item(
            "methods_package",
            methods_package,
            "node_modules/@nirs4all/methods/package.json",
            snapshot_member=None,
        )
    )
    yaml_root = args.yaml_package_root.resolve()
    yaml_package = _copy(yaml_root / "package.json", closure_sources / "external/yaml/package.json")
    closure.append(_closure_item("yaml_package", yaml_package, "node_modules/yaml/package.json", snapshot_member=None))
    for source in sorted((yaml_root / "dist").rglob("*.js")):
        relative = source.relative_to(yaml_root).as_posix()
        staged = _copy(source, closure_sources / f"external/yaml/{relative}")
        closure.append(
            _closure_item("runtime_dependency", staged, f"node_modules/yaml/{relative}", snapshot_member=None)
        )

    web_package = json.loads(web_package_path.read_text(encoding="utf-8"))
    aggregate_package = json.loads(
        (closure_sources / "web-app/vendor/nirs4all/package.json").read_text(encoding="utf-8")
    )
    runtime_closure = manifests / "web-wasm-runtime-closure.v1.json"
    web_runtime_contract = {
        "schema_version": "nirs4all.web-wasm-runtime-closure.v1",
        "web_commit_sha": args.web_commit,
        "web_version": web_package["version"],
        "core_commit_sha": args.core_commit,
        "aggregate_version": aggregate_package["version"],
        "source_snapshot_sha256": _sha256(source_snapshot),
        "entrypoint": "node_modules/nirs4all/src/index.js",
        "files": closure,
    }
    runtime_closure.write_text(
        f"{json.dumps(web_runtime_contract, indent=2)}\n",
        encoding="utf-8",
    )

    runtime_python, python_manifest, _distributions = _python_stage(
        stage_root,
        args.adapter_python,
        args.adapter_wheel,
        args.dependency_wheel,
    )
    launcher = bin_dir / "n4a-gate-web-wasm"
    launcher.write_text(
        f"#!/bin/sh\nexec {shlex.quote(str(runtime_python))} -I -m "
        "nirs4all_benchmarks.qualification.web_wasm_adapter\n",
        encoding="utf-8",
    )
    launcher.chmod(0o755)
    artifact_paths = {
        "runtime_python": runtime_python,
        "python_runtime_closure": python_manifest,
        "runtime_node": runtime_node,
        "web_runtime_closure": runtime_closure,
        "web_source_snapshot": source_snapshot,
        "strict_profile": profile,
        "archive_v2": archive_v2,
    }
    component = {
        "component": "web_wasm",
        "adapter": "stdio-json-v1",
        "executable": {"path": str(launcher), "sha256": _sha256(launcher)},
        "artifacts": [
            {"role": role, "path": str(path), "sha256": _sha256(path)}
            for role, path in artifact_paths.items()
        ],
        "version": web_package["version"],
        "commit_sha": args.web_commit,
        "timeout_seconds": 300,
    }
    component_path = manifests / "web-wasm-component.v1.json"
    component_path.write_text(f"{json.dumps(component, indent=2)}\n", encoding="utf-8")
    print(
        json.dumps(
            {
                "stage_root": str(stage_root),
                "scenario": str(scenario),
                "scenario_sha256": _sha256(scenario),
                "component": str(component_path),
                "component_sha256": _sha256(component_path),
                "launcher_sha256": _sha256(launcher),
                "runtime_closure_sha256": _sha256(runtime_closure),
                "python_runtime_sha256": _sha256(python_manifest),
            },
            sort_keys=True,
        )
    )


if __name__ == "__main__":
    main()
