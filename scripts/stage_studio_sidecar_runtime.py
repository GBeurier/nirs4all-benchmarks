#!/usr/bin/env python3
"""Build an autonomous Linux Studio native Archive V2 qualification stage."""

from __future__ import annotations

import argparse
import hashlib
import http.client
import json
import re
import selectors
import shlex
import shutil
import stat
import subprocess
import tarfile
import tempfile
from email.parser import Parser
from pathlib import Path
from zipfile import ZipFile

STUDIO_COMMIT = "6b0545771b7bb14e127ca4842d1a56e9b007ee42"
CORE_COMMIT = "bc001e53f41cee6eb5e6f1bf6b363db25cf750ee"
IO_COMMIT = "f41967d53b355b10951b5658af1dddb6bd926e3d"
STUDIO_SOURCE_SHA256 = "a49af896955b70de0ae25cdbe8d347dfd11de71a067981e70b52b16a045413f3"
SIDECAR_BINARY_SHA256 = "40b4aaac3f597ffba0680cdf0caf133250a65d0159fc5e6c4f191720c31e4ac1"
PACKAGED_CONTRACT_SHA256 = "23b6b7830aae3b0e485a5e723168abb71aa7fdb480b062a07c926e70d1829c7c"
METHODS_LIBRARY_SHA256 = "847f3b23986d9f74d392169bf559c49a38ca915492286e61ada10c31f72bfdf0"
METHODS_VERSION = "1.0.10+abi.2.2.0"
ARCHIVE_SHA256 = "994252030ff80129d0431995bae53eb473082f05825b65714379262b72af13fa"
SCENARIO_SHA256 = "a25ae04d4a60198f1bd3f8423533991c3b09943e2cd3f33d3d6441f1b84c1f51"
MAX_FILE = 512 * 1024 * 1024
MAX_TOTAL = 2 * 1024 * 1024 * 1024
CRITICAL_MEMBERS = [
    "package.json",
    "scripts/build-native-sidecar.cjs",
    "scripts/native-runtime-contract.cjs",
    "sidecar/Cargo.toml",
    "sidecar/Cargo.lock",
    "sidecar/src/main.rs",
    "sidecar/src/lib.rs",
    "sidecar/src/archive_v2_prediction.rs",
    "sidecar/src/settings.rs",
    "sidecar/contracts/studio_archive_v2_prediction_v1.json",
    "sidecar/vendor/nirs4all-core-bc001e5/INVENTORY.sha256",
    "sidecar/vendor/nirs4all-core-bc001e5/PROVENANCE.json",
    "sidecar/vendor/nirs4all-io-f419/INVENTORY.sha256",
    "sidecar/vendor/nirs4all-io-f419/PROVENANCE.json",
]


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _regular(path: Path, label: str) -> int:
    metadata = path.lstat()
    if path.is_symlink() or not stat.S_ISREG(metadata.st_mode) or not 0 < metadata.st_size <= MAX_FILE:
        raise ValueError(f"{label} must be a bounded non-symlink regular file")
    return metadata.st_size


def _copy(source: Path, destination: Path) -> Path:
    source = source.resolve()
    _regular(source, str(source))
    destination.parent.mkdir(parents=True, exist_ok=True)
    shutil.copyfile(source, destination)
    if _sha256(source) != _sha256(destination):
        raise ValueError(f"copy changed bytes for {source}")
    return destination


def _normal_name(value: str) -> str:
    return value.lower().replace("_", "-").replace(".", "-")


def _wheel_identity(path: Path) -> tuple[str, str]:
    with ZipFile(path) as wheel:
        metadata_names = [name for name in wheel.namelist() if name.endswith(".dist-info/METADATA")]
        if len(metadata_names) != 1:
            raise ValueError(f"wheel {path.name!r} must contain one METADATA")
        metadata = Parser().parsestr(wheel.read(metadata_names[0]).decode())
    if not metadata["Name"] or not metadata["Version"]:
        raise ValueError(f"wheel {path.name!r} has incomplete identity")
    return _normal_name(metadata["Name"]), metadata["Version"]


_INVENTORY = r"""
import hashlib, importlib.metadata, json, site, sys
from pathlib import Path
prefix = Path(sys.prefix).resolve()
if sys.flags.isolated != 1 or site.ENABLE_USER_SITE is not False:
    raise RuntimeError("adapter inventory interpreter is not isolated")
def sha256(path):
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()
result = {}
for distribution in importlib.metadata.distributions():
    name = distribution.metadata["Name"].lower().replace("_", "-").replace(".", "-")
    files = []
    for item in distribution.files or []:
        installed = Path(distribution.locate_file(item)).resolve()
        if installed.suffix == ".pyc" or not installed.is_file():
            continue
        relative = installed.relative_to(prefix)
        files.append({"path": relative.as_posix(), "sha256": sha256(installed)})
    if name in result or not files:
        raise RuntimeError("invalid installed distribution: %s" % name)
    result[name] = {"version": distribution.version, "files": sorted(files, key=lambda item: item["path"])}
json.dump(result, sys.stdout, sort_keys=True)
"""


def _adapter_stage(
    stage: Path, adapter_python: Path, adapter_wheel: Path, dependency_wheelhouse: Path
) -> tuple[Path, Path]:
    wheelhouse = stage / "adapter-wheelhouse"
    wheelhouse.mkdir()
    selected: dict[str, tuple[Path, str]] = {}
    total = 0
    for source in [*sorted(dependency_wheelhouse.resolve().glob("*.whl")), adapter_wheel.resolve()]:
        total += _regular(source, f"wheel {source.name}")
        if total > MAX_TOTAL:
            raise ValueError("adapter wheel closure exceeds total-size bound")
        name, version = _wheel_identity(source)
        if name == "nirs4all" or (name == "nirs4all-benchmarks" and source != adapter_wheel.resolve()):
            continue
        if name in selected:
            raise ValueError(f"duplicate adapter wheel distribution: {name}")
        selected[name] = (_copy(source, wheelhouse / source.name), version)
    if "nirs4all-benchmarks" not in selected or "pydantic" not in selected:
        raise ValueError("adapter wheel closure omits benchmark harness or pydantic")
    prefix = stage / "adapter-prefix"
    subprocess.run([str(adapter_python.resolve()), "-I", "-m", "venv", "--copies", str(prefix)], check=True, env={})
    runtime = prefix / "bin/python"
    subprocess.run(
        [
            str(runtime),
            "-I",
            "-m",
            "pip",
            "install",
            "--no-index",
            "--disable-pip-version-check",
            "--no-deps",
            *[str(value[0]) for value in selected.values()],
        ],
        check=True,
        env={},
    )
    subprocess.run(
        [str(runtime), "-I", "-m", "pip", "uninstall", "-y", "pip", "setuptools"],
        check=True,
        env={},
        capture_output=True,
    )
    process = subprocess.run([str(runtime), "-I", "-c", _INVENTORY], check=True, env={}, capture_output=True, text=True)
    installed = json.loads(process.stdout)
    if set(installed) != set(selected):
        raise ValueError(f"installed adapter closure differs: installed={sorted(installed)}, wheels={sorted(selected)}")
    distributions = [
        {"name": name, "version": version, "files": installed[name]["files"]}
        for name, (_wheel, version) in sorted(selected.items())
    ]
    manifest = stage / "manifests/adapter-runtime-closure.v1.json"
    manifest.parent.mkdir(parents=True)
    manifest.write_text(
        json.dumps(
            {
                "schema_version": "nirs4all.studio-gate-adapter-runtime.v1",
                "stage_root": str(stage),
                "prefix": str(prefix),
                "runtime_python_sha256": _sha256(runtime),
                "distributions": distributions,
            },
            indent=2,
        )
        + "\n",
        encoding="utf-8",
    )
    return runtime, manifest


def _tree_digest(entries: list[tuple[str, str]]) -> str:
    digest = hashlib.sha256()
    for name, member_digest in sorted(entries):
        digest.update(f"{name}\0{member_digest}\n".encode())
    return digest.hexdigest()


def _studio_manifest(
    stage: Path, snapshot: Path, sidecar: Path, contract: Path, methods: Path, archive_v2: Path, version: str
) -> Path:
    source_members = []
    core_entries = []
    core_provenance: dict[str, object] | None = None
    io_provenance: dict[str, object] | None = None
    with tarfile.open(snapshot, "r:") as source:
        members = source.getmembers()
        if source.pax_headers.get("comment") != STUDIO_COMMIT or len(members) > 4096:
            raise ValueError("Studio snapshot does not bind selected commit or exceeds bounds")
        if sum(member.size for member in members) > 64 * 1024 * 1024:
            raise ValueError("Studio snapshot exceeds content bound")
        for name in CRITICAL_MEMBERS:
            handle = source.extractfile(source.getmember(name))
            if handle is None:
                raise ValueError(f"critical Studio member missing: {name}")
            payload = handle.read()
            source_members.append({"path": name, "sha256": hashlib.sha256(payload).hexdigest()})
            if name == "sidecar/vendor/nirs4all-core-bc001e5/PROVENANCE.json":
                core_provenance = json.loads(payload)
            elif name == "sidecar/vendor/nirs4all-io-f419/PROVENANCE.json":
                io_provenance = json.loads(payload)
        for member in members:
            if member.isfile() and member.name.startswith("sidecar/vendor/nirs4all-core-bc001e5/"):
                handle = source.extractfile(member)
                if handle is None:
                    raise ValueError("vendored Core member unreadable")
                core_entries.append((member.name, hashlib.sha256(handle.read()).hexdigest()))
    if core_provenance is None or core_provenance.get("commit") != CORE_COMMIT:
        raise ValueError("Studio snapshot does not bind selected Core commit")
    if io_provenance is None or io_provenance.get("commit") != IO_COMMIT:
        raise ValueError("Studio snapshot does not bind selected IO commit")
    manifest = stage / "manifests/studio-runtime-closure.v2.json"
    manifest.write_text(
        json.dumps(
            {
                "schema_version": "nirs4all.studio-gate-runtime.v2",
                "platform": "linux",
                "arch": "x86_64",
                "studio_commit_sha": STUDIO_COMMIT,
                "studio_version": version,
                "core_commit_sha": CORE_COMMIT,
                "io_commit_sha": IO_COMMIT,
                "sidecar_binary_sha256": _sha256(sidecar),
                "source_snapshot_sha256": _sha256(snapshot),
                "packaged_runtime_contract_sha256": _sha256(contract),
                "methods_library_sha256": _sha256(methods),
                "methods_library_size": methods.stat().st_size,
                "methods_version": METHODS_VERSION,
                "methods_needed": ["libc.so.6", "libgcc_s.so.1", "libm.so.6", "libstdc++.so.6"],
                "methods_required_glibc": "GLIBC_2.35",
                "methods_required_glibcxx": "GLIBCXX_3.4.29",
                "portability_claim": "local-linux-x86_64-only-not-manylinux",
                "archive_v2_sha256": _sha256(archive_v2),
                "source_members": source_members,
                "vendored_core_member_count": len(core_entries),
                "vendored_core_tree_sha256": _tree_digest(core_entries),
            },
            indent=2,
        )
        + "\n",
        encoding="utf-8",
    )
    return manifest


def _verify_methods_dynamic_closure(methods: Path) -> None:
    dynamic = subprocess.run(["readelf", "-d", str(methods)], check=True, capture_output=True, text=True).stdout
    if "(RPATH)" in dynamic or "(RUNPATH)" in dynamic:
        raise ValueError("selected Methods library must not contain RPATH or RUNPATH")
    needed = sorted(set(re.findall(r"Shared library: \[([^]]+)\]", dynamic)))
    allowed = ["libc.so.6", "libgcc_s.so.1", "libm.so.6", "libstdc++.so.6"]
    if needed != allowed:
        raise ValueError(f"selected Methods library has dependencies outside the system allowlist: {needed}")
    versions = subprocess.run(
        ["readelf", "--version-info", str(methods)], check=True, capture_output=True, text=True
    ).stdout
    glibc = {(int(major), int(minor)) for major, minor in re.findall(r"GLIBC_(\d+)\.(\d+)", versions)}
    glibcxx = {
        (int(major), int(minor), int(patch))
        for major, minor, patch in re.findall(r"GLIBCXX_(\d+)\.(\d+)\.(\d+)", versions)
    }
    if max(glibc, default=(0, 0)) != (2, 35) or max(glibcxx, default=(0, 0, 0)) != (3, 4, 29):
        raise ValueError("selected Methods library platform requirements differ from the local Linux claim")
    embedded = subprocess.run(["strings", str(methods)], check=True, capture_output=True, text=True).stdout.splitlines()
    if METHODS_VERSION not in embedded:
        raise ValueError("selected Methods library does not embed the declared version and ABI")


def _preflight_native_archive_v2(sidecar: Path) -> None:
    with tempfile.TemporaryDirectory(prefix="n4a-studio-stage-preflight-") as root_text:
        root = Path(root_text)
        scratch = root / "tmp"
        config = root / "config"
        scratch.mkdir()
        config.mkdir()
        process = subprocess.Popen(
            [str(sidecar), "--host", "127.0.0.1", "--port", "0"],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            cwd=root,
            env={
                "LANG": "C.UTF-8",
                "HOME": str(root),
                "TMPDIR": str(scratch),
                "TMP": str(scratch),
                "TEMP": str(scratch),
                "NIRS4ALL_CONFIG": str(config),
                "NIRS4ALL_RUNTIME_MODE": "release",
                "NIRS4ALL_RUNTIME_KIND": "rust_sidecar",
            },
        )
        try:
            if process.stdout is None:
                raise ValueError("staged sidecar stdout is unavailable")
            selector = selectors.DefaultSelector()
            selector.register(process.stdout, selectors.EVENT_READ)
            if not selector.select(timeout=15):
                raise ValueError("staged sidecar did not publish readiness within 15 seconds")
            line = process.stdout.readline()
            prefix = "STUDIO_SIDECAR_READY "
            if not line.startswith(prefix):
                stderr = "" if process.stderr is None else process.stderr.read(4096).strip()
                raise ValueError(f"staged sidecar failed readiness: {stderr}")
            ready = json.loads(line.removeprefix(prefix))
            connection = http.client.HTTPConnection("127.0.0.1", ready["port"], timeout=5)
            try:
                connection.request("GET", "/sidecar/v1/capabilities")
                response = connection.getresponse()
                capabilities = json.loads(response.read(1024 * 1024))
            finally:
                connection.close()
            if (
                response.status != 200
                or capabilities.get("features", {}).get("native_archive_v2_prediction") is not True
            ):
                unresolved = subprocess.run(
                    ["ldd", str(sidecar.parent / "libn4m.so")],
                    check=False,
                    capture_output=True,
                    text=True,
                ).stdout
                missing = [line.strip() for line in unresolved.splitlines() if "not found" in line]
                detail = "; ".join(missing) or "Core preflight rejected the packaged runtime"
                raise ValueError(f"staged native Archive V2 capability is unavailable: {detail}")
        finally:
            process.terminate()
            try:
                process.wait(timeout=5)
            except subprocess.TimeoutExpired:
                process.kill()
                process.wait(timeout=5)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--studio-source-snapshot", type=Path, required=True)
    parser.add_argument("--studio-commit", default=STUDIO_COMMIT)
    parser.add_argument("--studio-version", required=True)
    parser.add_argument("--sidecar-binary", type=Path, required=True)
    parser.add_argument("--packaged-runtime-contract", type=Path, required=True)
    parser.add_argument("--methods-library", type=Path, required=True)
    parser.add_argument("--archive-v2", type=Path, required=True)
    parser.add_argument("--adapter-python", type=Path, required=True)
    parser.add_argument("--adapter-wheel", type=Path, required=True)
    parser.add_argument("--dependency-wheelhouse", type=Path, required=True)
    parser.add_argument("--scenario", type=Path, required=True)
    parser.add_argument("--output-dir", type=Path, required=True)
    args = parser.parse_args()
    identities = {
        "studio commit": (args.studio_commit, STUDIO_COMMIT),
        "source snapshot": (_sha256(args.studio_source_snapshot), STUDIO_SOURCE_SHA256),
        "sidecar": (_sha256(args.sidecar_binary), SIDECAR_BINARY_SHA256),
        "packaged contract": (_sha256(args.packaged_runtime_contract), PACKAGED_CONTRACT_SHA256),
        "Methods library": (_sha256(args.methods_library), METHODS_LIBRARY_SHA256),
        "Archive V2": (_sha256(args.archive_v2), ARCHIVE_SHA256),
        "scenario": (_sha256(args.scenario), SCENARIO_SHA256),
    }
    for label, (actual, expected) in identities.items():
        if actual != expected:
            raise ValueError(f"{label} does not match selected identity")
    stage = args.output_dir.resolve()
    if stage.exists() and any(stage.iterdir()):
        raise ValueError("output directory must be absent or empty")
    stage.mkdir(parents=True, exist_ok=True)
    artifacts = stage / "artifacts"
    native = stage / "backend-dist/native"
    bin_dir = stage / "bin"
    for directory in (artifacts, native, bin_dir):
        directory.mkdir(parents=True, exist_ok=True)
    snapshot = _copy(args.studio_source_snapshot, artifacts / f"nirs4all-studio-{STUDIO_COMMIT}.tar")
    scenario = _copy(args.scenario, artifacts / "perf001-scenario.v1.json")
    archive_v2 = _copy(args.archive_v2, artifacts / "perf001-multitarget.n4a")
    sidecar = _copy(args.sidecar_binary, native / "studio-sidecar")
    sidecar.chmod(0o755)
    contract = _copy(args.packaged_runtime_contract, native / "STUDIO_RUNTIME_CONTRACT.json")
    methods = _copy(args.methods_library, native / "libn4m.so")
    _verify_methods_dynamic_closure(methods)
    _preflight_native_archive_v2(sidecar)
    runtime_python, adapter_manifest = _adapter_stage(
        stage, args.adapter_python, args.adapter_wheel, args.dependency_wheelhouse
    )
    studio_manifest = _studio_manifest(stage, snapshot, sidecar, contract, methods, archive_v2, args.studio_version)
    launcher = bin_dir / "n4a-gate-studio-sidecar"
    launcher.write_text(
        f"#!/bin/sh\nexec {shlex.quote(str(runtime_python))} -I -m "
        "nirs4all_benchmarks.qualification.studio_sidecar_adapter\n",
        encoding="utf-8",
    )
    launcher.chmod(0o755)
    artifact_paths = {
        "runtime_python": runtime_python,
        "adapter_runtime_closure": adapter_manifest,
        "runtime_sidecar": sidecar,
        "studio_runtime_closure": studio_manifest,
        "studio_source_snapshot": snapshot,
        "studio_packaged_runtime_contract": contract,
        "methods_library": methods,
        "archive_v2": archive_v2,
    }
    component = {
        "component": "studio_sidecar",
        "adapter": "stdio-json-v1",
        "executable": {"path": str(launcher), "sha256": _sha256(launcher)},
        "artifacts": [
            {"role": role, "path": str(path), "sha256": _sha256(path)} for role, path in artifact_paths.items()
        ],
        "version": args.studio_version,
        "commit_sha": args.studio_commit,
        "timeout_seconds": 120,
    }
    component_path = stage / "manifests/studio-sidecar-component.v1.json"
    component_path.write_text(json.dumps(component, indent=2) + "\n", encoding="utf-8")
    print(
        json.dumps(
            {
                "stage_root": str(stage),
                "scenario": str(scenario),
                "scenario_sha256": _sha256(scenario),
                "component": str(component_path),
                "component_sha256": _sha256(component_path),
                "launcher_sha256": _sha256(launcher),
                "adapter_runtime_closure_sha256": _sha256(adapter_manifest),
                "studio_runtime_closure_sha256": _sha256(studio_manifest),
            },
            sort_keys=True,
        )
    )


if __name__ == "__main__":
    main()
