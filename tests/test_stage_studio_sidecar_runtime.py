"""Negative closure checks for the Studio native staging script."""

from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace

import pytest
from scripts import stage_studio_sidecar_runtime as stage


def _run_with_dynamic(dynamic: str):
    def fake_run(command: list[str], **_kwargs: object) -> SimpleNamespace:
        if command[:2] == ["readelf", "-d"]:
            return SimpleNamespace(stdout=dynamic)
        if command[:2] == ["readelf", "--version-info"]:
            return SimpleNamespace(stdout="GLIBC_2.35 GLIBCXX_3.4.29")
        if command[0] == "strings":
            return SimpleNamespace(stdout=f"{stage.METHODS_VERSION}\n")
        raise AssertionError(command)

    return fake_run


@pytest.mark.parametrize("tag", ["RPATH", "RUNPATH"])
def test_methods_staging_refuses_embedded_search_path(monkeypatch: pytest.MonkeyPatch, tag: str) -> None:
    dynamic = f"0x000000000000001d ({tag}) Library rpath: [/untrusted]\n"
    monkeypatch.setattr(stage.subprocess, "run", _run_with_dynamic(dynamic))

    with pytest.raises(ValueError, match="must not contain RPATH or RUNPATH"):
        stage._verify_methods_dynamic_closure(Path("libn4m.so"))


def test_methods_staging_refuses_needed_library_outside_allowlist(monkeypatch: pytest.MonkeyPatch) -> None:
    needed = ["libc.so.6", "libgcc_s.so.1", "libm.so.6", "libstdc++.so.6", "libpython3.11.so.1.0"]
    dynamic = "\n".join(f"Shared library: [{name}]" for name in needed)
    monkeypatch.setattr(stage.subprocess, "run", _run_with_dynamic(dynamic))

    with pytest.raises(ValueError, match="outside the system allowlist"):
        stage._verify_methods_dynamic_closure(Path("libn4m.so"))
