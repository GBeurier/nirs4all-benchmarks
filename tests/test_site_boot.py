"""The closed result builder refuses before store access or browser startup.

Historical SPA startup/retry assertions remain verbatim in the static-oracle
fixtures; this test does not qualify browser recovery or scientific publication.
"""

from __future__ import annotations

import subprocess
from pathlib import Path

import pytest

from nirs4all_benchmarks import site_build


def test_result_site_refuses_before_browser_startup(tmp_path: Path, monkeypatch) -> None:
    def forbidden(*args, **kwargs):
        pytest.fail("closed result publication must not open a store or launch a browser")

    monkeypatch.setattr(site_build, "ArenaStore", forbidden)
    monkeypatch.setattr(subprocess, "run", forbidden)
    with pytest.raises(ValueError, match=r"^publication_gate_closed$"):
        site_build.build_site(tmp_path / "store", tmp_path / "site", domain=None)
    assert not (tmp_path / "store").exists()
    assert not (tmp_path / "site").exists()
