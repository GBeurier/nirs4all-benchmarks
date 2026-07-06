"""Release version metadata consistency checks."""

from __future__ import annotations

from pathlib import Path

import yaml

from nirs4all_benchmarks.version import __version__

ROOT = Path(__file__).resolve().parents[1]


def test_release_version_metadata_agrees():
    version_file = (ROOT / "VERSION").read_text(encoding="utf-8").strip()
    citation = yaml.safe_load((ROOT / "CITATION.cff").read_text(encoding="utf-8"))

    assert version_file == __version__
    assert citation["version"] == __version__
