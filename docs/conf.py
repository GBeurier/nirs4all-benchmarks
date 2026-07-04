from __future__ import annotations

from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]

project = "nirs4all-benchmarks"
author = "nirs4all ecosystem"
copyright = "2026, nirs4all ecosystem"
release = (ROOT / "VERSION").read_text(encoding="utf-8").strip()

extensions = ["myst_parser"]
source_suffix = {
    ".rst": "restructuredtext",
    ".md": "markdown",
}
exclude_patterns = ["_build", "Thumbs.db", ".DS_Store"]

html_theme = "furo"
html_title = "nirs4all-benchmarks"
html_static_path: list[str] = []
html_context = {
    "display_github": True,
    "github_user": "GBeurier",
    "github_repo": "nirs4all-benchmarks",
    "github_version": "main",
    "conf_py_path": "/docs/",
}

myst_heading_anchors = 3
suppress_warnings = ["myst.header", "myst.xref_missing"]
