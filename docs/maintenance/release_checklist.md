# Release checklist — nirs4all-benchmarks

Publishing is via `publish.yml` (release / dispatch, PyPI Trusted Publishing). Branch pushes to `main`
never publish to PyPI. Pages deploys only on `workflow_dispatch` or site-affecting pushes
(`pyproject.toml`, `VERSION`, `.github/workflows/pages.yml`, or `src/nirs4all_benchmarks/**`).

## Pre-release

- [ ] Green gate + CI green (see `quality_gates.md`).
- [ ] `CHANGELOG.md` `[Unreleased]` → a dated version; `VERSION` / `version.py` agree.
- [ ] **Fix the failing dispatch publish first** — a prior `Publish to PyPI [workflow_dispatch]` run
      failed; confirm the PyPI Trusted Publisher + `pypi` environment before tagging.
- [ ] `version-guard` green; tag `vX.Y.Z` points at the exact release commit.

## Release

- [ ] Tag `vX.Y.Z` on the release commit; publish the GitHub Release (triggers `publish.yml`).
- [ ] Confirm the run is green and the version is on PyPI.

## Post-release

- [ ] `pip install nirs4all-benchmarks==X.Y.Z` in a clean venv; smoke `n4a-benchmarks --help`.
- [ ] Verify benchmarks.nirs4all.org reflects the intended store/site.

## Notes

- Single-registry (PyPI). The generated site is published to the shared `*.nirs4all.org` domain on
  site-affecting pushes — review site/content changes before merging to `main`.
