# Repository audit — nirs4all-benchmarks

> **Point-in-time audit — Phase-1 scan, 2026-07-04, PRE-hardening.** This snapshot reflects the repository
> *before* the hardening pass. Items it lists as missing (CODE_OF_CONDUCT / CITATION / SECURITY /
> .editorconfig / .pre-commit / Dependabot) or as floating action tags are **remediated by the same commit
> that adds this file**; the version and CI/packaging surface may have advanced since (the repo has since
> moved past 0.1.0). The **Deepest hardening roadmap** section below is the forward-looking list. Reviewed at
> Codex Gate 1.

- **Mode:** IN SCOPE — reconciled to origin/main; hardened + pushed. *(Was audit-only/diverged at the 2026-07-04 scan; the divergence has since been reconciled.)*
- **Baseline HEAD:** `1a32b38 (DIVERGED — audit only)`
- **Role:** Reproducible & scored NIRS pipelines on nirs4all-datasets — the Arena: a weights-free benchmark store + dataviz service (Python).
- **Stack:** Python >=3.10 (src layout, package nirs4all_benchmarks). Build backend: Hatchling (dynamic version from version.py). Package manager in CI: uv. Core deps: pydantic>=2.6, jsonschema, pyarrow, numpy, typer, rich, pyyaml; optional extras: fastapi/uvicorn/python-multipart (service), nirs4all/dag-ml-py/nirs4all-datasets (producers). Store = SQLite (stdlib) + Parquet. Frontend = no-build vanilla-JS SPA with vendored plotly/cytoscape. Docker image on python:3.11-slim + uv. Tooling: ruff (line 120), mypy (pydantic plugin), pytest+pytest-cov.

## Release-readiness verdict
nirs4all-benchmarks is in solid shape for a Beta/0.1.x public benchmark repo: a clean src layout, a 3-Python-version CI matrix running ruff+mypy+pytest with coverage plus a frontend syntax check and a Docker healthz smoke test, all currently green, and a sensible version-guard guardrail. The main release-readiness gaps are governance/hardening rather than correctness: missing SECURITY.md, CODE_OF_CONDUCT, CITATION.cff, .editorconfig, pre-commit, and Dependabot; Actions and Docker bases pinned only to floating tags; no coverage floor, no docs builder despite a rich docs/ corpus, and no PyPI release automation. The most concrete risk is a version drift — the manifest (0.1.0) trails the latest git tag (v0.1.2) — combined with a Pages workflow that redeploys the live public site on every push to main. This checkout is 2-ahead/13-behind origin and should remain audit-only (do not push); the quick wins are all low-risk additions, and the deep roadmap centers on SHA-pinned reproducible CI, tag-triggered Trusted-Publishing releases, unified version source, coverage/docs gating, and de-vendoring the multi-MB JS from the wheel.

## Gate commands (detected)
| key | value |
|---|---|
| `install` | uv pip install -e ".[service,dev]"  (CI uses: uv venv --python <ver> .venv && uv pip install --python .venv -e ".[service,dev]" pytest-cov) |
| `test` | pytest -q --cov=nirs4all_benchmarks --cov-report=xml |
| `lint` | ruff check src tests |
| `typecheck` | mypy |
| `format` | ruff format src tests  (available via ruff but NOT wired into CI or pre-commit) |
| `docs_build` | — |
| `package_build` | python -m build  (hatchling backend; no explicit command committed) |

## CI
- **Latest status:** All green. gh run list --limit 8 shows the 8 most recent runs (CI, version-guard, pages) all [ok]. No failing run to diagnose.
- **Workflows:**
- ci.yml — push/PR to main; matrix py3.10/3.11/3.12: ruff check, mypy, pytest+coverage; separate frontend-syntax job (node --check on SPA modules); docker build + healthz smoke test (needs green-gate). Image is built but NOT pushed to any registry.
- pages.yml — push to main + workflow_dispatch; seeds demo store, builds static site, deploys to GitHub Pages (benchmarks.nirs4all.org). Deploy-on-push-to-main.
- version-guard.yml — push/PR to main; fails if in-repo VERSION manifest is ahead of latest git tag (ecosystem version-sync guardrail).
- **Gaps:**
- No coverage threshold / fail_under enforced — coverage.xml is emitted as an artifact only, never gated.
- GitHub Actions are pinned to floating major tags (@v4/@v5, uv:latest), not commit SHAs — no supply-chain pinning.
- CI has no ruff format --check gate (only ruff lint); formatting drift is unguarded.
- No CodeQL / pip-audit / dependency scanning workflow.
- Docker CI job builds from unpinned python:3.11-slim and uv:latest — non-reproducible base.

## Standard files
- **Present:** readme, changelog, contributing, license, gitignore
- **Missing:** security, code_of_conduct, citation, editorconfig, precommit, pr_template, issue_template, dependabot

## Packaging
- **name:** `nirs4all-benchmarks` — **version:** `0.1.0`
- **issues:**
- Version drift: version.py and VERSION both say 0.1.0, but git tags exist up to v0.1.2 (v0.1.0/v0.1.1/v0.1.2). The manifest is 2 patch releases behind the latest tag — version-guard only blocks manifest-ahead, so this stale-behind state passes CI silently but means the source tree does not reflect the last two tagged releases.
- Two independent version sources: version-guard reads the VERSION file (version_file strategy) while Hatchling reads src/nirs4all_benchmarks/version.py — they can drift apart with nothing enforcing equality.
- No PyPI publish workflow exists; scripts n4a-benchmarks/n4a-arena are defined but the package is not released to any index, so 'packaging metadata' is untested by a real build/upload.
- 4.3 MB vendored plotly.min.js + 365 KB cytoscape.min.js are shipped inside the wheel (artifacts glob src/.../web/**/*), bloating the sdist/wheel; no provenance/version pin recorded for the vendored JS.
- requires-python >=3.10 but classifiers/CI stop at 3.12 — no 3.13 coverage.

## Tests
- **framework:** pytest (+pytest-cov, httpx for FastAPI TestClient)
- **estimate:** 12 test files, ~12+ test functions covering adapters, contract, identity, ingestion, scoring, store, queries, service API, site build, fixtures regression, repository bridge, upload indexing — broad surface coverage.
- **coverage:** pytest-cov configured and coverage.xml emitted for the nirs4all cockpit, but NO fail_under threshold, no [tool.coverage] section, no minimum enforced anywhere.

## Docs
- **system:** Plain Markdown design corpus in docs/ (ARCHITECTURE, API, CONTRACT, DATAVIZ, DEPLOYMENT, IDENTITY, INGESTION, ADAPTERS, CLI, ROADMAP) plus top-level DESIGN.md, DATA_MANAGEMENT.md, PERSISTENCE_FORMATS.md. No docs generator.
- **status:** No mkdocs.yml, docs/conf.py, or .readthedocs.yaml — docs are not built or published anywhere; the docs_build gate is empty. Content is rich but unrendered.

## Risks
| severity | area | detail |
|---|---|---|
| medium | packaging/versioning | Manifest (VERSION + version.py = 0.1.0) is behind the latest tag v0.1.2; the tree does not represent the last two releases and the two version sources can drift independently. A build off main would produce a 0.1.0 wheel despite v0.1.2 being tagged. |
| medium | ci-supply-chain | All workflow actions use floating major tags (actions/checkout@v4, astral-sh/setup-uv@v5, upload-artifact@v4, deploy-pages@v4) and Docker uses python:3.11-slim + uv:latest — none pinned to SHAs/digests. No CodeQL, pip-audit, or Dependabot. |
| medium | release-safety | pages.yml deploys the public site (benchmarks.nirs4all.org) on every push to main. Any push — including a docs-only commit — redeploys the live Pages site; with 13 unpushed-behind commits locally this repo is out of sync with origin and a push could unexpectedly move the site. |
| low | tests | No coverage threshold enforced; regressions in coverage cannot fail CI. |
| low | docs | Extensive Markdown docs with no build/publish path (no RTD/mkdocs); risks staleness and no rendered site. |
| low | wheel-size | 4.3 MB plotly + 365 KB cytoscape vendored JS bundled into the wheel with no recorded upstream version. |

## Security
- **info** — Secret scan over tracked Python source clean. Only match was in vendored src/nirs4all_benchmarks/web/vendor/plotly.min.js (library code, not a leak).
- **low** — No SECURITY.md / vulnerability-disclosure policy (ecosystem contact is nirs4all-admin@cirad.fr per policy).
- **low** — CI workflows use default/broad token behavior on non-Pages jobs; ci.yml and version-guard set no explicit top-level least-privilege permissions block (version-guard does set contents: read at job level; ci.yml sets none, inheriting repo default).

## Quick wins (pragmatic scope — safe to apply now)
- Add SECURITY.md with the ecosystem disclosure contact (nirs4all-admin@cirad.fr) and supported-versions note.
- Add explicit least-privilege 'permissions: contents: read' at the top of ci.yml (version-guard already scopes it; pages.yml correctly scopes pages/id-token).
- Add .github/dependabot.yml for pip + github-actions ecosystems (weekly).
- Add .editorconfig (line length 120 to match ruff) and a .pre-commit-config.yaml running ruff check + ruff format + mypy to match the CI gate locally.
- Add a ruff format --check step to ci.yml so formatting is gated, not just lint.
- Reconcile the version manifest: bump VERSION + version.py to 0.1.2 to match the latest tag (or document why main trails), so a build off main is not a stale 0.1.0.
- Add CITATION.cff (author Gregory Beurier <beurier@cirad.fr>) — the ecosystem is citation-oriented.
- Add a coverage floor: [tool.coverage.report] fail_under with a conservative starting value and --cov-fail-under in the pytest step.
- Add PR and issue templates under .github/ (bug/feature) for a public benchmark repo.
- Add CODE_OF_CONDUCT.md (Contributor Covenant) to match the other public ecosystem repos.

## Deepest hardening roadmap (fullest realistic hardening)
- Pin every GitHub Action to a full commit SHA (with a Dependabot actions updater to bump them), and pin Docker base images by digest (python:3.11-slim@sha256:..., uv by tag+digest) for reproducible CI/image builds.
- Add supply-chain scanning: CodeQL (python + javascript for the vendored SPA), pip-audit / uv pip audit, and optionally Trivy on the built Docker image in the docker job.
- Introduce a real release workflow: tag-triggered (on: push tags 'v*') build via python -m build, twine/uv publish to PyPI with Trusted Publishing (OIDC, id-token: write, no stored token), and GitHub Release notes generated from CHANGELOG. Keep version-guard as the pre-release gate.
- Unify the version source of truth: drive both version-guard and Hatchling from one file, or add a CI check asserting VERSION == version.py.__version__ == the tag being released.
- Enforce coverage: set fail_under (target 80%+), publish coverage to the cockpit, and add a coverage badge; extend the pytest matrix to include Python 3.13.
- Stand up a docs site: add mkdocs.yml (Material) or Sphinx+MyST over the existing docs/ Markdown, wire an RTD/.readthedocs.yaml or a docs-build CI job, and cross-link to the ecosystem card-grid per the RTD-ready standard used by the other repos.
- De-vendor or externalize the 4.3 MB plotly/cytoscape bundles from the wheel (ship a slim wheel; load JS as a separate asset/optional extra) and record upstream versions + license in THIRD_PARTY_NOTICES.
- Reproducibility hardening for the benchmark store: pin dataset/pipeline provenance (nirs4all/dag-ml/datasets versions) into each ArenaRunExport and add a CI job that re-seeds fixtures and diffs scoring output for determinism.
- Add a lockfile (uv.lock) and use uv sync --frozen in CI for byte-reproducible dependency installs.
- Add SLSA/attestation (actions/attest-build-provenance) for the wheel and Docker image once publishing is enabled.
- Split Pages deploy from every-push: build on push but deploy only on tag or workflow_dispatch/environment approval to decouple the live site from routine main commits.
- Add SBOM generation (syft/cyclonedx) for the Docker image and wheel.

## Push-safety notes
- pages.yml deploys the live public site benchmarks.nirs4all.org (permissions pages: write / id-token: write, actions/deploy-pages@v4) on EVERY push to main — a push here immediately mutates production Pages, even for non-site changes. (.github/workflows/pages.yml lines 9-12, 42-50)
- Local main is diverged: 2 ahead / 13 BEHIND origin/main. Pushing would either be rejected (non-fast-forward) or, if forced, would roll the remote back 13 commits and trigger a stale Pages redeploy + version-guard evaluation. This repo should not be pushed (audit-only).
- version-guard.yml gates on manifest-vs-tag: because VERSION/version.py (0.1.0) trails tags v0.1.1/v0.1.2, the guard passes now, but any commit that bumps the manifest to >0.1.2 on main WITHOUT first creating the matching tag will hard-fail CI on push (by design). Bump must ship as a tag. (.github/workflows/version-guard.yml lines 110-115)
- docker job in ci.yml builds an image but does not push it — no registry-publish risk on push. (.github/workflows/ci.yml lines 65-79)
- Cross-repo coupling: CI emits coverage.xml as an artifact consumed by the external 'nirs4all cockpit' (commit 2b407d6) and the Pages site is wired to the ecosystem domain/branding — changes to those outputs have downstream effects beyond this repo.
