# Quality gates — nirs4all-benchmarks

The *Arena*: reproducible, scored NIRS pipeline benchmarks + a static SPA published to
benchmarks.nirs4all.org.

## Local green gate (matches CI)

```bash
uv venv --python 3.11 .venv
uv pip install --python .venv -e ".[service,dev]" pytest-cov
.venv/bin/ruff check src tests
.venv/bin/mypy
.venv/bin/pytest -q --cov=nirs4all_benchmarks --cov-report=xml
```

Optional local hooks: `uvx pre-commit run --all-files`. Site preview:
`.venv/bin/n4a-benchmarks build-site --store ./pages-store --out ./site --domain benchmarks.nirs4all.org`.

## CI gates (`.github/workflows/`)

| workflow | trigger | gate |
|---|---|---|
| `ci.yml` | push/PR | ruff + mypy + pytest (coverage) matrix; Docker image build (not pushed) |
| `pages.yml` | dispatch or site-affecting push `main` | build the SPA + **deploy GitHub Pages → benchmarks.nirs4all.org** |
| `version-guard.yml` | push/PR | manifest not ahead of latest `v*` tag |
| `publish.yml` | **release / dispatch** | PyPI Trusted Publishing — **not** on branch push |

All third-party actions are **SHA-pinned** (17 across 4 workflows), Dependabot-tracked (github-actions + pip).

## Known gaps (deepest-hardening roadmap)

- A prior `Publish to PyPI [workflow_dispatch]` run failed — verify the Trusted-Publisher / environment setup before releasing.
- De-vendor the large plotly/cytoscape JS bundles from the wheel (ship slim; load JS as a separate asset); record upstream versions/licenses in `THIRD_PARTY_NOTICES`.
- Gate the Pages deploy behind tag/dispatch so routine `main` commits don't mutate the public site.
- Enforce a coverage floor; extend the matrix to Python 3.13.
