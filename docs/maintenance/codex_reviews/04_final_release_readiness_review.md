# Codex Gate 4 — final release-readiness (nirs4all-benchmarks)

Consolidated into the ecosystem-level **Gate 5**; per-repo Codex effort was on **Gate 3** (see `03_main_diff_review.md`).

**Readiness snapshot:** the *Arena* — reproducible, scored NIRS pipeline benchmarks + a static SPA published
to benchmarks.nirs4all.org. Push-hardening added the community-health set, SHA-pinned all 17 actions, and a
`docs/maintenance/` trail. **No product code changed.**

**Documented (not changed):**
- A prior `Publish to PyPI [workflow_dispatch]` run failed — verify the Trusted Publisher before releasing.
- Large plotly/cytoscape JS bundles are vendored into the wheel — de-vendor / slim before release.
- Pages deploys on push to benchmarks.nirs4all.org — gate behind tag/dispatch.
- Coverage floor + Python 3.13 matrix (release-readiness ratchets).
