# Final hardening report — nirs4all-benchmarks

**Date:** 2026-07-06 · **Branch:** `main` · **Operator:** Claude (Opus 4.8) · **Reviewer:** Codex CLI 0.142.5

## Summary
Pragmatic hardening of the *Arena* (reproducible scored benchmarks + SPA): added the community-health set and
SHA-pinned all workflow actions. **No product code changed.** This repo was audit-only in the earlier sprint
because it was diverged; it has since been reconciled to `origin/main`, so it is now hardened + pushed.

## Baseline / commit
- **Baseline HEAD:** `27b3abd` (origin/main, CI-green, in-sync).
- **Commit:** *(this commit)* — community-health + 17 SHA-pins + docs/maintenance.

## Files
Added: `CODE_OF_CONDUCT.md`, `CITATION.cff`, `SECURITY.md`, `.editorconfig`, `.pre-commit-config.yaml`,
`.github/dependabot.yml` (github-actions + pip),
`docs/maintenance/{repository_audit,quality_gates,release_checklist,final_report}.md` + `codex_reviews/{03,04}`.
Modified: `.github/workflows/{ci,pages,publish,version-guard}.yml` (17 SHA-pins). CHANGELOG already current (`[Unreleased]`).

## Checks
- YAML/CFF validated. Non-code change; ruff+mypy+pytest run in CI (authoritative). Baseline branch-push CI green at `27b3abd`.
- **Codex Gate 3** — governance valid; marked the stale point-in-time audit. **Gate 4** — consolidated into ecosystem Gate 5.

## GitHub Actions (this push)
Branch-push gating runs (no PyPI publish): `CI`, `pages` (deploys benchmarks.nirs4all.org), `version-guard`. Verified green post-push.

## Residual risks
- Failing dispatch PyPI publish (verify Trusted Publisher); vendored plotly/cytoscape bundles; Pages-on-push; no coverage floor.

## 12-month maintenance
- Merge weekly Dependabot PRs after CI-green. Keep `CHANGELOG.md` `[Unreleased]` current.
- Before release: fix the Trusted Publisher, slim the wheel, tag the exact release commit.
