# Arena contributor instructions

## Local evidence and ownership

Arena owns validated local run evidence, ingestion and comparison, not owner execution or scientific truth. Keep recipes, identity cards, versioned scores and sample-keyed residuals; strip fitted weights on read and never store fitted artifacts. Producer integrations stay optional and lazy.

Use content-addressed dimensions, never producer UUID join keys. Preserve idempotence and explicit failed/incomplete/cancelled/unreported records. Quarantine unsafe runs rather than silently dropping them. Recomputed scores create superseding score sets; never mutate stored scores.

Metadata validity is not scientific validation. Keep tri-state attestations, declared versus realized identities, explicit fold/variant context and exact task/metric/unit comparisons; unknown context is not ranked or silently pooled. Follow the [sole-v2 authority](docs/CONTRACT.md#sole-v2-contract-authority-4-november-2026) and [offline contract](docs/INGESTION.md#sole-v2-offline-contract-4-november-2026). Arena wire v1 is unsupported; its [exact-hash frozen-history converter](docs/INGESTION.md#pinned-frozen-history-explicit-one-off-conversion-only) is separately invoked, not a generic migration or fallback.

Keep the later store/transaction amendments distinct: [R3–R7 scope](docs/INGESTION.md#r3r7-restricted-rework-2026-11-04), [R1/R2 recovery](docs/INGESTION.md#approved-r1r2-recovery-successor-2026-11-04), [two-test recovery/atime scope](docs/INGESTION.md#authorized-two-test-recovery-successor-2026-11-04) and [F1–F3 corrections](docs/INGESTION.md#focused-f1f3-author-corrections-after-c833-review-2026-11-04). Preserve frozen inputs, original proofs, refusals and named platform limits; old checkpoints are not current whole-owner acceptance.

## Privacy and service boundaries

The [CLI reference](docs/CLI.md#commands) distinguishes unauthenticated offline-only `serve` from loopback project-token `serve-project`. The latter is an owner-documented LOCAL, UNQUALIFIED profile; it inherits no acceptance from the separately accepted S1 local-evidence slice. It grants no auth, hosting, upload, publication or remote-service acceptance. Root policy remains unchanged.

Follow [security](SECURITY.md) and the [project-token API](docs/API.md#project-token-api): authenticate before bodies, use exact project rights and startup snapshots, and never follow client paths/URLs. Private allowlisted labels can contain sensitive text; hashes and pseudonyms neither prove rights nor guarantee anonymity. Keep private per-export cards distinct from public receipts. Never publish offline output by hand. Scientific result publication remains closed. The explicit `build-site --unavailable-results` route may build only a data-free public status page; it grants no result, history, upload or service acceptance. Root owns existing-site deployment under the current maintainer authorization.

## Checks and licensing

Use [CONTRIBUTING](CONTRIBUTING.md#setup) for the existing Python environment/extras and complete [green gate](CONTRIBUTING.md#green-gate). From this checkout, in that environment:

```bash
.venv/bin/ruff check src tests
.venv/bin/mypy
.venv/bin/pytest -q
```

Keep Python public types/Google docstrings and configured Ruff/mypy rules. Frozen contract or fixture identity changes require their intended version/provenance treatment, not golden reblessing merely to force green. Frontend gates remain references for separately authorized joint work; no historical test count is a current green claim.

Follow [LICENSING](LICENSING.md): code is `CeCILL-2.1 OR AGPL-3.0-or-later`, with its stated commercial option; scored/content material uses CC-BY-4.0 and upstream dataset/asset terms remain authoritative. Security reports use the private channel in SECURITY.

## Working rules

Read applicable local and deeper instructions before editing. Preserve dirty/concurrent work and make only authorized changes. When a workspace policy exists at `../AGENTS.md`, read it within the current user scope; this local policy also works in a standalone checkout. An existing `CLAUDE.md` is a compatibility entrypoint, not a second policy dependency.

Use CodeGraph FIRST for implementation navigation only when this checkout has its own `.codegraph/`; use its exact projectPath or run the CLI here. Never auto-index or substitute an ancestor index. Unindexed code, docs and configs use ordinary source navigation; source, tests and build tools remain final authority.

Keep secrets, private data, generated workspaces, caches and build outputs out of Git. Respect owner licenses and fixture restrictions, record actual commands/exits/skips, and preserve scoped authorization: command examples do not grant installation, science, publishing, hosting or frontend work.

All application and project content must be in English, including UI text, subtitles, tooltips, errors, documentation, comments and docstrings, test and fixture prose, reports, pull requests, commits, and generated content. Conversation with the maintainer may be in French. Preserve proper names, identifiers, filenames, source quotations, and historical or provenance records where translation would change their meaning or provenance.
