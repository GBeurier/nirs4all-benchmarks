# Pipeline and component quality ranking

## Maintainer-approved scope

Date: 2026-11-04. Implement in order: numerical methods, benchmark usage logic,
shared UI components, then benchmark UI integration. Deliver one pull request per
project; never merge. Development uses isolated worktrees based on committed main
HEADs; concurrent refactor and qualification changes in original checkouts are
excluded and preserved. Sol high agents own complex work; Terra agents may own
simple bounded work. Each implementation block receives root review and tests.

Retain Elo, Bradley–Terry, TrueSkill 1, named OpenSkill/Weng–Lin variants,
average ranks/Friedman, and relative-gain meta-analysis. Exclude TrueSkill 2.
Evaluate mixed effects and fANOVA through minimal static external builds with
analysis-specific fixed configuration; defer expensive integrations with evidence.

## Questions and interpretation

Expose rankings of complete pipelines and of models, preprocessing operators,
operator parameters, feature selection, augmentation and split strategies.
Distinguish observed performance, matched contribution, adjusted contribution,
and variance importance. A component's marginal average is descriptive, not a
causal contribution. Interactions, operator order, domain and coverage matter.

Elo provides a simple sequential rating; expose order and update policy.
Bradley–Terry fits global paired outcomes; regularize separation and report
comparison graph connectivity. TrueSkill 1 and Weng–Lin provide approximate
conditional uncertainty, not universal cross-domain confidence intervals.
Average rank is descriptive; Friedman requires complete comparable blocks,
tie correction and a clearly identified global test. A non-significant result
does not prove practical equivalence. Relative gains preserve effect magnitude;
positive-loss ratios cannot be applied blindly to signed metrics such as R².
Meta-analysis needs supplied paired effect variances; fold independence must
never be invented. Repeated observations within a dataset are not independent
datasets. Unsupported analyses must return explicit reasons, never fake results.

## Scientific contract

1. Partition by metric, direction, target/task, score scope, partition and
   evaluation protocol before constructing comparisons.
2. Pair candidates on the same dataset/task and actual split/CV identity;
   missing identities cannot establish matched comparisons.
3. Preserve repeat/fold hierarchy and deduplicate repeated ingestion. Weight
   datasets explicitly rather than letting trial count determine importance.
4. Component parameters are operator-qualified; unrelated `n_components`
   parameters must not be merged. Show comparisons and component coverage.
5. Component contrasts require matching the rest of the pipeline; unmatched
   component distributions are labeled descriptive and are not adjusted effects.
6. Rank split strategies by a declared quality criterion (e.g. external error,
   stability or optimism gap), not simply their own best CV score. Different
   test sets do not make a controlled splitter comparison.
7. Distinguish raw per-dataset score, normalized gain and rank. Do not pool raw
   loss values across targets with different units as a universal score.
8. Version method configuration, filters, aggregation, seed/order, source
   coverage, uncertainty type and warnings in every result artifact.

## Ownership and implementation blocks

### A — nirs4all-methods

Own numerical kernels, validation, C ABI, thin language bindings and reference
parity. Use new ranking-specific files to minimize overlap with active refactor.
No scientific algorithms in benchmark Python or UI TypeScript. Heavy external
feasibility decisions are recorded in `docs/RANKING_EXTERNAL_DEPENDENCIES.md`.

### B — nirs4all-benchmarks

Construct controlled comparison cohorts from store identities, call methods,
return versioned artifacts and integrate service/static delivery. Cache keys
include all filters and method settings. Missing native runtime is an explicit
availability state. Existing data remains usable without installing ranking.
Snapshot delivery uses native precomputed artifacts; never duplicate scientific
ranking algorithms in JavaScript. Arbitrary static filters must not silently
reuse a ranking calculated for another cohort.

### C — nirs4all-ui

Provide presentational React components with a framework-free artifact contract:
rankings with uncertainty/coverage, pairwise matrices, component-effect views,
parameter and interaction views, and splitter diagnostics. Reuse Plotly through
a host-supplied renderer, keeping browser globals and scientific execution out
of the shared package. Every chart has a synchronized accessible values table;
uncertainty types and missing data remain explicit. App state and API calls
belong to the host.

### D — benchmark UI integration

Mount shared React components in the existing JavaScript application, without a
whole-application migration or changes to Studio/Web. Adapt native artifacts to
the shared contracts. Keep existing pages working; expose unavailable/deferred
analyses honestly. Build a reproducible shared-component bundle rather than
copying chart implementations into the benchmark.

## Validation and review

Check direction reversal, ties, disconnected comparisons, unbeaten candidates,
missing data, all-equal/constant scores, invalid/non-finite inputs, order effects,
duplicate ingestion and mismatched split/task/partition identities. Verify
cross-dataset unit invariance where promised. Compare numerical outputs against
independent references or hand-derived examples. Include C ABI and binding
checks, service/static consistency, UI rendering/type checks, and an end-to-end
native artifact → API/snapshot → shared component probe.

All estimates are preliminary: simple rank/rating kernels are small; native
mixed-effects optimization, external portability, and statistically valid input
assembly carry most risk. Deferred scope remains explicitly visible in PRs.

## Execution ledger

- Original checkout changes recorded before worktree creation; none copied.
- Methods base: `6a6ba8cf5e4930953d2fc10c80748547805fc87d`.
- Benchmarks base: `52d6cd03d19b8a4d1641b018831f2e5b619560aa`.
- UI base: `8bfc5f51600b59fc5cc899cd93f58da98fb2f851`.
- Branch in each project: `feat/pipeline-ranking-20261104`.
- GitHub publishing identity verified as `GBeurier`; token never printed.
- Block A implementation and heavy-library feasibility assessment started.
