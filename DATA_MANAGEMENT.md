# nirs4all-benchmarks — data management mechanism & the engine output contract

| Field | Value |
|---|---|
| Status | Design proposal (for review) |
| Date | 2026-06-14 |
| Audience | Arena devs; nirs4all / dag-ml / dag-ml-data / nirs4all-io maintainers |
| Scope | How the Arena **stores, keys, ingests, and serves** pipeline×dataset run data for meta-analysis — and the **output contract the Arena requires** from the soon-to-be-frozen "dag-ml-as-engine" storage |
| Premise | Short-term, **dag-ml becomes the execution engine of nirs4all**. The run-output storage format will be frozen. This doc states what the Arena needs so that freeze is Arena-ready; the libs are then adapted to emit it. |
| Companions | [`DESIGN.md`](DESIGN.md) = conceptual entity model + Arena store schema (the *what*). [`PERSISTENCE_FORMATS.md`](PERSISTENCE_FORMATS.md) = how nirs4all/dag-ml/nirs4all-io serialize today + compatibility (the *from where*). **This doc = the concrete data-management mechanism + the engine export contract (the *how* + the *requirements*).** |

---

## 0. Premise and where this fits

`DESIGN.md` fixes the conceptual model (the `RunCondition` tuple, `DatasetCard`/`DatasetFingerprint`, `PipelineDAGSpec`, `ScoreSet`/`ResidualSet`, "no artifacts", SQLite+Parquet). `PERSISTENCE_FORMATS.md` established the technical reality of the three producing libraries and reached three load-bearing conclusions this design builds on:

1. **Pipeline identity** is best taken from dag-ml's `graph_fingerprint` (a SHA-256 over a canonical `GraphSpec`), with an Arena-owned id/order normalization.
2. **Dataset + sample identity** comes from `nirs4all-io`'s `CoordinatorDataPlanEnvelope` (three SHA-256 fingerprints + stable `SampleId`s).
3. **The hard gap**: nirs4all today writes *positional* prediction `sample_indices` with no stable-id link, so residuals are not yet cross-run comparable by sample identity.

The premise here changes the calculus in the Arena's favour. When **dag-ml is the engine**, a run *natively* produces exactly the structures the Arena wants — fold-tagged, **sample-keyed** OOF predictions (`PredictionBlock.sample_ids`), a `graph_fingerprint`, a `fold_set_fingerprint`, hash-chain-derived seeds, metric reports tagged with scope, leakage attestations (`unsafe_flags`), and standards-conformant provenance. The Arena's job is therefore **not** to reverse-engineer a positional store; it is to **specify a weights-free, content-addressed export contract** that the frozen engine output must satisfy, and to manage that data for meta-analysis.

This document is deliberately opinionated about what the freeze must include, because the window to influence it is now.

---

## 1. Design principles

These are the non-negotiables the rest of the design obeys.

1. **Separate `ArenaStore`, fed by a defined export contract — not a reach into engine internals.** The Arena owns `arena.sqlite` + `arrays/*.parquet` (+ `exports/`); it ingests a versioned **`ArenaRunExport`** (§4). It never reads engine working files or live handles. (`DESIGN.md` §5.2 option 2, chosen over "extend the workspace" because the engine store is about to be frozen and should expose a *stable contract*, not its tables.)
2. **No artifacts, ever.** No fitted models, transformers, feature caches, or bundles. The Arena keeps documented pipelines, scores, residuals, identity cards. Artifact *references* (content fingerprints) may be recorded for provenance; bytes are never stored. (`DESIGN.md` §2.)
3. **Content-addressed identity, derived from engine fingerprints — never producer UUIDs.** Every dimension is keyed by a hash the Arena can recompute or that the engine guarantees stable (graph/fold/schema/relation fingerprints, derived hashes). UUIDs from any producer are recorded but never used as join keys.
4. **Sample-keyed, not position-keyed.** Residuals are keyed by a **stable `sample_id`** (io `SampleId`). Positional indices are accepted only with an explicit ordinal→`SampleId` map and are flagged as degraded. (Direct consequence of `PERSISTENCE_FORMATS.md` §4.4.)
5. **Scores are versioned facts, never bare numbers.** Every score carries a `ScoreComputationSpec` (metric implementation, scope, aggregation, filters, date). Recomputation adds a new `ScoreSet`; it never mutates the old one. (`DESIGN.md` §10.)
6. **Idempotent ingestion.** An export ingested twice produces the same store. The idempotency key is `(arena_export_hash, target_collection, arena_schema_version)`.
7. **Leakage-honest by construction.** Ingestion records the engine's OOF/leakage attestation and refuses (or quarantines) runs that cannot prove within-train CV. Trustworthy meta-analysis is the whole point.
8. **Privacy is a first-class field, applied at ingest.** Sample ids are pseudonymized at ingest (the engine *sanitizes* but does not pseudonymize); raw X/y is published only where the dataset policy allows.
9. **Every contract is versioned and drift-tracked.** Four independent schema versions feed the Arena (engine bundle/plan, dataset envelope, prediction/score format, Arena export). Fingerprints are comparable only within matching versions.

---

## 2. The identity spine (the heart of the mechanism)

Meta-analysis is only as good as its join keys. Everything hangs off a small set of **content-derived identities**, each sourced from a specific engine fingerprint. This table is the contract between "what the engine emits" and "how the Arena dedups and queries".

| Arena identity | Definition | Source (dag-ml-as-engine) | Today's gap |
|---|---|---|---|
| `dataset_fingerprint` | identity of the data regime | io `schema_fingerprint` (+ `relation_fingerprint`) from the `CoordinatorDataPlanEnvelope` | envelope is lossy (drops `repetition_id`, no schema/relation table) → must also capture `DatasetSpec` |
| `sample_id` | stable per-sample key | io `SampleId` (`SampleRelationTable`) | not threaded into nirs4all predictions → **needs the ordinal→`SampleId` sidecar** |
| `task_hash` | target + encoding identity | io `DatasetSpec` target + `task_type`; optional `target_vector_hash` if publishable | classification target encoding not yet hashed |
| `dataset_variant_hash` | view/subsample/aggregation of a dataset for a task | Arena-computed over `{dataset_fingerprint, task, variant_spec}` | — |
| `pipeline_dag_hash` | topology-aware pipeline identity | dag-ml `graph_fingerprint` (normalized graph/node ids+order) | needs Arena normalization + a `graph_fingerprint_json` API |
| `cv_instance_hash` | the exact fold assignment | dag-ml `fold_set_fingerprint` | order-independent already ✓ |
| `split_instance_hash` | the exact external train/test split | dag-ml split invocation (`SplitInvocation`) fingerprint | confirm a stable split-instance hash is emitted |
| `rng_context_hash` | seeds + determinism flags | dag-ml `SeedContext.root_seed` + derivation policy (+ framework seeds) | framework seeds (numpy/torch/…) not captured by dag-ml |
| `refit_strategy_hash` | selection + refit policy | dag-ml `SelectionDecision` + refit slot plan | — |
| `score_computation_hash` | metric impl + scope + aggregation + version | Arena `ScoreComputationSpec`; engine supplies metric name/scope/level | classification metrics absent in dag-ml |
| `run_condition_hash` | the natural identity of a condition | `H(dataset_variant_hash, split_instance_hash, cv_instance_hash, rng_context_hash, pipeline_dag_hash, refit_strategy_hash)` | composed by the Arena |
| `arena_export_hash` | idempotency key of an ingested export | SHA-256 of the canonical `ArenaRunExport` manifest | produced at export |

**Why this works under the premise:** dag-ml already produces `graph_fingerprint`, `fold_set_fingerprint`, sample-keyed `PredictionBlock`s, and `SelectionDecision`s; io already produces `schema`/`relation` fingerprints and `SampleId`s. The Arena's `run_condition_hash` is a pure composition of identities the engine can emit. The only *new* obligations on the freeze are the small, specific gaps in the right-hand column — collected as requirements in §7.

---

## 3. Physical storage (delta to `DESIGN.md` §7)

The store shape is `DESIGN.md` §7 verbatim — `arena.sqlite` (dimensions + facts) + `arrays/*.parquet` (residuals) + `exports/`, no `artifacts/`. This section only states the **mechanism deltas** that the engine premise introduces.

```
arena-store/
  arena.sqlite                 # dimensions + facts, all keyed by §2 hashes
  arrays/
    residuals_<partition>.parquet   # SAMPLE-KEYED (sample_id), not positional
  exports/
    arena_run_<arena_export_hash>.zip   # the ingested ArenaRunExport (audit/replay of ingestion)
```

- **Dimension tables** (deduped, keyed by §2 hashes): `dataset_fingerprints`, `dataset_cards`, `task_specs`, `dataset_variants`, `pipeline_dags`, `pipeline_nodes`, `pipeline_edges`, `operator_specs`, `parameter_values`, `split_specs`/`split_instances`, `cv_specs`/`cv_instances`, `rng_contexts`, `score_computation_specs`. (As `DESIGN.md` §6.) The new constraint: each carries the **engine fingerprint it was derived from** as a column, so provenance of the identity is auditable and drift is detectable.
- **`pipeline_dags`** stores the **canonical `GraphSpec` JSON** (the source of truth) plus the materialized effect-analysis columns (`pipeline_nodes`/`parameter_values`) extracted from it — so queries like "effect of `n_components`" run on normalized tables, not JSON. (`DESIGN.md` §6.11.)
- **Fact tables**: `run_conditions`, `executions` (`ExecutionSummary` — capsule, versions, time/mem, status, `unsafe_flags`), `score_sets` (+ `supersedes_score_set_id`), `metric_observations` (long format), `ingestion_batches`, `view_definitions`.
- **`residual_sets` (Parquet)** — the one schema change that matters: the row key is the **stable `sample_id`**, with `group_id`/`origin_sample_id` when present, plus `scope` (fold/cv/refit/test), `fold_id`, `partition`, `y_pred`/`y_true`/`y_proba` (per policy), `residual`, `weight`. `_agg`/aggregate rows are stored as **aggregate-level observations** (their own `unit_level`), never mixed into per-sample residuals.
- **Dedup tables** (`DESIGN.md` §7.4) become trivially correct because identities are content hashes: identical operators/params/DAGs/datasets/folds collapse by key on insert.

---

## 4. The ingestion contract: **`ArenaRunExport` v1**

This is the deliverable that influences the freeze. It is **one weights-free, content-addressed bundle per `execution`** (a concrete run of a `run_condition`). The engine (dag-ml driving a nirs4all run, with io supplying the dataset) emits it; the Arena ingests it with zero engine-internal knowledge.

**Layout** (zip or directory):

```
arena_run_<arena_export_hash>/
  manifest.json          # everything below, canonical-JSON, schema-versioned
  residuals.parquet      # SAMPLE-KEYED prediction/residual arrays (per policy)
  graph.json             # canonical GraphSpec (the pipeline DAG source of truth)
  provenance/            # OPTIONAL pass-through: lineage.prov.jsonld, ro-crate-metadata.json, openlineage.json
```

**`manifest.json` (v1) — the shape the freeze must be able to produce:**

```jsonc
{
  "arena_export_schema_version": 1,
  "producer": { "capsule": "python|studio|cluster|...", "nirs4all_version": "...",
                "dag_ml_version": "...", "dag_ml_data_version": "...", "io_version": "..." },

  "dataset": {                              // from io CoordinatorDataPlanEnvelope + DatasetSpec
    "dataset_fingerprint": "<sha256>",      // = io schema_fingerprint (the canonical dataset identity)
    "schema_fingerprint": "<sha256>", "relation_fingerprint": "<sha256>", "plan_fingerprint": "<sha256>",
    "dataset_card": { "name": "...", "modality": "spectroscopy", "signal_type": "...",
                      "axis": {"unit": "nm|cm-1", "n": 0, "range": [..]}, "sources": [..], "folds_declared": true },
    "dataset_spec_ref": "envelope is lossy → DatasetSpec embedded or referenced by hash",
    "visibility": "public|restricted|private|anonymized",
    "n_samples": 0, "n_features": 0
  },

  "task":            { "task_hash": "<sha256>", "task_type": "regression|binary|multiclass",
                       "target_name": "...", "target_unit": "...", "target_hash": "<sha256|null>" },
  "dataset_variant": { "dataset_variant_hash": "<sha256>", "variant_spec": { "size": "all", "aggregation": "none" } },

  "pipeline": {                             // from dag-ml ExecutionPlan
    "pipeline_dag_hash": "<sha256>",        // = normalized graph_fingerprint
    "controller_fingerprint": "<sha256>",   // operator-version identity
    "graph_ref": "graph.json",
    "nodes": [ { "node_id": "...", "role": "transform|model|merge|...", "operator": "pkg.Class",
                 "operator_version": "...", "params": { "n_components": 10 }, "branch_path": [], "fit_scope": "fold_train_only" } ]
  },

  "split": { "split_spec_hash": "<sha256>", "split_instance_hash": "<sha256>",
             "method": "kennard_stone|random|predefined|group|...", "params": {} },
  "cv":    { "cv_spec_hash": "<sha256>", "cv_instance_hash": "<sha256>",   // = fold_set_fingerprint
             "method": "kfold|stratified|group|nested", "n_folds": 5, "within_train_only": true },
  "rng":   { "rng_context_hash": "<sha256>", "root_seed": 0, "derivation": "sha256-hash-chain",
             "framework_seeds": { "numpy": 0, "torch": 0, "sklearn": 0 },
             "determinism_flags": { "PYTHONHASHSEED": 0, "cudnn_deterministic": true } },
  "refit": { "refit_strategy_hash": "<sha256>", "strategy": "global_best_params_full_train|per_fold|stacking|...",
             "selection_scope": "oof", "train_scope": "full_train", "selected_variant_id": "variant:..." },

  "run_condition_hash": "<sha256>",         // H(dataset_variant, split, cv, rng, pipeline_dag, refit)

  "execution": { "execution_id": "<uuid>", "status": "ok|failed|cancelled", "time_ms": 0, "peak_mem_mb": 0,
                 "os": "...", "hardware": "...", "failure_code": null },

  "leakage_attestation": { "oof_enforced": true, "group_leakage_checked": true,
                           "nested_cv_safe": true, "unsafe_flags": [] },

  "scores": {
    "score_computation_hash": "<sha256>", "score_version": "1.0",
    "observations": [
      { "metric_name": "rmse", "metric_value": 0.0, "direction": "min",
        "scope": "cv|fold|refit|test", "fold_id": "fold0|null", "partition": "validation|test|final",
        "aggregation_level": "sample|target|group", "n_samples": 0, "coverage": 1.0 }
    ]
  },

  "residuals": { "ref": "residuals.parquet", "key": "sample_id", "pseudonymized": false,
                 "publishable": { "y_pred": true, "y_true": true, "residual": true } },

  "provenance": { "prov_jsonld": "provenance/lineage.prov.jsonld",
                  "ro_crate": "provenance/ro-crate-metadata.json",
                  "openlineage": "provenance/openlineage.json" }   // all OPTIONAL
}
```

**`residuals.parquet` (v1) — sample-keyed:**

| column | type | note |
|---|---|---|
| `sample_id` | utf8 | **stable io `SampleId`** (pseudonymized at ingest) — the join key |
| `group_id` | utf8\|null | leakage unit, when known |
| `origin_sample_id` | utf8\|null | augmentation origin, when known |
| `scope` | utf8 | `fold`/`cv`/`refit`/`test` |
| `fold_id` | utf8\|null | |
| `partition` | utf8 | `validation`/`test`/`final` |
| `y_true` | f64\|null | per policy |
| `y_pred` | f64\|null | per policy (regression) |
| `y_proba` | list<f64>\|null | classification |
| `residual` | f64\|null | regression: `y_true − y_pred` |
| `weight` | f64\|null | |

Design notes:
- **Two halves, two producers.** `dataset.*` comes from **io**; everything else from **dag-ml**. The Arena composes `run_condition_hash` from both. This mirrors the architecture in `PERSISTENCE_FORMATS.md` §6.1.
- **The export is the freeze contract.** If a field here cannot be produced by the engine's frozen output, that is a §7 requirement, not an Arena workaround.
- **`graph.json` is the source of truth**; `pipeline.nodes[]` is the denormalized, query-friendly projection the Arena materializes for effect analysis.
- **Provenance is optional pass-through** — dag-ml already emits PROV/RO-Crate/OpenLineage; the Arena stores them verbatim as the provenance layer when present, and synthesizes a minimal record when absent.

---

## 5. Ingestion pipeline & the run/store/display state machine

```
INGEST(ArenaRunExport)
  1. VERIFY        arena_export_schema_version supported; canonical-JSON well-formed; arena_export_hash matches.
  2. DEDUP-CHECK   idempotency key (arena_export_hash, collection, arena_schema_version) → if seen, no-op.
  3. RESOLVE IDS   recompute/validate every §2 hash; normalize the GraphSpec → pipeline_dag_hash;
                   compose run_condition_hash. (Never trust producer UUIDs.)
  4. VALIDATE      dataset card/fingerprint present; single-target task; split/CV consistent with n_samples;
                   rng_context present; leakage_attestation.oof_enforced == true (else QUARANTINE);
                   residuals keyed by sample_id (else mark DEGRADED/position-keyed); score_version present.
  5. PSEUDONYMIZE  map sample_id/group_id → sample_pseudo_id/group_pseudo_id per dataset visibility policy.
  6. STRIP         drop any artifact refs' bytes (keep fingerprints only); drop disallowed arrays per policy.
  7. STAGE→COMMIT  upsert dimensions (dedup by hash) + facts in one transaction; write residuals.parquet.
  8. REPORT        clean_report_json (deduped rows, dropped fields, degraded keys, quarantines).

UPLOAD(pipeline | n4a | richer export, target_datasets)         # the user-facing entry
  PARSE → CANONICALIZE (pipeline_dag_hash; dataset_fingerprint via io; strip weights)
  for each target dataset/variant:
     run_condition_hash = compose(...)
     if EXISTS(run_condition_hash):                 → DISPLAY existing ScoreSet + ResidualSet     # "already run"
     elif export carries results for this condition: → INGEST(ArenaRunExport) → DISPLAY            # "has results"
     else:                                          → SCHEDULE engine run on the dataset
                                                      → on completion ingest its ArenaRunExport → DISPLAY
```

- **Quarantine, don't drop.** A run failing leakage validation is stored with `validity_status = quarantined` and excluded from published views, never silently discarded (auditability).
- **DEGRADED keying.** A legacy/position-keyed export (no stable `sample_id`) is ingested with scores intact but residuals flagged `key=positional`; it is queryable per-run but excluded from cross-run sample-level residual comparison until an ordinal→`SampleId` sidecar is supplied.
- **Re-ingest is safe.** Step 2 makes the whole pipeline idempotent; a corrected export supersedes via a new `ingestion_batch` without mutating prior rows.

---

## 6. Serving model (brief — `DESIGN.md` §9 owns dataviz)

The store is normalized so meta-analysis queries are plain SQL joins on §2 hashes:
- **Leaderboard / matrix**: `run_conditions ⋈ score_sets ⋈ metric_observations`, filtered by `score_computation_hash` + scope, grouped by `pipeline_dag_hash` × `dataset_fingerprint`.
- **Operator / parameter effect**: join `pipeline_nodes`/`parameter_values` (e.g. `n_components`) to scores — the reason the DAG is materialized into normalized tables, not left as JSON.
- **Residual explorer / complementarity**: `residual_sets` keyed by `sample_id` lets two pipelines' residuals be compared *on the same samples* — only possible because of sample-keying (§1.4). This is the single biggest capability the engine freeze unlocks.
- **Robustness**: variance across `cv_instance_hash` / `rng_context_hash` for one `pipeline_dag_hash`.

---

## 7. Requirements on the frozen engine output (the actionable list)

This is what "the Arena influences the choices" means concretely. Priorities: **[MUST]** = the Arena cannot do trustworthy meta-analysis without it; **[SHOULD]** = strongly wanted, has a workaround; **[NICE]** = opportunistic.

### On dag-ml (the engine)
- **[MUST] Sample-keyed predictions in the persisted output.** The frozen prediction store must persist `PredictionBlock.sample_ids` (stable io `SampleId`s), not positional indices. dag-ml already carries `sample_ids` in `PredictionBlock`; the requirement is that the **persisted/exported** form keeps them and that they equal the io `SampleId`s (not engine-internal renames). *Without this, cross-run residual analysis is impossible.*
- **[MUST] A stable `graph_fingerprint` exposed in the output**, plus a callable fingerprint API (`graph_fingerprint_json`, or `build_execution_plan_json` + extract) so the Arena can verify/recompute `pipeline_dag_hash`. Pair with an agreed **graph/node-id + ordering normalization** so semantically-equal pipelines hash equal (`PERSISTENCE_FORMATS.md` §5.3).
- **[MUST] Leakage attestation in the output**: persist `unsafe_flags` + a boolean that within-train CV / group / nested-CV safety held. The Arena quarantines runs that cannot attest.
- **[MUST] `fold_set_fingerprint` and a split-instance fingerprint** persisted per run (for `cv_instance_hash` / `split_instance_hash`).
- **[SHOULD] Classification metrics.** dag-ml metrics are regression-only (`Mse/Rmse/Mae/R2`). The Arena needs accuracy/F1/AUC/log-loss + `y_proba` with the same scope/fold tagging as `RegressionMetricReport`. Until then, the Arena ingests classification scores from nirs4all's native `scores` JSON and does **not** route them through dag-ml's metric model.
- **[SHOULD] Capture a full `RNGContext`**: `root_seed` is enough for dag-ml's own determinism, but the Arena wants framework seeds (numpy/sklearn/torch/jax/tf) and determinism flags (`PYTHONHASHSEED`, cuDNN) in the output, since those are experimental conditions.
- **[SHOULD] A weights-free export profile** = the `ArenaRunExport` of §4 emitted directly (manifest + sample-keyed residuals + provenance refs, **no artifact bytes**). This is the single call that replaces hand-assembly.
- **[NICE] Pass through PROV/RO-Crate/OpenLineage** into the export's `provenance/` (already implemented in dag-ml; just include refs).
- **[NICE] `schema_version` on every persisted contract** (bundle/plan/prediction/metric) surfaced in the export so the Arena can detect drift.

### On nirs4all-io (the dataset-identity provider)
- **[MUST] The ordinal→`SampleId` sidecar.** Emit, from the *same* assembly that builds the dataset the engine runs on, a manifest mapping `global_row_ordinal + partition + partition_row → observation/sample/group/repetition id`, with row counts/order validated. This is the bridge that makes engine predictions sample-keyed end-to-end (`PERSISTENCE_FORMATS.md` §4.4). *Highest-leverage single change.*
- **[MUST] Thread stable ids through `AssembledDataset`/`PartitionBlock`** (not only synthesize them at the dag-ml emit), so `sample_index.key`/`observation_id`/`group_id` — not just the repetition key — produce stable ids.
- **[SHOULD] Surface the full `DatasetSpec` + `DatasetSchema`/`SampleRelationTable`** alongside the (lossy) `CoordinatorDataPlanEnvelope`, so the Arena gets card fields + `repetition_id` the envelope drops.
- **[SHOULD] Pin the axis-kind contract**: cm⁻¹ currently maps to `AxisKind::Frequency`, not the first-class `AxisKind::Wavenumber` — and the axis kind feeds `schema_fingerprint`. Decide one mapping and freeze it, or `dataset_fingerprint` drifts across versions.
- **[NICE] A pseudonymization hook** at emit, or a documented guarantee that ids are *sanitized-only* so the Arena always pseudonymizes (it does — but make the contract explicit).

### On nirs4all (the host, during the dag-ml transition)
- **[SHOULD] Persist the io `SampleId` into the prediction path** so that even before full dag-ml-as-engine, nirs4all's parquet carries stable ids (today the io→`SpectroDataset` adapter passes only `{partition}`).
- **[SHOULD] Emit the `ArenaRunExport` profile** from the run lifecycle (the `export-arena` profile, `DESIGN.md` Stream H1) — weights-free, results-rich, one call.
- **[NICE] Stop relying on positional `sample_indices`** for `_agg`/aggregate twins (they are `sample_indices=None` today) — model them as aggregate-level units with their own ids.

---

## 8. Versioning, drift, and invalidation

- **Four contract versions feed the Arena**: dag-ml bundle/plan, dag-ml-data envelope/fingerprint, io `DatasetSpec`/canonical-JSON, and the `arena_export_schema_version`. The `ArenaRunExport.producer` block records all of them; `executions` stores them. **Fingerprints are comparable only within matching contract versions** — the Arena tags every identity with the producing version and refuses to dedup across incompatible ones.
- **First concrete drift case**: the cm⁻¹ → `Frequency`-vs-`Wavenumber` axis mapping changes `schema_fingerprint`, hence `dataset_fingerprint`. Pin it before the freeze (a §7 io requirement).
- **Score versioning**: a metric fix adds a new `ScoreSet` (`supersedes_score_set_id`); old ones stay auditable, marked superseded. (`DESIGN.md` §10.)
- **Invalidation is explicit, never destructive**: leakage detected, metric bug, dataset withdrawn, policy change → flip `validity_status`, do not delete rows. (`DESIGN.md` §10.2.)

---

## 9. Build order & open decisions

### Smallest useful build order
1. **Freeze the `ArenaRunExport` v1 manifest schema** (§4) as a JSON Schema in the Arena repo — this is the contract the libs target.
2. **io ordinal→`SampleId` sidecar spike** (§7 io [MUST]) — unblocks sample-keyed residuals end-to-end.
3. **dag-ml `graph_fingerprint` API + normalization spike** (`PERSISTENCE_FORMATS.md` §8 step 1) — fixes `pipeline_dag_hash`.
4. **`ArenaStore` skeleton**: ingest a hand-authored `ArenaRunExport` fixture → dimensions + facts + sample-keyed `residuals.parquet`, idempotent, with a clean report. No dataviz.
5. **Two fixtures** (a branch/merge pipeline + a PLS sweep, across two datasets) → freeze their `run_condition_hash`es as a regression contract.
6. **Wire the engine export**: have the dag-ml-as-engine run emit `ArenaRunExport` directly; validate against the fixtures.

### Open decisions for you to steer
1. **Export granularity** — one `ArenaRunExport` per `execution` (recommended: clean idempotency, natural for the run/store/display loop) vs. per `run_condition` (smaller, but merges re-runs). *Recommend per-execution.*
2. **Provenance retention** — store the full PROV/RO-Crate/OpenLineage pass-through (richer, heavier) vs. synthesize a minimal lineage record (lighter). *Recommend: store when present, since dag-ml emits it for free; cap size.*
3. **Residual retention policy** — a hard per-`ResidualSet` size cap with fallback to scores-only for very large user runs (`DESIGN.md` §13). Pick the cap.
4. **How hard to gate on leakage** — quarantine non-attested runs (recommended) vs. ingest-and-flag. *Recommend quarantine for public benchmarks, flag for user runs.*
5. **Whether the freeze adopts the `ArenaRunExport` as dag-ml's native weights-free export**, or the Arena keeps a thin adapter. *Recommend native* — it is the cheapest way to make the freeze Arena-ready and avoids a perpetual adapter.

Once these are settled, the requirements in §7 become concrete tickets against dag-ml, nirs4all-io, and nirs4all.
