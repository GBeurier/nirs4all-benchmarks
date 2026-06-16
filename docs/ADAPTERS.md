# Producer adapters: dual compatibility

How the Arena turns two mechanically incompatible producers — a **nirs4all workspace** and a **dag-ml `ExecutionBundle`** — plus a portable **`.n4a`** recipe upload, into one weights-free, content-addressed `ArenaRunExport`.

## Contents

- [Two producers, one contract](#two-producers-one-contract)
- [Adapter A: nirs4all workspace](#adapter-a-nirs4all-workspace)
- [Adapter B: dag-ml bundle](#adapter-b-dag-ml-bundle)
- [`.n4a` recipe extraction](#n4a-recipe-extraction)
- [Side-by-side comparison](#side-by-side-comparison)
- [What happens after the adapter](#what-happens-after-the-adapter)

## Two producers, one contract

nirs4all and dag-ml persist a run in fundamentally different ways — there is no common container and they are not interchangeable on disk (PERSISTENCE_FORMATS.md §5.1). nirs4all writes `store.sqlite` + `arrays/*.parquet` + content-addressed `artifacts/*.joblib`; dag-ml writes a single serde-JSON `ExecutionBundle` whose artifacts are referenced, never embedded. The Arena does **not** try to unify the containers. Instead each producer has its own adapter, and both adapters emit the **same** `ArenaRunExport` — the typed contract in `contract/arena_run_export.py`, validated against `contract/schema/arena_run_export.schema.json`.

The adapters live in `src/nirs4all_benchmarks/adapters/` and share three rules (see the package docstring in `adapters/__init__.py`):

- **One contract.** Every adapter produces an `ArenaRunExport`; the Arena store ingests that single shape (DESIGN.md §5.2).
- **Touch no producer library.** The adapters read the *documented* on-disk formats (SQLite tables, Parquet schema, bundle JSON) directly. They do **not** import `nirs4all`, `dag-ml`, or `dag-ml-data`.
- **Degrade honestly.** Where a producer cannot supply something (e.g. nirs4all has no stable sample id and no OOF attestation), the adapter records that honestly instead of faking it.

Public API:

```python
from nirs4all_benchmarks.adapters import (
    WorkspaceAdapter, workspace_to_exports,   # Adapter A
    bundle_to_export,                          # Adapter B
    extract_n4a_recipe, n4a_pipeline_identity, # .n4a recipe
)
```

Both feed the same ingestion entry point, `nirs4all_benchmarks.ingestion.ingest_export`, which recomputes identities, pseudonymizes sample ids, and writes the Arena store. Identity is always recomputed at ingest — producer UUIDs are never trusted as join keys.

## Adapter A: nirs4all workspace

Source: `adapters/nirs4all_workspace.py`. Class `WorkspaceAdapter`, convenience `workspace_to_exports`. This is the **live** producer today.

### What it reads

`WorkspaceAdapter(workspace_dir)` requires `workspace_dir/store.sqlite` (raises `FileNotFoundError` otherwise) and reads from the frozen workspace schema (PERSISTENCE_FORMATS.md §2.1):

- The **`pipelines`** table — one `ArenaRunExport` is produced per pipeline row. Fields used: `pipeline_id`, `dataset_name`, `dataset_hash`, `expanded_config` (or `original_template` as fallback) for the step list, `name` (carries the identity hash, used as the human label).
- The **`predictions`** table — joined on `pipeline_id`. Supplies `task_type`, `n_samples`, `n_features`, `metric`, `fold_id`, `partition`, the score columns (`val_score`, plus a multi-metric `scores` JSON), and `prediction_id`.
- The per-dataset **`arrays/<dataset_name>.parquet`** — indexed by `prediction_id`, supplying `y_true`, `y_pred`, and the **positional** `sample_indices`. If the parquet is missing, residuals are simply empty (scores still flow).

A pipeline with no predictions is skipped.

### How it maps to `ArenaRunExport`

- **Pipeline.** `expanded_config` is parsed into a flat step list and turned into Arena `pipeline.nodes` (`_steps_to_nodes`); `nirs4all_identity_hash(steps)` reproduces nirs4all's `get_hash` (`sha256(sorted-JSON step list)[:16]`) and is recorded as `pipeline.nirs4all_identity_hash`. The Arena recomputes the canonical topology-aware `pipeline_dag_hash` from these nodes at ingest.
- **Scores.** `_iter_pred_scores` reads the multi-metric `scores` JSON when present, else falls back to `metric`/`val_score`. Each metric becomes a `ScoreObservation` tagged with a `scope` (`test`/`refit`/`cv`, derived from `partition` and whether `fold_id == "final"`) and a `direction` from `scoring.direction_of`. Classification scores flow through unchanged — they are taken from the store, not routed through any regression metric model.
- **Residuals.** Built from the parquet `y_true`/`y_pred`; `residual = y_true - y_pred` only for regression. `_agg` (aggregated) prediction twins are excluded.

### Two honest degradations

These are by design (the module docstring documents both):

1. **Positional residuals.** nirs4all writes positional `sample_indices` with no stable sample identity, so residuals are emitted with `residuals.key = "positional"` and synthetic `pos_<idx>` sample ids. They are queryable per-run but are excluded from cross-run sample comparison until an io ordinal→`SampleId` sidecar exists (PERSISTENCE_FORMATS.md §4.4).

2. **No OOF attestation.** The workspace store carries no out-of-fold guarantee, so the export reports it honestly:

   ```json
   "leakage_attestation": {
     "oof_enforced": false,
     "group_leakage_checked": false,
     "nested_cv_safe": true,
     "unsafe_flags": ["nirs4all_store_no_oof_attestation"]
   }
   ```

### Artifacts are ignored entirely

`artifacts/*.joblib` is **never** read. The Arena is weights-free; the adapter only reads `pipelines`/`predictions` and the prediction arrays.

### Usage

CLI (`ingest-workspace` is Adapter A; it ingests as a `user_run_collection`):

```bash
n4a-benchmarks ingest-workspace ./my-nirs4all-workspace \
  --store ./arena-store \
  --collection nirs4all-workspace
```

Python:

```python
from nirs4all_benchmarks.adapters import WorkspaceAdapter
from nirs4all_benchmarks.ingestion import IngestionPolicy, ingest_export
from nirs4all_benchmarks.store import ArenaStore

policy = IngestionPolicy(collection_id="nirs4all-workspace", collection_kind="user_run_collection")
with ArenaStore("./arena-store") as store:
    for export in WorkspaceAdapter("./my-nirs4all-workspace").iter_exports():
        res = ingest_export(store, export, policy=policy)
        print(res.status, res.validity_status)
```

`workspace_to_exports("./my-nirs4all-workspace")` is the eager equivalent, returning a `list[ArenaRunExport]`.

## Adapter B: dag-ml bundle

Source: `adapters/dagml_bundle.py`. Function `bundle_to_export(...)`. This is the **future** engine: its persisted output is already the Arena's shape, so the mapping is mostly free.

### What it reads

`bundle_to_export` parses the serde JSON dag-ml writes (`bundle.rs` `ExecutionBundle`, schema_version 1) **without importing dag-ml**. The required argument is the bundle; companions are optional and degrade gracefully:

```python
def bundle_to_export(
    bundle,                       # ExecutionBundle JSON: dict | path
    *,
    graph=None,                   # canonical GraphSpec JSON (dict | path)
    dataset_envelope=None,        # io CoordinatorDataPlanEnvelope JSON (dict | path)
    dataset_card=None,            # dict
    metric_reports=None,          # list of RegressionMetricReport dicts
    prediction_blocks=None,       # list of PredictionBlock dicts
    provenance=None,              # {prov_jsonld, ro_crate, openlineage}
    task_type="regression",
    n_samples=None,
    n_features=None,
) -> ArenaRunExport
```

Fields read from the bundle: `graph_fingerprint`, `unsafe_flags`, `bundle_id`, `controller_fingerprint`, `campaign_fingerprint`, `selected_variant_id`, `schema_version`, `refit_artifacts` (presence only, to set the refit strategy), `metadata` (`label`, `root_seed`), and `prediction_caches` (the materialized OOF blocks, used when `prediction_blocks` is not passed explicitly).

- **Prediction blocks → residuals** (`_prediction_residuals`). dag-ml `PredictionBlock`s are **sample-keyed**: each carries `sample_ids`, `values`, `y_true`/`targets`, optional `group_ids`, `partition`, `fold_id`. Residuals are emitted with `residuals.key = "sample_id"` — the native, non-degraded path. `partition` maps to `scope` via `{validation→cv, test→test, final→refit, train→cv}`.
- **Metric reports → score observations** (`_metric_observations`). dag-ml `RegressionMetricReport`s are regression-only; each metric becomes a `ScoreObservation` with a `scope` derived from `partition`/`fold_id`/`level` and `aggregation_level = "target"` when `level == "macro"`.

### OOF enforced by construction

dag-ml enforces out-of-fold safety structurally (PERSISTENCE_FORMATS.md §3.4), so Adapter B asserts it:

```json
"leakage_attestation": {
  "oof_enforced": true,
  "group_leakage_checked": true,
  "nested_cv_safe": true,
  "unsafe_flags": ["...sorted bundle unsafe_flags..."]
}
```

The bundle's `unsafe_flags` set is carried through verbatim (sorted) as the audit trail of any explicit leakage opt-ins.

### Engine graph fingerprint recorded

The bundle's `graph_fingerprint` is recorded as `pipeline.engine_graph_fingerprint` (and `controller_fingerprint` alongside). It is a *secondary id* for verification/drift, never the Arena join key — dedup uses the Arena-computed `pipeline_dag_hash`. When a `graph` JSON is supplied it becomes `pipeline.graph` + `pipeline.nodes`; without it, identity degrades to a single opaque pipeline node `dagml:<graph_fingerprint>`.

Dataset identity comes from the io `CoordinatorDataPlanEnvelope` when supplied: `schema_fingerprint`, `relation_fingerprint`, `plan_fingerprint` populate the dataset block; otherwise `dataset_fingerprint` falls back to a hash of `bundle_id` + `graph_fingerprint`. The producer block records `capsule = "dag-ml"` and `dag_ml_version` from the bundle `schema_version`; RNG `derivation` is recorded as `sha256-hash-chain`.

### Usage

CLI (`ingest-bundle` is Adapter B; dag-ml runs are leakage-safe by construction, so it defaults to ingesting as a `benchmark_release` — pass `--user` to override):

```bash
n4a-benchmarks ingest-bundle ./run/bundle.json \
  --store ./arena-store \
  --graph ./run/graph.json \
  --envelope ./run/envelope.json \
  --collection dag-ml
```

Python:

```python
from nirs4all_benchmarks.adapters import bundle_to_export
from nirs4all_benchmarks.ingestion import IngestionPolicy, ingest_export
from nirs4all_benchmarks.store import ArenaStore

export = bundle_to_export(
    "./run/bundle.json",
    graph="./run/graph.json",
    dataset_envelope="./run/envelope.json",
)
policy = IngestionPolicy(collection_id="dag-ml", collection_kind="benchmark_release")
with ArenaStore("./arena-store") as store:
    res = ingest_export(store, export, policy=policy)
    print(res.status, res.validity_status)
```

## `.n4a` recipe extraction

Source: `adapters/n4a_bundle.py`. A `.n4a` is a ZIP (`manifest.json` + `pipeline.json`/`chain.json` + optional `trace.json` + `artifacts/*`, PERSISTENCE_FORMATS.md §2.3). The Arena accepts it as a pipeline *upload* but is weights-free, so it **always strips the weights** (DESIGN.md §2).

### Weights stripped

`extract_n4a_recipe(path)` opens the ZIP and reads only the JSON entries. Artifact bytes are **never** read — only the `artifacts/*` member names are listed (for the audit trail). It returns:

```python
{
  "manifest": {...},          # manifest.json
  "pipeline": {...} | [...],  # pipeline.json (recipe)
  "steps": [...],             # the DSL step list
  "nodes": [...],             # _steps_to_nodes(steps)
  "stripped_artifacts": [...] # names of artifacts/* that were ignored
}
```

### Identity from `pipeline.json`, not `chain.json`

The recipe step list is taken from `pipeline.json` — a DSL step list — **never** `chain.json`, which holds chain step descriptors + artifact refs and is not a DSL (PERSISTENCE_FORMATS.md §5.3). If `pipeline.json` yields no steps, the loader falls back to `manifest.preprocessing_chain`.

`n4a_pipeline_identity(path)` computes the canonical `PipelineDagIdentity` from those steps via `compute_pipeline_dag_hash`, passing the same step list as `steps_for_identity_hash` so the nirs4all `get_hash` is recorded as a secondary id. With no usable steps it falls back to an `unknown` single-node graph.

### Usage

CLI (`inspect-n4a` extracts and prints the identity; it does not ingest):

```bash
n4a-benchmarks inspect-n4a ./model.n4a
# steps: 5  stripped artifacts: 6
# pipeline_dag_hash: <64-hex>
# nirs4all_identity_hash: <16-hex>
```

Python:

```python
from nirs4all_benchmarks.adapters import extract_n4a_recipe, n4a_pipeline_identity

recipe = extract_n4a_recipe("./model.n4a")
print(len(recipe["steps"]), "steps;", len(recipe["stripped_artifacts"]), "artifacts ignored")

ident = n4a_pipeline_identity("./model.n4a")
print(ident.pipeline_dag_hash, ident.nirs4all_identity_hash)
```

## Side-by-side comparison

Distilled from PERSISTENCE_FORMATS.md §5.1.

| Dimension | Adapter A: nirs4all workspace | Adapter B: dag-ml bundle |
|---|---|---|
| Status | Live today | Future engine |
| On-disk source | `store.sqlite` + `arrays/*.parquet` (+ `artifacts/*.joblib`, ignored) | one JSON `ExecutionBundle` + optional graph / envelope / metric reports / prediction blocks |
| Entry point | `WorkspaceAdapter.iter_exports()` / `workspace_to_exports()` | `bundle_to_export()` |
| Exports per source | one `ArenaRunExport` per `pipelines` row | one `ArenaRunExport` per bundle |
| Producer library imported | none | none |
| Pipeline model | linear step list (from `expanded_config`) | DAG (`GraphSpec`, when `graph` supplied; else opaque node) |
| Engine fingerprint recorded | `nirs4all_identity_hash` (`get_hash`, 16-hex) | `engine_graph_fingerprint` (dag-ml `graph_fingerprint`, 64-hex) + `controller_fingerprint` |
| Residual key | `positional` (synthetic `pos_<idx>` ids) | `sample_id` (native stable ids) |
| Residual coverage | regression `y_true - y_pred`; `_agg` twins excluded | sample-keyed, from `PredictionBlock`s / `prediction_caches` |
| Scores | from `predictions` (`val/test/train`, multi-metric JSON); any task type | from `RegressionMetricReport` (regression only) |
| `oof_enforced` | `false` (honest; `unsafe_flags: ["nirs4all_store_no_oof_attestation"]`) | `true` (by construction; bundle `unsafe_flags` carried through) |
| Dataset identity | `dataset_name` / `dataset_hash` fingerprint | io `CoordinatorDataPlanEnvelope` (schema / relation / plan fingerprints), else bundle-derived fallback |
| Artifacts/weights | ignored entirely | referenced only; not read |
| Default collection kind (CLI) | `user_run_collection` | `benchmark_release` |

## What happens after the adapter

All three paths converge on `ingest_export` (`ingestion/ingest.py`), which is producer-agnostic. Regardless of source it:

- recomputes the canonical `pipeline_dag_hash` from the export's `graph`/`nodes`, recording the producer's own hashes (`engine_graph_fingerprint`, `nirs4all_identity_hash`) as secondary ids only;
- pseudonymizes every `sample_id`/`group_id` at ingest (so the `pos_<idx>` ids from Adapter A and the native ids from Adapter B are both mapped) and stores residuals with `pseudonymized = 1`;
- is idempotent on `input_export_hash` + collection + schema version (re-ingesting returns `already_ingested`);
- quarantines on leakage when the collection policy requires it — which is why dag-ml bundles (leakage-safe) default to release ingestion and nirs4all workspaces (no OOF attestation) default to user-run ingestion.
