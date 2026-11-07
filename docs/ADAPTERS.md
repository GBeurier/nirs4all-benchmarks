# Producer adapters: dual compatibility

> Current runtime authority is the **Sole v2** section appended below. All preceding v1/default and review-checkpoint descriptions are retained historical references, not current compatibility or scientific evidence.


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


## Offline review amendments

# Arena producer adapters

Adapters produce the existing `ArenaRunExport` v1 contract without importing the producer runtime. Arena validates metadata and records owner declarations; it does not verify scientific execution, rebuild a dataset or run a new scorer. Missing evidence is unreported or explicitly refused.

## DAG-ML bundles

`bundle_to_export` accepts the existing bundle JSON and optional graph/prediction/metric companions. Generic bundle `status: ok`, graph fingerprints, campaign fingerprints and refit-artifact presence do not prove completed execution, fold coverage, OOF safety or a specific refit strategy.

Supply owner context as an actual ArenaRunExport, rather than a new envelope format:

```python
from nirs4all_benchmarks.adapters import bundle_to_export

export = bundle_to_export(
    "bundle.json",
    owner_export="owner_arena_export.json",
    graph="graph.json",                  # optional; owner graph can supply it
    dataset_envelope="io-envelope.json",  # optional schema/relation/plan metadata
)
```

```bash
n4a-benchmarks ingest-bundle bundle.json --metadata owner_arena_export.json \
  --store /chosen/private/arena --collection local
```

The owner manifest is metadata-validated without opening a store. Its genuine dataset/task/variant identities, explicit execution block, score context, split/CV/RNG/refit metadata, reported attestations and provenance are carried through. Bundle/source metadata digests preserve lineage. A schema-only IO envelope cannot substitute for a scientific dataset fingerprint. Without a native owner manifest, Python callers must supply a real `dataset_fingerprint` in `dataset_envelope` and native `task` / `dataset_variant` contract blocks explicitly; missing identities raise a specific refusal. There is no bundle-id, graph-id or dataset-name-derived scientific identity fallback.

The default is unreported completion and no reported safety attestations (`reported_fields: []`). Only an explicitly supplied Arena execution block can declare completion; the adapter does not interpret generic DAG-ML success as completion. A reported generic bundle failure/cancellation is retained conservatively. Visibility defaults to explicit `unreported`. Public visibility requires both `visibility: public` and `visibility_source: producer_reported`; a normalized legacy public default is unknown. CLI ingestion defaults to a user collection; `--release` enables the existing OOF quarantine policy.

Scalar single-target prediction blocks preserve sample ids until ingestion pseudonymizes them. Inconsistent lengths, unsupported partitions and multi-target vectors refuse instead of truncating silently. Numeric metric reports preserve fold, scope, partition, aggregation, unit, sample count and coverage; directions use the existing registry. Native owner observations keep their declared score context. Expected folds come only from explicit owner metadata. A bundle schema version is recorded as a schema version, never invented as a DAG-ML library version. No actual owner runtime completion semantics were established by these metadata tests.

## nirs4all workspace

`WorkspaceAdapter` / `workspace_to_exports` read the documented SQLite prediction tables and Parquet arrays. They require a genuine 64-hex `dataset_hash` and reuse it directly; missing or incompatible identity refuses instead of hashing the dataset name. Exports are grouped by model/branch. A pipeline without predictions can still export an explicit failure/unreported outcome when source task/variant identities are also present; otherwise it refuses.

Status is read from available pipeline metadata, never hard-coded to success. Expected fold count comes only from explicit `expected_fold_count`, not the number of prediction folds. Missing OOF attestations use `reported_fields: []`; old false/true boolean defaults are hash compatibility values, not fabricated safety evidence. Visibility defaults to explicit `unreported`; legacy unmarked public values never authorize publication. Positional residuals remain explicitly degraded and cannot establish cross-run scientific sample identity. The adapter does not read fitted model artifacts.

```bash
n4a-benchmarks ingest-workspace /chosen/source-workspace --store /chosen/private/arena
```

Missing genuine source identity may therefore prevent exporting a failed or incomplete workspace run. That refusal is a contract limitation, not a successful ingestion.

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

## After adaptation

Use `validate_metadata(export)` for store-free metadata checking and `ingest_export(store, export)` for local idempotent ingestion. Claims use the normalized `ArenaRunExport.to_manifest()` hash domain. Storage keeps allowlisted metadata receipts, not raw exports. Re-import originals, not receipts. [The ingestion guide](INGESTION.md) describes terminal filtering, legacy-file refusal, privacy boundaries and exact focused checks.

### WORK review evidence rules

Defaults are explicit `unreported` visibility, no reported safety fields, and unreported terminal completion. Public output requires a genuine owner declaration marked `visibility_source: producer_reported`; neither an IO/schema default nor a bundle success flag supplies it. Registry-derived metric directions are labelled as registry metadata, not fabricated producer declarations. Known registry directions can resolve omitted legacy directions without changing the v1 hash key. Custom metrics need a supplied direction for ranking. Native v1 hash defaults remain intact; absent extensions default to null. See [the B1–B5 rules and exact checks](INGESTION.md#work-review-corrections-b1b5).


### D1 owner fold sentinel correction

Workspace fold identity uses the local owner-mirrored rule in `contract/folds.py`:
`final`, `final_agg`, `avg`, `w_avg`, `ensemble`, `all`, `macro`, `fold_avg`,
`fold_w_avg`, and any id ending in `_agg` are pseudo-folds. Their scores remain
reported with no real fold id. Aggregate prediction twins are excluded from CV
residual rows; `final` refit predictions remain separately scoped. Arbitrary
genuine string ids are retained, without importing nirs4all. Offline metadata
coverage uses the same rule after scope/partition filtering, so aggregates cannot
complete 3/5 folds or invalidate genuine 5/5 coverage. See the
[D1–D3 checks and remaining limits](INGESTION.md#d1d3-targeted-corrections-and-verification).
All original adapter documentation remains preserved; these evidence rules amend
the older descriptive examples above.


### E1–E2 workspace score correction

The owner-mirrored predicate converts string/integer ids with `str(...).strip()`;
empty/whitespace ids are pseudo-folds. Genuine string ids remain valid and integer
SQLite ids no longer depend on column affinity. Owner source references are in
`contract/folds.py` (reduction's `is_real_cv_fold` and storage's
`target_domain_summary.py`). Workspace `final`/`final_agg` scores use refit scope
and retain the source partition (R1 correction); other pseudo scores use view
scope with their original partition. Values remain queryable and cannot contaminate the ranked CV mean.
See the [E1–E2 checks](INGESTION.md#e1e2-fold-normalization-and-score-scope-correction)
for exact commands, measured scratch/disk usage and outstanding review status.


### R1 actual owner partition and score shapes

The owner stores `val` (normalized to `validation`), separate final train/test
rows, and a full nested `scores[partition][metric]` map on each partition row.
The adapter selects the row's own metrics, retains source partition under refit
scope, and uses the matching scalar column for fallback. It preserves every
supported reported score, including null values. Invalid or ambiguous contexts
refuse explicitly. An owner `completed` status is explicit evidence for metadata
completion; missing expected-fold totals and absent statuses remain unreported.
Prediction counts describe partitions and cannot establish dataset cardinality.

CV queries default to validation; refit aggregation requires `partition='train'`,
`partition='test'` or another explicit canonical partition. Residual inspection
accepts scope/partition filters and keeps positional context distinct; unsupported
keys/arrays refuse rather than receive fabricated identities. Branch zero and
missing branch are separate. Unsupported multiple-chain grouping refuses.
Static packs requiring partition selection refuse pending a separate consumer
update; no site/frontend change or build is included.

[Read-only owner evidence, exact checks and limitations](INGESTION.md#r1-owner-partitions-and-explicit-aggregation-context)
document the inspected source pin and the synthetic owner-schema fixtures.
These rules amend the older descriptive adapter examples while retaining all
original documentation. Metadata compatibility does not verify scientific
identity, execution, safety, completeness or evaluation correctness.

## Sole v2 adapter amendments (4 November 2026)

This section supersedes the v1/default/hash compatibility descriptions above.
The original reference is retained intact. See the [current v2 ingestion rules](INGESTION.md#sole-v2-offline-contract-4-november-2026)
for exact fields, identity basis, ranking lenses, frozen-history mapping and checks.
Adapters produce only explicit wire version 2. No legacy version is upgraded.

Workspace adaptation is metadata-only and preflights the complete input before
its first yield. It mirrors all eight generation keywords from owner
`nirs4all/pipeline/config/_generator/keywords.py` at `3a2013b9`; even one unresolved
keyword refuses `workspace_unresolved_generator`. Different owner names resolving
to one run condition refuse `workspace_identity_collision`, both during adapter
preflight and under the store write transaction across separate ingests. Distinct
resolved pipelines or datasets are not rejected merely for sharing a template.
The current SDK fix is separate; Arena never invents realized pipeline choices.

Owner `val` becomes `validation`. Train/validation/test scores retain their source
partitions; `final@train` and `final@test` are distinct refit observations. Exact
aggregate labels, including whitespace, remain in `source_fold_id`. `final` and
`final_agg` use refit scope; other pseudo-folds use view scope; `_agg` twins use
group level. Pseudo-fold recognition strips string/integer input, excludes empty
ids, the owner sentinels plus macro/fold-average sentinels and every `_agg` suffix,
and retains arbitrary genuine fold strings. It never counts aggregates or test
pseudo-ids as missing real CV folds. For workspaces, `sample` means an unreduced
owner prediction row, not established biological sample aggregation. Explicit
expected fold counts must agree; absent expectations in actual owner stores stay
unreported. Known directions use the registry; qualified unknown metrics such as
`r2:y` have no guessed direction.

Read-only owner references are `data/reduction.py:is_real_cv_fold` and
`prediction_scope_from_legacy`, `pipeline/storage/target_domain_summary.py`,
`pipeline/storage/store_schema.py`, `workspace_store.py:save_prediction`, and
`controllers/models/base_model.py:_assemble_prediction_result` /
`_add_all_predictions`. The latter persists each partition separately. These are
source references, not evidence that a producer engine was executed in this batch.

Task type must be present. Binary/multiclass owner aliases normalize explicitly;
bare `classification` refuses `workspace_classification_task_unsupported`. The
owner feature hash remains an owner declaration. Arena-derived task/variant
identities have machine-readable inputs in a separate Arena namespace and bind
actual ordered target context; no arrays means an opaque unranked record. A
missing identity on a failed run may require explicit refusal rather than a
fabricated dataset/task. Workspace bytes are read into a private SQLite snapshot;
nonempty WAL sources, mismatched residual context or unsupported shapes refuse.

DAG reports from multiple variants refuse `dagml_variant_context_unsupported`
before flattening or intake. Native v2 owner metadata/identity basis is required;
a schema or bundle fingerprint is not a scientific dataset identity. Missing
report context remains unreported rather than CV/validation/sample. Single-label
and report content provenance is retained, and output metadata is validated.
Unsupported classification/prediction shapes are typed refusals, not fake
regression results. Aggregate reports require their actual level/scope and exact
source label. Bundle success does not supply terminal completion, safety,
visibility or expected fold counts. No new scorer, scientific parser, assembly,
producer import, fit or remote delivery is used.

Partial target arrays are also opaque/unranked; they cannot establish a comparable
task from a few available targets. Duplicate prediction-array IDs, missing or
malformed pipeline configuration, and unsupported nonempty probability shapes
refuse. No missing score is automatically recomputed during metadata-only intake.

## DAG-ML training-outcome path (7 November 2026)

This section adds one explicit path to the Sole v2 adapter; the bundle path above is unchanged.

**Default bundle path.** `bundle_to_export(bundle, ...)` maps bundle/owner metadata and never imports the
`dag_ml` runtime.

**Explicit `training_outcome` path.**
`bundle_to_export(training_outcome=<path>, dataset_envelope=..., task=..., dataset_variant=..., identity_basis=..., execution=..., leakage_attestation=..., score_context=..., ...)`.

- **Input.** The path (`str` or `Path`) of a retained DAG-ML `TrainingOutcome` JSON file. It cannot be combined
  with `bundle`, `graph`, `metric_reports`, `prediction_blocks` or `owner_export`; doing so raises
  `dagml_training_outcome_input_conflict`.
- **Owner API.** Only this path lazily imports the optional `dag_ml` package and uses its public
  `validate_training_outcome_json`, `fold_set_fingerprint_json` and `DagMlError`. There is no fallback and no
  local reimplementation.
  - `dag_ml` absent → `dagml_runtime_required_for_fold_identity`.
  - Any of the three names missing → `dagml_fold_identity_api_unsupported`.
  - An outcome the owner rejects (strict JSON or `outcome_fingerprint`) → `dagml_training_outcome_invalid`.
  - Whether a given installed version accepts a given outcome schema is decided by that owner call, not by Arena.
- **Supported outcomes.** One variant with no `parameter_patches`, a `FoldSet`, and a single
  `relation_fingerprint` shared by all `data_identities`. Anything else is refused with a named code. A
  declared `dataset_envelope.relation_fingerprint` must match (`dagml_outcome_relation_mismatch`).
- **Identity.** `cv.fold_set_fingerprint` is the owner's canonical fingerprint of the outcome's `FoldSet`;
  `dataset.relation_fingerprint` comes from the outcome. Dataset, task and variant identities, execution,
  attestations and score context remain explicit caller declarations, exactly as on the bundle path.
- **Scores.** Only reports of the selection output's producer and the selected variant are mapped.
  - Real fold ids at partition `validation` become `cv` observations keeping their `fold_id`.
  - Native pooled `avg`/`w_avg` reports become `view` observations with `source_fold_id`.
  - Unfolded `final`/`test` reports become `refit`/`test`.
  - Every other report or metric is listed under `provenance.dagml_unmapped_score_reports`: fold-level
    `train`/`train_pool`/`test`, other levels or grouping keys, other producers or variants, unknown folds,
    and metric names outside the registry. Nothing is dropped silently and no score is recomputed.
- **Graph.** The DAG `GraphSpec` is expressed in Arena graph keys without dropping fields. A structured
  operator is kept as canonical JSON text, so its steps and parameters stay in the pipeline identity.
- **Output.** Scores and identity metadata only: no predictions or residuals, prediction caches, artifacts,
  fitted state or outcome blob. The `ExecutionBundle` contributes only scalar identity fields.

**Ranking limits.** A bound fold set replaces the absent outer-split identity only for `cv`/`fold` requests
at partition `validation`. `train`, `refit`, `test`, `view`, `final` and the `split` dimension remain refused.
See [QUALITY_RANKING.md](QUALITY_RANKING.md) for the scope table.

**Evidence status.** The repository test outcome
(`tests/fixtures/dagml_outcome_native_role/training_outcome.json`) is a DAG-ML production-path test fixture.
It is identity and mapping evidence only, not an executed scientific run. Its single-candidate result shows
plumbing, not a qualified ranking.

**Dependency floor.** The optional `dagml` extra declares `dag-ml>=0.4.0`. This is the selected floor: the
dag-ml 0.4 cohort chosen to execute and qualify this path, not the first release that introduced these APIs.
The three names were also observed statically in installed `dag_ml` 0.3.23 stubs, but older versions carry no
compatibility obligation or schema qualification. Whether an installed version accepts a given outcome remains
decided by the owner call at runtime. No upper bound is set.
