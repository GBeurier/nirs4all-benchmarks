# Ingestion

> Current runtime authority is the **Sole v2** section appended below. All preceding v1/default and review-checkpoint descriptions are retained historical references, not current compatibility or scientific evidence.


The ingestion pipeline turns one `ArenaRunExport` into rows in an `ArenaStore`: idempotent, leakage-honest, privacy-first, and never trusting producer-supplied UUIDs. This is the reference for `nirs4all_benchmarks.ingestion` — what the state machine does, how `IngestionPolicy` controls it, and what `IngestionResult` reports.

## Contents

- [Overview](#overview)
- [The state machine](#the-state-machine)
  - [1. VERIFY](#1-verify)
  - [2. DEDUP-CHECK](#2-dedup-check)
  - [3. RESOLVE IDS](#3-resolve-ids)
  - [4. VALIDATE](#4-validate)
  - [5/6. PSEUDONYMIZE + STRIP](#56-pseudonymize--strip)
  - [7. STAGE → COMMIT](#7-stage--commit)
  - [8. REPORT](#8-report)
- [The unified upload state machine](#the-unified-upload-state-machine)
  - [Accepted inputs](#accepted-inputs)
  - [The run / store / display decision](#the-run--store--display-decision)
  - [register_pipeline + planned_runs](#register_pipeline--planned_runs)
  - [Role-aware run_facets at ingest](#role-aware-run_facets-at-ingest)
  - [UploadResult](#uploadresult)
  - [Upload examples](#upload-examples)
- [IngestionPolicy](#ingestionpolicy)
- [IngestionResult](#ingestionresult)
- [Idempotency](#idempotency)
- [Leakage: quarantine vs. flag](#leakage-quarantine-vs-flag)
- [Pseudonymization](#pseudonymization)
- [Score recomputation and supersede](#score-recomputation-and-supersede)
- [The clean report](#the-clean-report)
- [Examples](#examples)
  - [Python: ingest an export](#python-ingest-an-export)
  - [CLI: ingest-export](#cli-ingest-export)
  - [CLI: ingest-workspace](#cli-ingest-workspace)
  - [CLI: ingest-bundle](#cli-ingest-bundle)

## Overview

The single public entry point is `ingest_export`:

```python
from nirs4all_benchmarks.ingestion import IngestionPolicy, IngestionResult, ingest_export
from nirs4all_benchmarks.store import ArenaStore

with ArenaStore("./arena-store") as store:
    result: IngestionResult = ingest_export(store, "run_export.json")
```

The `export` argument is flexible: an `ArenaRunExport` model, a plain `dict`, or a path (`str`/`Path`) to a manifest JSON. When given a path, the parent directory becomes the base for resolving any out-of-line `residuals.ref` Parquet file.

Everything else in `ingestion/` is internal machinery that `ingest_export` orchestrates, one module per phase:

| Phase | Module | Key symbol |
|---|---|---|
| VERIFY, DEDUP, STAGE→COMMIT | `ingestion/ingest.py` | `ingest_export` |
| RESOLVE IDS | `ingestion/resolve.py` | `resolve_identities` → `ResolvedExport` |
| VALIDATE | `ingestion/validate.py` | `validate_export` → `ValidationOutcome` |
| PSEUDONYMIZE | `ingestion/pseudonymize.py` | `Pseudonymizer` |
| REPORT | `ingestion/report.py` | `CleanReport` |

## The state machine

`ingest_export` walks the phases described in `DATA_MANAGEMENT.md` §5:

```
VERIFY → DEDUP-CHECK → RESOLVE IDS → VALIDATE → PSEUDONYMIZE → STRIP → STAGE→COMMIT → REPORT
```

A fresh batch id (`ib_<uuid4 hex>`) is allocated up front and travels through all phases.

### 1. VERIFY

The export is loaded into a typed `ArenaRunExport` (`_load_export`) and gated twice:

- **Schema version.** `model.arena_export_schema_version` must equal `ARENA_EXPORT_SCHEMA_VERSION`. A mismatch returns `status="rejected"` with code `unsupported_export_version` — nothing is written.
- **JSON Schema.** `model.to_manifest()` produces the canonical manifest (`model_dump(mode="json", exclude_none=True)`), which is checked against the wire contract via `validate_manifest`. Any errors return `status="rejected"` with code `schema_invalid`.

If both pass, the **idempotency key** is computed: `export_hash = compute_export_hash(manifest)`, i.e. `sha256(canonical_json(manifest))` with the `arena_export_hash` field itself excluded so the hash is a fixed point.

### 2. DEDUP-CHECK

The store is queried for an existing batch with the same `(input_export_hash, target_collection, arena_schema_version)`:

```sql
SELECT * FROM ingestion_batches
WHERE input_export_hash = ? AND target_collection = ? AND arena_schema_version = ?
```

If a prior batch exists and its `status` is `committed` or `quarantined`, ingestion short-circuits: it reads back the associated execution's `execution_hash`, `run_condition_hash`, and `validity_status`, and returns `status="already_ingested"` with the original `clean_report`. Nothing is re-written. (A prior `rejected`/`staging` batch does not block a retry.)

### 3. RESOLVE IDS

`resolve_identities(model)` builds a `ResolvedExport` — the full identity set plus the extracted dimension rows. The Arena **never trusts producer UUIDs**; it splits hashes into two classes (`ingestion/resolve.py` docstring):

- **Adopted** (producer-reported opaque values that Arena does not independently verify): `dataset_fingerprint`, the io `schema`/`relation`/`plan` fingerprints, `cv_instance_hash`, `cv.fold_set_fingerprint`, `split_instance_hash`, `engine_graph_fingerprint`, `target_hash`. Producer values are opaque and unverified. Where an instance hash is omitted, Arena derives a declared-condition key from the spec and summary, not a realized assignment.
- **Recomputed** (Arena-owned, for cross-producer dedup correctness): `pipeline_dag_hash` (normalized Merkle graph), `task_hash`, `dataset_variant_hash`, `split_spec_hash`, `cv_spec_hash`, `rng_context_hash`, `refit_strategy_hash`, `score_computation_hash`, and the composed `run_condition_hash`.

The pipeline graph is normalized (from `pipeline.graph`, or `pipeline.nodes`, or a single opaque node if neither is present) and exploded into `operator_specs`, `parameter_values`, `pipeline_nodes`, `pipeline_node_params`, and `pipeline_edges` rows. The `run_condition_hash` is composed from its six component hashes in fixed labelled order. Finally an `execution_hash` is derived from `{run_condition, export_hash, execution_id}` so two genuinely distinct runs of the same condition get distinct executions, but a re-ingest of the same export collapses.

### 4. VALIDATE

`validate_export(model, resolved, quarantine_on_leakage=...)` returns a `ValidationOutcome` carrying `issues`, a `validity_status` (`valid` / `quarantined` / `rejected`), and the `residual_key` (`sample_id` or `positional`). The checks:

| Check | Code | Effect |
|---|---|---|
| `dataset.dataset_fingerprint` missing | `no_dataset_identity` | reject |
| `task.task_type` not in `{regression, binary, multiclass, multilabel}` | `bad_task_type` | reject |
| `cv.n_folds > dataset.n_samples` (when both known) | `cv_inconsistent` | reject |
| no score observations | `no_scores` | warning |
| `leakage_attestation.oof_enforced` is false | `leakage_unattested` | quarantine **or** warning (see below) |
| `leakage_attestation.unsafe_flags` non-empty | `unsafe_flags` | warning |
| `residuals.key == "positional"` | `degraded_residual_keying` | warning, sets `residual_key="positional"` |

If the outcome is **rejected**, a single `ingestion_batches` row is written with `status="rejected"` (carrying the report and issues), committed, and `ingest_export` returns immediately. No dimensions or facts are written.

### 5/6. PSEUDONYMIZE + STRIP

These two phases run together over the residual rows:

1. **Load residuals.** `_residual_rows` returns `residuals.inline` if present, otherwise reads `residuals.ref` as a Parquet table relative to the export's directory. If neither is available, residuals are empty (scores-only).
2. **Cap check (STRIP).** If the row count exceeds `policy.residual_row_cap`, residuals are dropped entirely (stored scores-only); `report.residuals_truncated` is set and a note recorded. Otherwise rows are prepared.
3. **Prepare + pseudonymize.** `_prepare_residual_rows`:
   - Pseudonymizes `sample_id`, `group_id`, and `origin_sample_id` via the store-global `Pseudonymizer`.
   - Honors the `residuals.publishable` flags: `y_true`, `y_pred`, `y_proba` are each set to `None` when not publishable; dropped fields are noted in `report.dropped_fields` (e.g. `residuals.y_true (publication policy)`).
   - For regression, fills a missing `residual` as `y_true - y_pred`.
   - Normalizes `scope` (default `cv`), `partition` (default `validation`), and coerces numeric fields.

Note that the engine *sanitizes* ids but does not pseudonymize them; the Arena does that here at ingest.

### 7. STAGE → COMMIT

Everything below happens inside a single `store.transaction()` (atomic — any exception rolls the whole thing back):

1. `ensure_collection(policy.collection_id, kind=policy.collection_kind)`.
2. **Stage** the batch first (`status="staging"`) so fact rows can satisfy their foreign key to `ingestion_batches`.
3. `_write_dimensions` — upsert every dimension (dataset card/fingerprint, task spec, variant, pipeline DAG + nodes/edges/operators/params, split/CV/RNG/refit specs and instances, score-computation spec). All dimension PKs are content hashes, so `INSERT OR IGNORE` makes a repeat a no-op; the report tallies new vs. deduped rows.
4. `_write_facts` — upsert `run_conditions`, the `executions` row (stamped with `validity_status`), the `score_sets` row and its `metric_observations`, and (if any) the `residual_sets` row plus the residual Parquet file written by `store.residuals.write(...)`.
5. **Finalize** the batch row: `status` becomes `committed` (or `quarantined` if validation quarantined it), with `n_dimensions`, `n_facts`, the serialized clean report, and the issues.
6. If `policy.store_export_bundle` is true, the canonical manifest is written to `<store>/exports/arena_run_<export_hash>.json` for audit/replay.

The final `status` is `quarantined` when validation marked the execution quarantined, otherwise `committed`.

### 8. REPORT

`ingest_export` returns an `IngestionResult` whose `clean_report` is the `CleanReport.to_json()` snapshot recorded on the batch. The same JSON is persisted in `ingestion_batches.clean_report_json`.

## The unified upload state machine

`ingest_export` is the *results* path: it requires a fully-formed `ArenaRunExport`. But a user often has something looser — a `.n4a` bundle, a raw pipeline recipe, a dag-ml bundle — and just wants to hand it to the Arena and let it decide what to do. That is `nirs4all_benchmarks.ingestion.upload` (`ingestion/upload.py`): one auto-detecting entry point that wraps `ingest_export` for results and adds a *register + plan* path for bare pipelines.

```python
from nirs4all_benchmarks.ingestion import upload, register_pipeline, UploadResult
from nirs4all_benchmarks.store import ArenaStore

with ArenaStore("./arena-store") as store:
    result: UploadResult = upload(store, "my_pipeline.n4a", target_datasets=["my-dataset"])
```

The Arena itself **never runs compute** (DESIGN.md non-objectives). For results it ingests; for a bare pipeline it registers the recipe and, against each target dataset, either marks the run as *already run* or records a *planned run* that a runner fulfils later by ingesting the actual execution.

### Accepted inputs

`upload(store, payload, ...)` sniffs `payload` and routes it. Detection order (the first match wins):

| Detected as | How it is recognized | What happens |
|---|---|---|
| **path on disk** | `str`/`Path` that exists and isn't recipe text (`_looks_like_path`) | a `.n4a` zip → strip weights + register; any other file → read as UTF-8 text and recurse |
| **raw text** | a `str` that isn't a path | `_load_text_recipe` parses it as JSON, then falls back to YAML, then recurses on the parsed object |
| **`ArenaRunExport`** | an `ArenaRunExport` instance, or a `dict` carrying `arena_export_schema_version` | `ingest_export` with a policy keyed to `collection_id` + `as_release` → `kind="arena_export"` |
| **dag-ml `ExecutionBundle`** | a `dict` carrying `graph_fingerprint` or `bundle_id` | `bundle_to_export(payload)` then recurse as an export |
| **pipeline recipe** | any remaining `list`, or `dict` with `pipeline` / `steps` / `nodes` | `register_pipeline` → `kind="pipeline"` |

`.n4a` handling goes through `adapters/n4a_bundle.extract_n4a_recipe`: a `.n4a` is a ZIP of `manifest.json` + `pipeline.json`/`chain.json` + `artifacts/*`. Only the recipe is read — **artifact bytes are never opened**; `artifacts/*` entries are listed by name as `stripped_artifacts` for the audit trail. Identity is taken from `pipeline.json` (a DSL step list), never `chain.json` (§5.3). So a `.n4a` exported *with* fitted weights and one exported *without* register to the same `pipeline_dag_hash`; the only difference is the reported strip count. A bare recipe — a step `list`, or a `{pipeline|steps|nodes}` dict — is normalized by `_recipe_to_pipeline_source` into a `(graph_source, steps, label, n_stripped)` tuple before registration.

### The run / store / display decision

Once a payload is classified, the outcome is one of three:

```
upload(payload)
  ├── results (ArenaRunExport / dag-ml bundle) ──► ingest_export ──► ingested | rejected
  └── pipeline recipe (.n4a / list / JSON / YAML) ──► register_pipeline
                                                       └── per target dataset:
                                                            ├── valid execution exists ──► already_run   (display)
                                                            └── none yet               ──► planned        (runner fulfils later)
```

For a results payload the decision is `ingest_export`'s own state machine above; `UploadResult.status` is `ingested` when `IngestionResult.ok` (committed / already_ingested / quarantined), else `rejected`. For a pipeline payload the decision is made per dataset, described next.

### register_pipeline + planned_runs

`register_pipeline(store, recipe, *, collection_id="uploads", target_datasets=None, source="upload", human_label=None, stripped_artifacts=0)` does two things inside one `store.transaction()`:

1. **Register the recipe as a dimension.** It computes `pipeline_dag_hash` (`compute_pipeline_dag_hash`), explodes the normalized graph with `_extract_pipeline_rows`, and upserts the `pipeline_dags` row plus its `operator_specs`, `parameter_values`, `pipeline_nodes`, `pipeline_node_params`, and `pipeline_edges` — the same dimension tables `ingest_export` writes, so a later real execution of this pipeline dedups straight onto these rows. The collection is auto-created as a `user_run_collection`.

2. **Plan or detect per target dataset.** Each `target_datasets` token is resolved to a fingerprint by `_resolve_dataset_fingerprint` (a 64-hex token is already a fingerprint; otherwise it looks up `dataset_cards` by `name`/`dataset_id`; failing that, it derives a deterministic `fingerprint({"dataset_token": token})`). Then it queries for a **valid** execution of this `(pipeline_dag_hash, dataset_fingerprint)`:

   ```sql
   SELECT COUNT(*) n FROM run_conditions rc
   JOIN executions e ON e.run_condition_hash = rc.run_condition_hash
   WHERE rc.pipeline_dag_hash = ? AND rc.dataset_fingerprint = ? AND e.validity_status = 'valid'
   ```

   - **already run** → the dataset entry is `{status: "already_run", n_executions: n}`. Nothing is planned.
   - **not run yet** → a row is upserted into `planned_runs` with a deterministic `plan_id` (`plan_<16 hex>` over pipeline + dataset + collection) and `status="planned"`, and the entry is `{status: "planned", n_executions: 0, plan_id}`.

The `planned_runs` table (`store/schema.sql`) records these not-yet-run conditions:

```sql
CREATE TABLE IF NOT EXISTS planned_runs (
    plan_id             TEXT PRIMARY KEY,
    pipeline_dag_hash   TEXT REFERENCES pipeline_dags (pipeline_dag_hash),
    dataset_fingerprint TEXT,
    task_hash           TEXT,
    collection_id       TEXT,
    status              TEXT NOT NULL DEFAULT 'planned',  -- planned | fulfilled
    source              TEXT,
    created_at          TEXT NOT NULL,
    UNIQUE (pipeline_dag_hash, dataset_fingerprint, collection_id)
);
```

**Fulfillment.** A planned run is fulfilled the moment a *real* execution for the same `(pipeline, dataset)` is ingested. Inside `ingest_export`'s fact-writing phase (`ingestion/ingest.py`), right after the `run_conditions` row is written:

```python
store.conn.execute(
    "UPDATE planned_runs SET status = 'fulfilled' WHERE pipeline_dag_hash = ? AND dataset_fingerprint = ?",
    (r.pipeline_dag_hash, r.dataset_fingerprint),
)
```

So the lifecycle is `register → planned → (runner executes + exports) → ingest_export → fulfilled`. The Arena never executes the pipeline itself; it only tracks the plan and recognizes its fulfillment.

### Role-aware run_facets at ingest

Independently of upload, every `ingest_export` now materializes a long-format `run_facets` table for each run condition, so the dataviz can slice the benchmark space by *any* dimension (split / CV / seed / refit / dataset / model family / preprocessing / augmentation / per-parameter sweeps) without re-walking the DAG. This happens in the same fact-writing phase, right after the `run_conditions` upsert:

```python
from nirs4all_benchmarks.indexing import build_run_facets

for facet in build_run_facets(r, model):
    store.upsert("run_facets", facet)
```

`indexing.classify_role(operator, declared_kind)` buckets each node into a stage *role* — `preprocessing`, `augmentation`, `scaler`, `feature_selection`, `model`, `merge`, `split`, `sampler`, `input`, `other`. A declared graph `kind` wins via `_KIND_TO_ROLE`; otherwise substring heuristics run over the **leaf** of the dotted entrypoint (the class name), so `sklearn.ensemble` can't masquerade a `RandomForestRegressor` as a merge node and a `PowerTransformer` stays preprocessing rather than being caught by a neural "transformer" needle. Augmentation is checked against the full path first, because its signal is usually in the module (e.g. `nirs4all.augmentation.RandomShift`).

`indexing.build_run_facets(resolved, model)` then emits deduped `{run_condition_hash, facet_key, facet_value, facet_num, role}` rows:

- **condition-level facets**: `dataset`, `task_type`, `split_method`, `cv_method`, `n_folds`, `seed`, `refit_strategy`, `pipeline`, `is_linear`.
- **stage facets** per node: a presence row `{role}_op` (e.g. `preprocessing_op = SNV`) plus a generic `operator` row, `merge_strategy` for merge nodes, and per-parameter `param:<name>` rows (numeric params carry `facet_num` for ordered/sweep plots).
- **rollups**: `model` / `model_family`, `n_models`, `n_preprocessing`, `n_augmentation`, `has_augmentation`, `has_scaler`, `has_feature_selection`, `n_stages`.

The table is keyed `PRIMARY KEY (run_condition_hash, facet_key, facet_value)`, so a repeated stage operator collapses to one row and re-ingest is idempotent. `Queries.facets` / `facet_values` / `pivot` / `parallel` read this table for the faceted dataviz, and `Queries.planned` exposes the `planned_runs` backlog.

### UploadResult

```python
@dataclass
class UploadResult:
    kind: str            # arena_export | dagml_bundle | pipeline | unknown
    status: str          # ingested | registered | rejected
    pipeline_dag_hash: str | None = None
    pipeline_label: str | None = None
    stripped_artifacts: int = 0
    datasets: list[dict[str, Any]] = field(default_factory=list)  # [{dataset, token, status, n_executions, plan_id}]
    ingestion: dict[str, Any] | None = None
    message: str = ""
```

- For a **results** upload: `kind="arena_export"`, `status` is `ingested`/`rejected`, `datasets=[]`, and `ingestion` carries the underlying ingest outcome (`status`, `validity_status`, `run_condition_hash`, `execution_hash`, `issues`, `clean_report`).
- For a **pipeline** upload: `kind="pipeline"`, `status="registered"`, `pipeline_dag_hash` + `pipeline_label` set, `stripped_artifacts` is the count dropped from a `.n4a`, and `datasets` holds one entry per target dataset (`already_run` or `planned`).

### Upload examples

#### CLI: ingest-pipeline

Register a pipeline recipe (`.n4a`, `.json`, or `.yaml`/`.yml`) — weights stripped — and plan/inspect it on target datasets:

```bash
# Register a .n4a recipe (weights stripped) and plan it on two datasets
n4a-benchmarks ingest-pipeline my_pipeline.n4a \
    --store ./arena-store \
    --collection uploads \
    --datasets my-dataset,another-dataset

# A bare pipeline as JSON or YAML works the same way
n4a-benchmarks ingest-pipeline pipeline.yaml --store ./arena-store -d my-dataset
```

It prints the recognized `kind` + message, then a per-dataset line (`already run (N runs)` or `planned`). `n4a-arena` is the same CLI. To see a `.n4a`'s identity without registering anything, use `inspect-n4a`:

```bash
n4a-benchmarks inspect-n4a my_pipeline.n4a   # prints step count, stripped artifacts, pipeline_dag_hash
```

#### Python: upload anything

```python
from nirs4all_benchmarks.ingestion import upload
from nirs4all_benchmarks.store import ArenaStore

with ArenaStore("./arena-store") as store:
    # A .n4a bundle (with or without fitted artifacts) → register + plan.
    res = upload(store, "model.n4a", collection_id="uploads", target_datasets=["my-dataset"])
    assert res.kind == "pipeline" and res.status == "registered"
    print(res.stripped_artifacts, res.datasets)   # e.g. 3 [{'token': 'my-dataset', 'status': 'planned', ...}]

    # A bare nirs4all pipeline as a Python list → register the recipe.
    res = upload(store, [{"op": "SNV"}, {"op": "PLSRegression", "n_components": 10}],
                 target_datasets=["my-dataset"])

    # An ArenaRunExport (dict or model) → ingest the results.
    res = upload(store, manifest_dict, as_release=True)
    assert res.kind == "arena_export"
```

For any public benchmark release publication, pass `as_release=True` on result uploads. That is the switch that
creates a `benchmark_release` collection and applies release-safe leakage handling: runs without OOF attestation
are stored as `quarantined`, not silently shown as valid. As a guardrail, result uploads to release-like public
collection ids such as `benchmark_release`, `release`, or `public` are rejected unless `as_release=True` is set.

#### REST: POST /api/upload

The service exposes the same machine as a multipart form (`service/app.py`). Provide **either** a `file` (a `.n4a` / pipeline JSON·YAML / dag-ml bundle / `ArenaRunExport`) **or** a `text` field (raw JSON/YAML), plus `target_datasets` (comma-separated fingerprints or names), `collection`, and `as_release`:

```bash
# Upload a .n4a file and plan it on a dataset
curl -X POST http://127.0.0.1:8000/api/upload \
  -F "file=@my_pipeline.n4a" \
  -F "target_datasets=my-dataset,another-dataset" \
  -F "collection=uploads" \
  -F "as_release=false"

# Or paste a pipeline recipe as text
curl -X POST http://127.0.0.1:8000/api/upload \
  -F 'text=[{"op":"SNV"},{"op":"PLSRegression","n_components":10}]' \
  -F "target_datasets=my-dataset"
```

The response is `UploadResult.to_json()`:

```json
{
  "kind": "pipeline",
  "status": "registered",
  "pipeline_dag_hash": "…",
  "pipeline_label": "my_pipeline",
  "stripped_artifacts": 3,
  "datasets": [{"dataset": "…", "token": "my-dataset", "status": "planned", "n_executions": 0, "plan_id": "plan_…"}],
  "ingestion": null,
  "message": "registered pipeline …; stripped 3 artifact(s)"
}
```

A `file` is written to a temp path and routed through `upload`; missing both `file` and `text` returns HTTP 400. (The older `POST /api/ingest` route still accepts a bare `ArenaRunExport` JSON body and calls `ingest_export` directly.)

## IngestionPolicy

```python
@dataclass
class IngestionPolicy:
    collection_id: str = "default"
    collection_kind: str = "user_run_collection"   # or "benchmark_release"
    quarantine_on_leakage: bool | None = None       # default: True for releases, False for user runs
    residual_row_cap: int = 1_000_000               # per ResidualSet; fall back to scores-only above
    store_export_bundle: bool = True
    recompute_scores: bool = False                   # always recompute even when observations are present
```

| Field | Meaning |
|---|---|
| `collection_id` | Target collection; part of the idempotency key. The collection is auto-created (`ensure_collection`) at commit. |
| `collection_kind` | `user_run_collection` or `benchmark_release`. Drives the default quarantine behavior. |
| `quarantine_on_leakage` | Tri-state. `None` (the default) resolves to `True` for `benchmark_release` and `False` for `user_run_collection` via `resolved_quarantine()`. Set `True`/`False` to override. |
| `residual_row_cap` | If the residual set exceeds this many rows, residuals are stored scores-only and the run is flagged `residuals_truncated`. |
| `store_export_bundle` | Whether to write the canonical manifest into `<store>/exports/`. |
| `recompute_scores` | When `True`, scores are recomputed from residuals even if the export already carries observations (see below). |

`resolved_quarantine()` is the single place the default is applied:

```python
def resolved_quarantine(self) -> bool:
    if self.quarantine_on_leakage is not None:
        return self.quarantine_on_leakage
    return self.collection_kind == "benchmark_release"
```

## IngestionResult

```python
@dataclass
class IngestionResult:
    status: str                       # committed | already_ingested | rejected | quarantined
    ingestion_batch_id: str
    run_condition_hash: str | None = None
    execution_hash: str | None = None
    arena_export_hash: str | None = None
    validity_status: str = "valid"    # valid | quarantined
    clean_report: dict[str, Any] = field(default_factory=dict)
    issues: list[dict[str, str]] = field(default_factory=list)

    @property
    def ok(self) -> bool:
        return self.status in ("committed", "already_ingested", "quarantined")
```

- `status` is the batch outcome; `ok` is a convenience that treats everything except `rejected` as success (a quarantined run *is* stored).
- `validity_status` mirrors the execution's stored validity (`valid` or `quarantined`).
- `issues` is the list of validation findings, each `{"level", "code", "message"}` with level `error` / `warning`.
- `arena_export_hash` is the computed idempotency key, present on every non-version-rejected result.

## Idempotency

Idempotency is keyed on the tuple `(export_hash, collection, schema_version)`, enforced two ways:

1. The DEDUP-CHECK in step 2 returns `already_ingested` before doing any work when a prior `committed`/`quarantined` batch matches.
2. The `ingestion_batches` table has `UNIQUE (input_export_hash, target_collection, arena_schema_version)`, so the key is enforced at the storage layer too.

Below the batch level, idempotency is even stronger: every dimension PK is a content hash, so `INSERT OR IGNORE` makes re-writing a dimension a no-op, and one Parquet file per `residual_set` (named by its content hash) means re-ingesting the same residuals writes byte-identical rows to the same path. The practical guarantee: re-ingesting the same manifest into the same collection never duplicates or mutates anything.

## Leakage: quarantine vs. flag

Leakage honesty hinges on `leakage_attestation.oof_enforced`. When it is false, behavior depends on `policy.resolved_quarantine()`:

- **Quarantine** (releases by default, or `quarantine_on_leakage=True`): an `error`-level `leakage_unattested` issue is added and `validity_status` becomes `quarantined` (unless already rejected). The execution, scores, and residuals are **stored** with `validity_status="quarantined"` — never silently dropped — but excluded from published views. The final `IngestionResult.status` is `quarantined`.
- **Flag** (user runs by default, or `quarantine_on_leakage=False`): a `warning`-level `leakage_unattested` issue is added; the run is ingested as `valid` and simply flagged. `IngestionResult.status` is `committed`.

Separately, any `unsafe_flags` reported by the engine are always recorded as a warning (and stored in `executions.unsafe_flags_json`), regardless of quarantine policy. This is the "quarantine, don't drop" rule from `DATA_MANAGEMENT.md` §5.

## Pseudonymization

`Pseudonymizer` (`ingestion/pseudonymize.py`) maps each raw `sample_id` / `group_id` / `origin_sample_id` to a salted one-way digest:

```python
def map(self, raw: str | None) -> str | None:
    if raw is None:
        return None
    digest = hashlib.sha256(self._salt + str(raw).encode("utf-8")).hexdigest()
    return f"s_{digest[:16]}"   # default length 16
```

The salt is **store-global and persistent**: `Pseudonymizer.for_store(store)` reads the `pseudonymization_salt` key from `arena_meta`, lazily creating and persisting a random 16-byte salt on first use. Because the same salt is reused for the life of the store, the *same* raw id always maps to the *same* pseudo id across every run of a dataset. That stability is exactly what makes **cross-run residual comparison on the same samples** possible (the residual-explorer / complementarity capability in `DATA_MANAGEMENT.md` §6) — two pipelines' residuals can be joined on `sample_id` even though the original ids were never stored. The mapping is one-way (SHA-256), so the raw ids cannot be recovered from the store.

## Score recomputation and supersede

Scores can come from the export or be recomputed from residuals. The decision (in `ingest_export`):

```python
observations = [obs.model_dump(mode="json") for obs in model.scores.observations]
if (policy.recompute_scores or not observations) and prepared_residuals:
    observations = recompute_observations(prepared_residuals, model.task.task_type)
    report.scores_recomputed = True
```

So scores are recomputed when there are no producer observations, or when `policy.recompute_scores=True` — provided residuals are available. `recompute_observations` (`scoring/metrics.py`) groups residual rows by `(scope, fold_id, partition)` and computes a NumPy-only metric set (regression: rmse/mse/mae/medae/bias/r2/rpd/rpiq/ccc; classification: accuracy/macro-F1/precision/recall/MCC, plus log-loss/ROC-AUC when probabilities are present). Recomputing in-house keeps scores re-derivable and independent of any producer's metric implementation.

**Supersede.** A `score_set_id` is the fingerprint of `{execution_hash, score_computation_hash}`. Before inserting, `_write_facts` looks for a prior **valid** score set for the same `(execution_hash, score_computation_hash)` with a different id. If found, the new set records `supersedes_score_set_id` pointing at it and the prior set is updated to `validity_status="superseded"`. A metric fix that changes the `score_version` or `metric_implementation` yields a new `score_computation_hash` → a new, superseding score set, leaving the prior rows in place for audit.

## The clean report

`CleanReport.to_json()` (stored as `ingestion_batches.clean_report_json` and returned in `IngestionResult.clean_report`) records exactly what an ingestion did:

```json
{
  "source": "python:ArenaRunExport",
  "new_dimensions": {"pipeline_dags": 1, "operator_specs": 3, "...": 1},
  "deduped_dimensions": {"rng_contexts": 1},
  "facts_written": {"run_conditions": 1, "executions": 1, "score_sets": 1, "metric_observations": 9, "residual_sets": 1},
  "dropped_fields": ["residuals.y_true (publication policy)"],
  "issues": [{"level": "warning", "code": "degraded_residual_keying", "message": "..."}],
  "residual_key": "sample_id",
  "residual_rows": 120,
  "residuals_truncated": false,
  "scores_recomputed": true,
  "notes": []
}
```

| Field | Meaning |
|---|---|
| `source` | `"<producer.capsule>:ArenaRunExport"`. |
| `new_dimensions` / `deduped_dimensions` | Per-table counts of newly inserted vs. dedup-hit dimension rows. |
| `facts_written` | Per-table counts of fact rows written. |
| `dropped_fields` | Fields removed for publication policy (non-publishable residual columns). |
| `issues` | Validation findings (`level` / `code` / `message`). |
| `residual_key` | `sample_id` (cross-run comparable) or `positional` (degraded). |
| `residual_rows` | Number of residual rows stored. |
| `residuals_truncated` | True when residuals exceeded `residual_row_cap` and were stored scores-only. |
| `scores_recomputed` | True when scores were derived from residuals rather than taken from the export. |
| `notes` | Free-form notes (e.g. the truncation message). |

## Examples

### Python: ingest an export

```python
from nirs4all_benchmarks.ingestion import IngestionPolicy, ingest_export
from nirs4all_benchmarks.store import ArenaStore

policy = IngestionPolicy(
    collection_id="my-experiments",
    collection_kind="user_run_collection",   # leakage → flagged, not quarantined
    recompute_scores=True,                    # recompute from residuals even if scores present
)

with ArenaStore("./arena-store") as store:
    result = ingest_export(store, "run_export.json", policy=policy)

print(result.status)             # "committed"
print(result.run_condition_hash) # the recomputed condition identity
print(result.clean_report["facts_written"])

# Re-ingesting the same manifest into the same collection is a no-op:
with ArenaStore("./arena-store") as store:
    again = ingest_export(store, "run_export.json", policy=policy)
assert again.status == "already_ingested"
```

Ingest as a benchmark release (quarantine non-attested runs):

```python
policy = IngestionPolicy(collection_id="release-2026", collection_kind="benchmark_release")
with ArenaStore("./arena-store") as store:
    result = ingest_export(store, "run_export.json", policy=policy)
# If the export's leakage_attestation.oof_enforced is false:
#   result.status == "quarantined"  and  result.validity_status == "quarantined"
```

### CLI: ingest-export

Ingest one manifest, a directory of `*.json` manifests, or a benchmark release. The command prints per-file status and a final tally.

```bash
# One manifest into the default collection
n4a-benchmarks ingest-export run_export.json --store ./arena-store

# A directory of manifests into a named collection
n4a-benchmarks ingest-export ./exports/ --store ./arena-store --collection my-experiments

# As a benchmark release (quarantine on leakage)
n4a-benchmarks ingest-export ./exports/ --store ./arena-store -c release-2026 --release
```

`n4a-arena` is an alias for the same CLI.

### CLI: ingest-workspace

Adapter A — ingest every export the `WorkspaceAdapter` derives from a nirs4all workspace (the workspace's `store.sqlite` + `arrays/`; fitted artifacts are ignored). Always ingested as a `user_run_collection`.

```bash
n4a-benchmarks ingest-workspace /path/to/nirs4all-workspace \
    --store ./arena-store --collection nirs4all-workspace
```

Programmatic equivalent:

```python
from nirs4all_benchmarks.adapters import WorkspaceAdapter
from nirs4all_benchmarks.ingestion import IngestionPolicy, ingest_export
from nirs4all_benchmarks.store import ArenaStore

policy = IngestionPolicy(collection_id="nirs4all-workspace", collection_kind="user_run_collection")
with ArenaStore("./arena-store") as store:
    for export in WorkspaceAdapter("/path/to/nirs4all-workspace").iter_exports():
        ingest_export(store, export, policy=policy)
```

### CLI: ingest-bundle

Adapter B — convert a dag-ml `ExecutionBundle` (with optional canonical `GraphSpec` and io `CoordinatorDataPlanEnvelope`) into an `ArenaRunExport`, then ingest it. dag-ml runs are leakage-safe by construction, so the default is `--release` (quarantine on leakage); pass `--user` to ingest into a `user_run_collection` instead.

```bash
n4a-benchmarks ingest-bundle bundle.json \
    --store ./arena-store \
    --graph graph.json \
    --envelope envelope.json \
    --collection dag-ml          # --release is the default; use --user to override
```

Programmatic equivalent:

```python
from nirs4all_benchmarks.adapters import bundle_to_export
from nirs4all_benchmarks.ingestion import IngestionPolicy, ingest_export
from nirs4all_benchmarks.store import ArenaStore

export = bundle_to_export("bundle.json", graph="graph.json", dataset_envelope="envelope.json")
policy = IngestionPolicy(collection_id="dag-ml", collection_kind="benchmark_release")
with ArenaStore("./arena-store") as store:
    result = ingest_export(store, export, policy=policy)
```


## Offline review amendments

# Offline Arena ingestion

Arena accepts the existing `ArenaRunExport` v1 manifest; upload or a server is not required. Metadata checks are separate from scientific verification. Arena does not execute a run, parse scientific data, assemble a dataset or qualify a producer's scientific claims.

## Validate without a store

```bash
n4a-benchmarks validate-export run_export.json
```

```python
from nirs4all_benchmarks.ingestion import validate_metadata

result = validate_metadata("run_export.json")
print(result.to_json())
```

This opens no store and writes nothing. The result is `metadata_valid` or `rejected`, with `scientific_verification: not_performed`, terminal status, fold coverage, claim hash profile and `adopted_unverified` identities. Issues contain safe codes and never echo submitted values. The full [contract](CONTRACT.md) defines hash ownership, normalization and the terminal-status table.

Every non-null `residuals.ref` rejects as `residual_ref_unsupported` before dedup, including path/dict inputs, missing files, absolute paths, traversal and inputs also carrying inline rows. `graph_ref` is also refused. The current v1 ref does not bind referenced bytes, so silently reading or dropping a member is unsafe. Referenced-member integrity/transport is deferred; there is no alternate run format.

## Ingest into an explicitly chosen local store

```bash
n4a-benchmarks ingest-export run_export.json --store /chosen/private/arena --collection local
```

```python
from nirs4all_benchmarks.ingestion import IngestionPolicy, ingest_export
from nirs4all_benchmarks.store import ArenaStore

with ArenaStore("/chosen/private/arena") as store:
    result = ingest_export(store, "run_export.json", policy=IngestionPolicy(collection_id="local"))
    print(result.status, result.metadata_status, result.scientific_verification)
```

`committed` means this call wrote an execution's facts. `quarantined` means facts were written with release-policy quarantine. `already_ingested` returns the actual original batch/execution. `rejected` writes no facts and does not reserve a new ingestion-batch slot. A legacy rejected slot can be reused on a corrected retry.

Ingestion performs these steps:

1. Store-free metadata checking: schema, finite JSON, graph consistency, hash claims, status contradictions, directions and explicit fold coverage.
2. Prepare inline residuals outside the write lock, using a salt already committed during store provisioning. The salt uses `INSERT OR IGNORE` and read-back, never overwrite-on-conflict.
3. Apply publication policy and pseudonymize sample/group/origin ids. Hiding `y_true` or `y_pred` also hides the residual to remove the direct within-row equation; this is not a guarantee against reconstruction from other runs or scores. Residual queries reapply the same policy to old stored rows. Unknown residual columns are dropped. Oversized inline row sets become scores-only with `residuals_truncated` recorded.
4. Preserve supplied observations, including scope, partition, aggregation, direction, coverage and sample count. Record their declared origin, or `producer_reported` when unspecified. Only the existing optional residual scorer is used when observations are absent or `recompute_scores=True`; its output is marked `recomputed`. A declared custom implementation/version/aggregation that the existing scorer cannot represent refuses as `recompute_context_unsupported`, rather than relabelling Arena computations as producer computations. Neither path is scientific qualification, and this batch adds no scorer or score-versus-residual scientific check.
5. Enter `BEGIN IMMEDIATE`, check dedup again under the lock, and upsert dimensions/facts. The normalized v1 export hash is the only idempotency key. Another collection receives a typed `collection_conflict`; the store has no multi-collection membership.
6. Write an allowlisted metadata receipt atomically and fsync it before SQLite commit. A receipt write failure rolls back database facts rather than reporting a successful commit.

Cold-open WAL/schema negotiation has a bounded lock retry and explicit SQLite busy timeout. Transactions roll back on an exception. Database and filesystem writes are not one cross-filesystem transaction: a failed database commit or process crash can leave an unreferenced receipt/residual file. SIGKILL recovery and orphan cleanup are not claimed as tested or implemented.

## Terminal outcomes and score visibility

`execution.terminal_status` is nullable: `completed`, `incomplete`, `failed`, `cancelled`. `incomplete` requires legacy `status: failed`; `completed` requires `status: ok`. Missing terminal status plus `ok`, even an explicit historical `ok`, resolves to `unreported`. Missing terminal status with `failed` or `cancelled` keeps that outcome. Contradictions reject.

Resolved status is stored in the existing `executions.status` column. Existing `ok` rows remain untouched and read as `unreported`. Default leaderboard, matrix, effects, pivot, parallel, statistics, robustness and score-based graph queries use the new `v_ranked_metrics` view: completed, policy-valid execution and score, finite value. The retained `include_quarantined` aggregate argument cannot bypass this selection. Explicit run detail, fold scores and the run explorer remain inspection paths for non-completed runs, including runs with no observations.

```python
from nirs4all_benchmarks.store import Queries

strict = Queries(store)
legacy_inspection = Queries(store, include_unreported=True)
```

The Python opt-in includes unreported outcomes and is labelled in aggregate dictionary outputs and `overview`. It never promotes failures. It is not exposed by the service or the static pack. Planning and `already_run` require a completed policy-valid execution. Readers also correct the interpretation of a legacy `fulfilled` plan if no such execution exists, without rewriting the old row.

Expected folds are an explicit total from `scores.expected_fold_count` or `cv.n_folds * (cv.n_repeats or 1)`, never from observed folds; both declarations must agree. Explicit completed counts must agree with fold rows. A claimed completed run with fewer observed folds than expected rejects. No fold rows means `fold_coverage: not_reported`; aggregate-only data does not itself prove an incomplete run.

## Metadata receipts and old files

`exports/arena_run_<normalized_hash>.json` now contains an allowlisted metadata receipt marked `sanitized: true` and linked by `source_export_hash`. Inline residuals, arbitrary graphs/params/cards, unknown fields and raw provenance documents are omitted. Failure messages are redacted. Score metadata and complete provenance content digests are kept; known historical source/exporter hashes are preserved, with local paths and experiment keys represented only by digests.

Receipts are intentionally refused at ingestion (`sanitized_manifest_not_ingestible`); importing them as new runs would change identity and duplicate facts. Re-ingest the original owner export for idempotent local ingestion. These receipts do not permit exact scientific replay or reconstruction of removed provenance documents. `store_export_bundle=False` explicitly disables receipt preservation.

Old raw exports are not rewritten. The shared reader refuses them as `legacy_unsanitized_export_refused`; run detail and the service return that explicit limitation. Raw failure messages in legacy SQLite rows are redacted on read. Symlinked store/export/residual paths and unsafe residual-set ids are refused. Existing raw files remain a local-at-rest privacy gap requiring a separate authorized cleanup; direct filesystem access is not protected by Python query sanitization.

Static metrics packs require rankable completed runs with current explicit producer-reported public evidence in both the ingestion report and sanitized receipt; every table, count and run file is restricted to that set. Static run files are produced only for rankable executions, so inspecting failed/incomplete/unreported runs requires Python or the service. Site generation fails before writing assets when no rankable rows exist. The historical corpus has no reported terminal completion, so its strict site generation is refused. No existing user stores or site outputs are used by these checks.

## Policies and remaining boundaries

`IngestionPolicy` retains `collection_id`, `collection_kind`, `quarantine_on_leakage`, `residual_row_cap`, `store_export_bundle` and `recompute_scores`. Release collections default to quarantining unattested OOF safety; user collections retain it as a warning. Legacy boolean defaults remain in the hash domain; `reported_fields: []` means no safety attestation was supplied. Visibility metadata is a declaration, not proof of private or public dataset origin.

Service authentication, project token isolation, remote transport, release authorization and upload quotas are deferred pending Cluster's shared verifier and its review. This batch does not make the existing service safe for network deployment. Repository/Datasets services and exact Core canonical n4a are separate owner work. Windows SQLite, cross-platform crash durability and actual DAG-ML completion semantics remain unverified.

## Focused verification

From the benchmarks repository, run:

```bash
PYTHONDONTWRITEBYTECODE=1 .venv/bin/python scripts/check_arena_offline.py
```

The runner prints the exact expanded pytest command. It selects the offline regressions plus existing contract, identity, store, ingestion, adapter, query, upload, fixture identity, scoring, Repository bridge, in-process service and historical-pack checks. It deselects the browser-rendering test. All executed tests are Python; no server sockets, frontend build, installation or network delivery is used. The historical site test checks refusal before assets are written; the successful site-build/browser/Node suites are not run.

Every run uses a new `benchmarks-offline-check-*` temporary root, isolated `TMPDIR`/cache/store paths, disabled pytest cache/plugin auto-loading and single-thread BLAS/OpenMP variables. It polls free disk every 0.25 seconds and terminates pytest within a 64 MiB reserve above the required 2,147,483,648-byte floor. Temporary stores are removed on exit. Synthetic fixtures and counterexamples exercise metadata contracts only; they do not attest that a scientific run succeeded.

### WORK review corrections (B1–B5)

These rules supersede legacy examples and earlier offline descriptions above. The original reference sections remain intact; they describe v1 structure, not evidence that execution, public sharing or safety was reported.

- **Static publication:** only completed, policy-valid runs with finite scores and `visibility: public`, `visibility_source: producer_reported` qualify. Both the immutable ingestion report and sanitized receipt must carry the current `arena.metadata/2` evidence profile and agree on public visibility. A default or unmarked legacy public value is unknown. An old store cannot be published based on that default; a separate explicit owner/operator decision is required, and no legacy override or store rewriting is implemented here. Disabling or losing receipts also prevents publication.
- **All public outputs:** metrics, catalogues, facets, nodes, parameters, collections, counts and run files use the same selected run set and one SQLite read snapshot. Descriptive metadata and secondary identity declarations come from the selected public receipt, so a shared key first inserted by a private run does not publish its labels or provenance. Global collection free text, raw execution ids/hardware/unsafe flags, arbitrary node parameters and `parquet_path` are omitted from public run files. Private/non-rankable plans are not exported. A build with no qualifying runs refuses before writing assets. A nonempty output directory is refused to prevent stale private files surviving a rebuild. Source frontend/static assets are unchanged; tests generate only isolated Python fixture snapshots.
- **Attestations:** v1 booleans remain hash placeholders. `reported_fields` explicitly identifies declared values. An excluded non-default value contradicts the marker and rejects; a listed value must be supplied. Without a marker, only non-default legacy values can establish a reported declaration: `oof_enforced: true`, `group_leakage_checked: true`, or `nested_cv_safe: false`. In particular `nested_cv_safe: true` is reported only with a marker. Normalized sparse defaults never become facts. Local run detail and the explorer return null for unreported fields, even though the old SQLite integer column retains its compatibility sentinel. Old receipts cannot promote default-valued booleans using their former presence-based marker. No safety or scientific verification is inferred.
- **Folds:** count observations with a real non-null `fold_id` at `scope: fold` or `scope: cv`, on train/validation partitions, and fold/CV residual rows at those partitions. Ignore `final`, `final_agg`, `avg`, `w_avg`, `ensemble`, `all`, `macro`, `fold_avg`, `fold_w_avg`, every id ending in `_agg`, refit/test rows and aggregate observations without a fold id. This includes the workspace adapter's per-fold CV-scope score shape. Expected totals must agree with explicit CV folds and repeats. For a completed declaration with a known expected total, every reported `(metric_name, partition)` fold series must cover that total independently; a full RMSE series cannot fill missing R2 folds. Residual rows cannot fill a metric-specific gap. Repeated CV requires distinct owner-provided repeat/fold ids; no repeat identity is synthesized. Without explicit expectations, observed ids do not supply an expected total. Aggregate-only reports remain `fold_coverage: not_reported`, not inferred incomplete.
- **Residual policy:** withholding a residual also withholds predictions and probabilities, so the published operands do not reconstruct it. The same rule is reapplied to legacy Parquet rows. Withholding either operand also withholds the residual. This does not claim protection against inference from single-sample scores or other external evidence.
- **Read locking:** warm opens SELECT the salt first and insert only if absent. Cold provisioning still uses INSERT OR IGNORE and read-back, before preparing rows. Warm readers do not acquire that salt write lock when a writer holds BEGIN IMMEDIATE.
- **Visibility and direction:** adapters emit explicit `unreported` visibility unless an actual declaration is supplied; an IO envelope's unmarked public default cannot be promoted to producer evidence. Known metric directions use the existing registry. The legacy min value remains in the v1 hash domain and is not a reported custom direction. `direction_source: producer_reported` makes a supplied direction explicit; a known-metric mismatch rejects. A non-default legacy max is also a real declaration. Unknown metrics without reported direction remain null in stored/read score facts and cannot be ranked using a guessed minimum; custom directions must agree across the selected data. No new scorer was introduced.
- **Provenance:** absent provenance stays absent, not a fabricated digest of null. Actual supplied provenance is retained as content/source digests. Historical origin and CSV line text still depend on the original archive; arbitrary documents and local paths are not copied into receipts.

Remaining limitations: unknown genuine pipeline identity still refuses rather than inventing one; actual nirs4all owner status semantics are not established by synthetic tests. Local inspection still exposes owner execution ids, hardware, unsafe flags, parameters and fold ids; service output allowlisting/authentication are separate work, and no service/auth files are modified here. SIGKILL recovery, Windows SQLite, full repository tests, single-sample privacy inference and scientific execution remain unverified. Exact Core canonical n4a remains separate owner work. Metadata validation always reports `scientific_verification: not_performed`.

### Historical WORK corrective verification commands

```bash
PYTHONDONTWRITEBYTECODE=1 .venv/bin/python scripts/check_arena_offline.py
PYTHONDONTWRITEBYTECODE=1 .venv/bin/python scripts/check_arena_offline.py --production-probes
```

The first command runs focused Python tests including all seven required WORK regressions and temporary site fixtures. The second executes the externally supplied WORK reviewer probe unchanged first at that historical checkpoint. The corrected B2 rejection causes its old diagnostic to index a nonexistent run; the wrapper records that TypeError honestly and retries with exactly one null-safe diagnostic printer. All original inputs and production calls remain unchanged. Original source SHA-256: `a64fcdb598f86229dcbd5cb7ff8bc9bb28d64f4b9db098c9125a66907dc38246`. The original script is never edited. Its temporary `/var/tmp/arena-w-*` roots and the guard's unique temp root are cleaned up; no existing user store is opened.

Both commands force BLAS/OpenMP to one thread, isolate caches/stores/temp paths, and monitor free disk with a 64 MiB reserve above 2,147,483,648 bytes. The guard terminates the spawned process group on a disk stop. No native builds, installs, network, deployment or subagents are used. An independent Opus re-review is still required; running these probes is not a new independent acceptance verdict.

### Historical WORK corrective check results

The final focused command completed **214 passed, 1 deselected, 1 warning in 56.41 seconds**. The deselected case is browser rendering; the warning is the installed FastAPI/Starlette TestClient's httpx deprecation. Final focused-check minimum observed free disk was **3,543,797,760 bytes**; the final production-probe run observed **3,588,730,880 bytes**. The lowest reported guard value across corrective runs was **2,485,407,744 bytes**, above the 2,147,483,648-byte floor. No installation or dependency change was made.

All seven required WORK cases are in `tests/test_arena_work_review.py`, alongside shared-catalog, stale-output, repeat-count, excess-fold and metric-direction cases. Unknown directions do not contribute to default ranked counts. Custom reported directions work in Python; static publication refuses custom/unregistered metric names rather than relying on an unchanged frontend's guessed direction.

The original production cases recorded: all 10,824 historical exports metadata-valid and unreported; a temporary 120-row ingest had zero ranked and 120 inspectable executions; 5 concurrent-repeat commits plus 35 idempotent returns with zero phantom batches; 60 cold-root commits with no lock error or divergent pseudonyms; a held-writer reader open/query recorded 0.0 seconds and was not blocked after three seconds. Every R8 bypass and the per-metric gap rejected. R2's unmarked legacy public example now refuses publication; the new explicit-public mixed-store test separately verifies successful safe output. Contradictory R3 input rejected and absent/defaulted attestations returned null. R7 withheld residual output contained neither prediction nor residual. Ref and forged-hash refusals remain intact.

The original harness itself did not pass unchanged: B2's correct rejection exposed its diagnostic TypeError. The documented one-line printer guard permitted all remaining original production calls to complete, with exit code zero, without editing the probe file or changing any input. This is recorded execution evidence, not a fresh independent review or scientific execution validation.

Original documentation restoration was checked line by line against HEAD: all **590** INGESTION.md lines and all **258** ADAPTERS.md lines remain in order; the corrections are additive. `git diff --check` passed. Source service/auth and frontend/static files were not modified. All stores and generated site fixtures were isolated temporary data and were removed; no existing user store or site was opened or rewritten. Original dirty AGENTS.md and codegraph.json remain untouched.

Implementation is frozen after these checks. Independent Opus re-review is **pending**, not accepted or executed: no callable review tool is available in this session, and invoking a remote CLI/agent would exceed the no-network/no-subagent constraints. No actual owner scientific execution, SIGKILL recovery, Windows SQLite, full repository suite, native/frontend build, service/auth or Core canonical n4a qualification is claimed.

### Narrow re-review corrections C1–C4

Real CV-scope score fold ids now count alongside fold-scope ids, matching the workspace adapter. Aggregate CV observations without ids, sentinels, refit/test scopes and test/final partitions do not fill coverage gaps. Per-metric/per-partition expectations still apply.

The backend static pack retains only numerically or boolean-typed `param:*` values, checked against stored parameter types. Free-form strings (including strings that look like numbers or booleans), objects and sequences are omitted from parameter facets and the public parameter catalogue. Local pipeline metadata is unchanged. No site rebuild, frontend or asset change is part of this recheck.

A dangling citation foreign key returns `rejected` with `metadata_not_indexable` after rollback. It leaves no partial batch or fact rows and does not reserve a retry slot. Supplying the genuine missing citation later permits the same export hash to be ingested idempotently; ingestion does not fabricate a citation.

**Privacy limit:** publication flags are applied per run and per row, and scores are not suppressed. Stable pseudonyms intentionally support cross-run joining within a store; they are not an anonymity guarantee. A published `y_true` of 10 in run A can join to run C's published `y_pred` of 7 and reveal C's residual of 3, even when C's target/residual cells are withheld. A one-sample fold's published RMSE of 3 and prediction of 7 narrow its hidden target to 4 or 10. Withholding cells does not prevent these cross-run or score-based inferences. No global information-flow or statistical privacy protection is implemented or claimed.

### Historical C1–C4 targeted verification and stop point

Exact commands executed:

```bash
PYTHONDONTWRITEBYTECODE=1 .venv/bin/python scripts/check_arena_offline.py --targeted-recheck
PYTHONDONTWRITEBYTECODE=1 .venv/bin/python scripts/check_arena_offline.py --targeted-reviewer-probe
```

The targeted tests completed **83 passed in 22.53 seconds**, including real workspace score-only CV-fold output, complete and partial fold/CV series, per-metric gaps, numeric/boolean versus string-lookalike parameter facets, dangling-citation rollback and same-hash retry, and the documented cross-run/n=1 information leaks. The selected tests do not call the site builder or render frontend assets. Minimum observed free disk for that run was **3,315,003,392 bytes**.

The targeted probe executed the unchanged relevant input/call sections of the externally supplied C1–C4 reviewer probe (SHA-256 `ab9b80450fbdbf0fdb14cdc72d9c92adb05a2fd9fd1d753164de261cfa4fab41`). Its public fixture was checked with `_build_bundle` in memory, not `build_site`; the subsequent site-rendering statements and the already-closed B2/B5/history sections were not run. It confirmed no `PARAM-PATH` in the pack, typed `metadata_not_indexable` for the citation FK, rejection and reported coverage for both CV-scope bypass cases, acceptance of the complete/incomplete controls, cross-run recovery of residual 3, and the unsuppressed n=1 score/prediction pair 3 and 7. All targeted assertions completed with exit code zero. Minimum observed free disk was **3,593,670,656 bytes**. The original probe file was not edited and no site output was created.

Both runs forced BLAS/OpenMP to one thread, used isolated temporary stores/caches, monitored the 2,147,483,648-byte disk floor with a reserve, and removed their temporary roots. All original INGESTION.md/ADAPTERS.md reference lines remain preserved. No new dependency/package, native build, installation, network, other repository, frontend/static asset, service/auth change or subagent was used. `git diff --check` passed. Work stops here for the required independent Opus targeted recheck; no new acceptance verdict or scientific execution validation is claimed.


### D1–D3 targeted corrections and verification

The authoritative coverage rule is now shared locally in `contract/folds.py`,
without importing nirs4all. It mirrors the owner definitions identified in the
Opus review: `final`, `final_agg`, `avg`, `w_avg`, `ensemble`, `all`, `macro`,
`fold_avg`, `fold_w_avg`, and every id ending in `_agg` (including `_agg`).
After string conversion and trimming, matching is case-sensitive; empty ids
are pseudo-folds. Arbitrary genuine ids such as `fold0`, `split/A` and `FINAL`
remain eligible. The workspace adapter clears pseudo-fold ids from
score/residual fold identity, retains their reported scores, and excludes
aggregate prediction twins from CV residual rows. The separately scoped `final`
refit predictions remain available. Native offline validation applies the same
rule to both observations and residual evidence. Only eligible train/validation
fold/CV observations with genuine ids create a metric/partition series; filtered
test or sentinel rows cannot create empty series. Genuine partial metric series
still reject a completed declaration with a known expectation.

Ruff repairs are limited to the reported import, unused import, closure binding,
line-length and simplification findings. Mypy now narrows native manifests using
`isinstance`, checks guaranteed COUNT result rows explicitly, and returns a
metadata-unavailable marker if an execution lookup has no row. No lint/type
configuration, broad suppression, scoring algorithm or v1 hash default changed.

The historical command records above describe earlier checkpoints. External
reviewer scripts are not distributed dependencies. Their two old wrappers now
print an explicit **SKIP** when no existing script is supplied; that is not a
passing production probe. Documentation no longer depends on volatile script
locations. Current T1/T2 fixtures, assertions and the extra owner-sentinel cases
are self-contained in `tests/test_arena_fold_coverage.py`.

Current exact repeatable commands (repository root):

```bash
.venv/bin/python -B scripts/check_arena_offline.py --targeted-recheck
.venv/bin/python -B scripts/check_arena_offline.py --fold-reviewer-probe
.venv/bin/python -B scripts/check_arena_offline.py --ruff
.venv/bin/python -B scripts/check_arena_offline.py --mypy
```

Results: **134 passed in 45.92 seconds** for the focused existing/regression
suite; **51 passed in 5.50 seconds** for the self-contained fold probes;
`ruff check src tests`: **all checks passed**; `mypy`: **no issues in 58 source
files**. The initial gate runs reproduced 58 Ruff findings and six mypy errors.
The first new probe run had two incorrect test expectations (it added a sixth
genuine id instead of a partial second metric over the existing ids); these were
corrected before both final passing test runs.

The original reviewer T1/T2 input/call sections also executed unchanged from an
explicitly supplied external source, SHA-256
`63058f22122d61d32d1499e846d79da74face639fef0e04eea6fc0b4421a0fc1`.
Only the prefix before C2 was executed; its diagnostics completed with exit zero.
In particular, 3/5 plus `avg`/`w_avg` rejected and had zero ranked rows; 5/5 plus
those aggregates or test rows committed and ranked. Both native test-partition
cases and the other-metric `_agg` case were metadata-valid. Incomplete/unreported
runs remained ingestible and unranked. Aggregate-only coverage stayed
`not_reported`; genuine metric gaps and excess nested ids remained refusals.
The portable assertions independently check these outcomes. The optional source
runner takes `--targeted-fold-source PATH`; no external script is needed for the
repeatable commands above. Both historical wrapper no-source invocations were
checked and printed SKIP without opening a store or building a site.

All commands forced BLAS/OpenMP to 1, disabled bytecode/pytest caches, isolated
stores/temp/mypy/Ruff caches, and monitored free disk with a 64 MiB reserve above
2,147,483,648 bytes. The lowest observed guard value during this D1–D3 batch was
**3,241,291,776 bytes**. Temporary roots were removed. No existing user store,
site, frontend/static asset, other repository, service/auth implementation, notes,
package dependencies or original dirty AGENTS.md/codegraph.json was changed.
No site rebuild, scientific execution, installation, native build, network,
commit or subagent was used.

Remaining limits from the review still apply: actual current producer workspace
contents/status semantics, browser rendering, full-suite execution, SIGKILL and
Windows behavior are unverified. Free-form method/operator/parameter names and
raw published fold ids remain; default publication flags, cross-run joining and
single-sample score inference are not statistical privacy guarantees. Nested CV
ids exceeding the declared expected total are refused. Owner identities and
attestations are not scientifically verified. Service authentication and exact
Core canonical n4a stay with their separate owners. Work stops for the required
independent Opus targeted check; **no acceptance verdict is claimed**.


### E1–E2 fold normalization and score scope correction

The local pseudo-fold mirror now applies `str(fold_id).strip()` to string/integer
ids and treats empty or whitespace-only strings as pseudo-folds. An absent id
remains absent fold evidence. Matching stays case-sensitive and genuine string
ids remain supported, including `fold0`, `repeat0_fold1`, `w_avg_1` and `FINAL`.
The workspace adapter emits trimmed string ids for genuine folds, so integer
SQLite values work even when `fold_id` has no type affinity.

Owner source references are pinned in `contract/folds.py`:
`nirs4all/nirs4all/data/reduction.py:is_real_cv_fold` and
`nirs4all/nirs4all/pipeline/storage/target_domain_summary.py:115` (workspace-relative
paths, identified in the actual Opus review). Base Arena imports neither source.
The intentional difference from the owner's integer-only real-fold recognition
is continued support for genuine arbitrary string fold ids in this contract.
This local mirror can still drift; these tests do not verify a live owner runtime.

Workspace scores for `final`/`final_agg`, including whitespace-padded spellings,
now use `scope: refit` and retain their source partition (R1 correction below).
All other pseudo ids use `scope: view` and retain their source partition. This takes precedence over test-partition
scope mapping for pseudo rows. Their values remain stored and queryable; they
have no real fold id and do not enter the CV mean. Genuine test rows still use
`scope: test`. Existing aggregate prediction-twin exclusion from residuals is
preserved; genuine `final` refit predictions remain separately scoped.

The new regression cases reproduced the unfixed failures (**41 failed, 48
passed**), then passed after correction. They assert 4/5 plus each empty/padded
pseudo id rejects, genuine 5/5 plus those ids commits, and the leaderboard has
exactly five CV observations with mean **1.0** when `avg`/`w_avg` report **0.1**.
Refit aggregates and pseudo rows on test partitions also leave that CV mean
unchanged, with their original scores queryable at the corrected scopes.

Exact repeatable commands, executed sequentially from the repository root:

```bash
.venv/bin/python -B scripts/check_arena_offline.py --fold-reviewer-probe
.venv/bin/python -B scripts/check_arena_offline.py --ruff
.venv/bin/python -B scripts/check_arena_offline.py --mypy
.venv/bin/python -B scripts/check_arena_offline.py --targeted-recheck
```

| Check | Observed result | Minimum free bytes | Maximum sampled scratch bytes |
|---|---|---:|---:|
| Self-contained fold regressions | 89 passed in 5.62 s | 7,746,490,368 | 15,224,832 |
| `ruff check src tests` | All checks passed | 7,707,709,440 | 24,576 |
| `mypy` | No issues in 58 source files | 7,655,235,584 | 48,881,664 |
| Focused existing/regression suite | 172 passed in 39.36 s | 7,578,107,904 | 75,100,160 |

The actual reviewer's full workspace-fold diagnostic source also executed with
exit zero (SHA-256
`1eccf55024b3f233b06806472fc10bf7b4f6bd379382274e58f35ef9b02d29cb`).
Its case inputs and production calls were unchanged; the optional
`--workspace-fold-source PATH` runner redirected its explicit external temp parent
into the guard's owned scratch. The diagnostics confirmed all empty/padded-id,
aggregate-mean and integer-affinity outcomes. They also preserved genuine string
and case-sensitive controls, native per-metric gaps and incomplete coverage
refusals. Minimum sampled free disk was **7,711,571,968 bytes**, maximum scratch
**18,481,152 bytes**, and the source reported its temporary root removed. This
source remains optional; the bundled tests supply permanent assertions.

Following the reviewer's reported ENOSPC incident, this batch ran one check at a
time. The guard now measures only its isolated scratch tree and stops with a
64 MiB reserve before **1,000,000,000 bytes**, as well as before the existing
**2,147,483,648-byte free-disk floor**. Figures above are sampled observations.
BLAS/OpenMP remained 1, bytecode/pytest caches were disabled, and mypy/Ruff caches
and every test/probe store were isolated and removed. No ENOSPC occurred in these
checks. No existing user store, site rebuild, frontend/assets, other repository,
native compiler/build, installation, network, dependency, service/auth work,
commit, notes edit or subagent was involved. Original documentation remains
preserved. The earlier scientific, privacy, browser and full-suite limitations
still apply. Stop here for a fresh independent Opus review; no acceptance or
scientific validation is claimed.


### R1 owner partitions and explicit aggregation context

Refit scope and data partition are independent facts. Workspace `final` and
`final_agg` scores keep refit scope while retaining source `train`, `val` or
`test` as canonical `train`, `validation` or `test`. They are never relabelled
`partition: final`. This also applies to retained `final` residuals. Scalar values
and partition provenance survive ingestion and `run_detail`; refit train/test
scores can be retrieved and aggregated separately.

Every Python score aggregation now selects one partition. CV/fold/view default
to validation, test scope defaults to test, and refit **requires an explicit
partition**, even on an empty store. `leaderboard`, `matrix`, `run_explorer`,
operator/parameter effects, robustness, pivot, parallel, stats, pipeline/operator
graphs and composition share this selection rule. An explicit CV train lens is
available without mixing training values into default validation rankings. Query
results identify the selected partition; invalid partitions raise
`invalid_score_partition`, and missing refit selection raises
`score_partition_required_for_refit`. The CLI accepts `--partition`, reports the
selected context, and returns a usage error for unsupported selection. Row-level
residual inspection supports both partition and scope filters; an unfiltered row
listing is not an aggregate.

The static backend pack refuses public score contexts requiring a partition
selection its unchanged consumer cannot supply (`static_score_partition_selection_unsupported`).
This includes refit and non-default partition rows. It neither averages those
contexts nor silently drops their facts to manufacture a publishable pack. Local
inspection retains them. Only the pure in-memory packing refusal was tested; no
site was built and no frontend/static file changed. Service routing/authentication
and browser adaptation remain separate, unqualified work.

#### Read-only owner evidence

Exact owner code was read without importing or executing it, at nirs4all
`3a2013b9`; the following files were clean when inspected:

- `nirs4all/pipeline/storage/store_schema.py`: pipelines/predictions DDL,
  non-null source partitions and fold ids, scalar train/val/test score columns,
  partition-sized prediction `n_samples`, and the prediction natural key.
- `nirs4all/pipeline/storage/workspace_store.py:save_prediction`: nested
  `scores[partition][metric]`, array separation, and train/val/test row semantics.
- `nirs4all/controllers/models/base_model.py:_assemble_prediction_result` and
  `_add_all_predictions`: compute partition-specific metrics and persist each
  partition separately with the full scores map.
- `nirs4all/data/predictions.py:_dedupe_final_candidates`: finals explicitly
  retain separate train/test records.
- `nirs4all/data/reduction.py:prediction_scope_from_legacy`: refit scope precedes
  the test-partition rule. The local aggregate-view distinction remains the
  Arena adapter rule established by E2.
- `nirs4all/pipeline/storage/store_queries.py:COMPLETE_PIPELINE` and
  `workspace_store.py:complete_pipeline`: an explicit persisted `completed`
  status means producer-reported completion. `array_store.py` documents the
  per-dataset Parquet and positional sample indices.

The adapter now reads only the row's own partition from the nested score map.
Legacy flat metric maps remain supported; scalar fallback chooses `train_score`,
`val_score` or `test_score` for that source partition. It never substitutes a
validation score for a refit test/train value. Malformed scores, ambiguous/missing
nested partition maps and unsupported values refuse with explicit workspace
error codes. Partition-sized prediction counts are attached to observations,
not claimed as dataset cardinality. Explicit owner status `completed` maps to
Arena completed/ok; absent status remains unreported, and running or unknown
states refuse rather than manufacture a terminal result.

The actual owner pipelines table has no `expected_fold_count`. Its absence leaves
`cv.n_folds`, `scores.expected_fold_count` and `scores.completed_fold_count` null.
Neither five observed ids nor a completed status supplies an expected total.
`fold_coverage: reported` means fold evidence was supplied, not that all expected
folds were independently verified. In this owner shape, four observed folds can
be metadata-valid because there is no declared five-fold expectation to enforce.
Scientific verification remains `not_performed`.

Residuals stay positional. The local composite context is `(sample_id, scope,
fold_id, partition)`, so overlapping train/test positions remain distinct without
inventing global sample identities. Missing/mismatched position arrays, conflicting
array-vs-SQL context and duplicate composite keys refuse explicitly. Aggregate
prediction twins remain omitted as previously documented. Branch id zero is
preserved separately from an absent branch; multiple chain ids inside a model /
branch export refuse as unsupported instead of being merged under one identity.
Cross-run positional comparisons remain unsupported.

The permanent fixture in `tests/test_workspace_partitions.py` copies the verified
owner pipelines/predictions DDL, including constraints and all columns. Referenced
runs/chains tables are minimal stubs. It uses owner `val`, nested partition maps,
per-partition counts, owner `completed` status and no expected-fold extension.
Synthetic arrays deliberately reuse positions across final train/test rows.
This verifies compatibility with those source shapes; no existing user store or
actual scientific execution was used. Live dataset identity validity, owner
runtime semantics beyond the inspected declarations, heterogeneous/reduced-unit
contexts and arbitrary multi-chain workspaces are not qualified. Genuine dataset
identities remain mandatory; no dataset or provenance is manufactured here.

#### R1 exact verification commands

Executed sequentially, repository root:

```bash
.venv/bin/python -B scripts/check_arena_offline.py --owner-partition-probe
.venv/bin/python -B scripts/check_arena_offline.py --ruff
.venv/bin/python -B scripts/check_arena_offline.py --mypy
.venv/bin/python -B scripts/check_arena_offline.py --targeted-recheck
```


Final R1 results (exit codes captured, one suite at a time):

| Check | Observed result | Minimum sampled free bytes | Maximum sampled scratch bytes |
|---|---|---:|---:|
| Owner partition probes | 31 passed in 10.94 s | 7,210,500,096 | 9,322,496 |
| Full `ruff check src tests` | All checks passed | 7,231,819,776 | 24,576 |
| Full `mypy` | No issues in 58 source files | 7,182,602,240 | 49,168,384 |
| Focused existing/regression suite | 203 passed in 54.44 s | 7,149,215,744 | 84,041,728 |

The owner probe asserts all twelve aggregation paths select validation **1.0**
from five `val` scores while five train scores remain **0.1** in their own lens.
Refit train **0.05** and test **0.5** remain separately inspectable/aggregated;
no query produces the unselected **0.275** mean. It checks nested JSON and scalar
fallback, explicit owner status, absence of expected-fold declarations, zero-vs-null
branches, overlapping residual positions, malformed/unsupported inputs, CLI usage
errors and the pure backend static refusal. The existing fold/sentinel/metric-gap,
contract, identity, store, ingestion, query and upload regressions remain included.

All runs used BLAS/OpenMP 1, isolated scratch/stores/caches and the existing guard
with 64 MiB reserves before the 1,000,000,000-byte scratch ceiling and the
2,147,483,648-byte free-disk floor. Across this batch, the lowest guard observation
was **7,142,248,448 free bytes**, and the largest sampled scratch was
**84,156,416 bytes** (the earlier 202-test check before the branch-zero regression).
Temporary roots were removed. No existing user stores or scientific datasets were
opened, no site/frontend/static assets or other repository was edited, and no
installation, native compiler/build, network, subagent, commit, dependency or
service/auth change was used. All 590/258 original reference documentation lines
remain preserved; `git diff --check` is clean. These are metadata/consumer checks,
not an actual owner scientific execution, full repository qualification, browser
validation or new acceptance verdict. Freeze and stop for independent Opus review.

## Sole v2 offline contract (4 November 2026)

This section supersedes every v1 compatibility, migration, default-attestation,
`reported_fields`, raw-hash fallback, and `include_unreported` description above.
The original reference and previous review checkpoints remain historical records.
Only wire version **2** and newly identified store schema **2** are supported.
The accepted plan is `arena-v2-plan.md`, SHA-256
`8f763b3a4fd79771809debddc65afff0584cacbf07afe3ab432ba1a989624b10`;
`opus-arena-v2-closure.json` accepts its text, not the implementation. A separate
Opus WORK review is still required.

Raw dictionaries require an explicit integer version 2, finite content, the raw
JSON Schema, then strict Pydantic validation. Unknown consumed fields are refused.
The one hash profile is `v2_normalized`: SHA-256 of canonical JSON of
`model_dump(mode="json", exclude={"arena_export_hash"})`, including null fields.
There is no schema 1 runtime, migration, second version alias, or compatibility
hash profile. Schema 1 inputs return `unsupported_export_version` before fresh
store provisioning in the result CLI/API upload entry points.

Every observation supplies its value, direction/source, scope, partition, level,
`fold_id`, and `source_fold_id`. Null or `unreported` context is stored without
ranking. Both fold fields set is contradictory; both null is unranked. Context
keys are unique per case-insensitive metric. Genuine folds and aggregate source
labels have separate fields. CV defaults select validation/sample/genuine folds;
test defaults select test/sample. Refit needs an explicit partition and defaults
to the exact source label `final`; `final_agg` needs explicit group level and
label. View and non-sample selection require an explicit source label. No generic
mean combines train/test, unreduced/group observations, or aggregate labels.

Ranking additionally requires producer-reported completion, known identity
context, three true safety declarations, an explicitly empty unsafe-flags list,
and finite scores with known directions. Null attestations remain null. Explicit
false or unsafe flags cannot rank or appear in the unknown-only shared view.
`Queries.unknown_context_rows()` returns `ranked: false`, has no average and no
score ordering, and excludes restricted/private/anonymized observations. There
is no leaderboard opt-in for unknowns. Failed, incomplete, cancelled and
unreported runs remain locally inspectable and idempotently ingestible when their
actual identities and context are representable. These are metadata checks;
`scientific_verification` remains `not_performed`.

`identity_basis` is required for dataset, task and variant identity. Each entry is
`owner_reported`, `arena_derived` with input facts in an Arena namespace, or
`source_declared_unverified`. The last never ranks. Owner declarations are not
scientific validation. Workspace-derived task identities bind dataset name,
feature identity, task type and a digest of typed, ordered
`(partition, sample_index, y_true)` context. Missing arrays yield an opaque source
record identity and no ranking, rather than an assumed common task. These are
Arena record identities, not IO/DMD scientific identities. A target digest does
not prove dataset correctness, leakage safety or reproducibility.

### Store ownership, transactions and privacy

Only an absent or empty root can be provisioned. Foreign nonempty roots, empty
SQLite files, symlinks, old versions, and WIP-migrated unstamped v2 files refuse.
Before any SQLite connection to an existing file, a plain 100-byte read must
identify `application_id=0x4e344132` and `user_version=3`. A private in-memory
snapshot, including a checksum-checked committed WAL image when present, verifies
`arena_meta.schema_fingerprint == sha256(schema.sql)` and the salt before any
store write. Fresh provisioning commits stamps, DDL, fingerprint and salt in a
sibling temporary file, closes it, then uses no-clobber publication. The loser of
a fresh-init race verifies the winner independently. Refusals are path-free
`ArenaStoreRefusal(ValueError)` values with a `.code`; CLI exit status is 2.
Health checks inspect only the raw header and explicitly report that they did
not verify the fingerprint. A v1 volume is not healthy.

Pack intake validates every input before entering one `BEGIN IMMEDIATE` for all
dimensions, facts, collection metadata and allowlisted receipts. A write-phase
refusal rolls the whole pack back. Receipts live in `metadata_receipts` in SQLite,
so their visibility follows the same commit; ingestion no longer publishes
receipt JSON files before commit. Replays deduplicate under the same write lock.
Raw exports are not stored. Existing raw files under `exports/` are ignored by
queries or explicitly refused by the metadata reader. Marked receipts are also
validated and projected again; nested extras cannot bypass that projection.
Identity input and free-form provenance documents become content digests in
receipts. The native source export/conversion artifact retains its original
provenance; a receipt is not replay evidence or a scientific reproducibility
bundle. Unsupported residual references are always refused, including dedup
hits, traversal and absolute paths. Residual rows need explicit scope, partition
and publication policy; recomputation cannot supply missing scientific context.

Supported store snapshot verification uses Python's SQLite `deserialize` and
Linux OFD read locks against writers/checkpoints. Managed writes and opens also
share a process lock. Unsupported snapshot/locking capabilities refuse explicitly;
other platforms and direct unmanaged `store.conn` mutation are not qualified by
this batch. Workspace input must be quiescent; a nonempty source WAL refuses,
and source bytes are copied with no SQLite connection or sidecars in the source
root. No existing user database was opened for these checks.

Pseudonyms and column removal do **not** provide anonymity. Stable pseudonyms can
join records across runs in one store. Publishing both predictions and residuals
can derive targets. Even aggregate scores can reveal a target/error for `n=1` or
under auxiliary knowledge; scores-only output has no general privacy guarantee.
Restrictions and explicit local publication policy remain necessary. Metadata
validation cannot certify sharing rights or defeat scientific re-identification.

### Pinned frozen history: explicit one-off conversion only

The original `data/catalog-history-2026-09` pack stays immutable and unsupported
by normal ingestion. The separate package command accepts only its exact pins:

| Input | SHA-256 |
|---|---|
| `manifest.json` | `46fcab13599f366ee3db586ba328d117b6d7a478351cd90c7191cdc3fb71e37e` |
| `runs.jsonl.gz` | `e04427694d39d9815c41fefc87a5d20d425b0b28a78bfcdb2f7edb29b709abe7` |

The converter checks regular files, hashes read buffers before parsing, requires
one gzip member without trailing bytes, one final JSONL newline without blank
lines, strict JSON without duplicate keys/NaN, exact frozen key sets and counts,
and validates every v2 output. Inputs are rehashed before artifact publication.
A sibling temporary directory is fsynced and published with no-clobber rename;
identical existing digests are a no-op and different outputs refuse.

All 10,824 source records / 32,286 observations retain measured values and source
identities. Old safety/default claims, status `ok`, historical `sample` level,
CV default flags and empty residual claims remain verbatim only in
`provenance.frozen_source.source_claims`; v2 attestation, completion and historical
level are unreported. Test scope/partition and registered directions are retained.
Every record binds archive SHA, zero-based ordinal and source-line SHA; moved
pipeline extras and `historical_source` remain verbatim in the conversion artifact.
All identities are `source_declared_unverified`. All 5,826 restricted records stay
restricted/local-policy; the other 4,998 become unreported visibility, preserving
their old public claim solely as provenance. No historical record can rank.

The authoritative output identity is SHA-256 over **uncompressed canonical JSONL**:
`d29af7685161345775d69ac03a1c0f86120a7442bb99cc7921461e67176d4945`.
Two independent temporary paths with `PYTHONHASHSEED=17` and `91` produced this
same digest before it was pinned. The gzip digest is supplemental, since zlib
builds may differ; the converter-source hash belongs only in the pack manifest.
Neither compressed cross-platform reproducibility nor scientific execution was
qualified.

The explicit sequence, using new paths outside existing stores, is:

```sh
n4a-benchmarks convert-frozen-history --out /NEW/SCRATCH/converted-history
n4a-benchmarks ingest-history /NEW/SCRATCH/converted-history --store /NEW/SCRATCH/arena-v2
```

These placeholders must be replaced by fresh, isolated locations. The commands
do not migrate any old store. The converter is in the installed package for the
backend Docker command; no Docker build or deployment was run. The private
upstream collector is a separate dependency: `scripts/collect_history.py` refuses
`collect_history_owner_exporter_required` without reading private code.

### Publication and remaining owners

Historical checkpoint: for the current data-free Pages route, see [9 November 2026](#data-free-existing-site-route-9-november-2026).

Pages has no push trigger. Its first dispatch step fails with
`publication_gate_closed`, with no override; `build_site()` also refuses before
opening a store or creating output. Backend static pack selection still requires
explicit producer-reported public visibility, safe completed rankable records,
and a compatible score context across every projected table. The existing live
site remains online and stale; it was not rebuilt or altered. Reopening requires
the joint frontend migration, nonzero genuinely eligible records and an explicit
root decision. History alone cannot supply genuine missing source fold labels.
The browser v1 upload example remains a disclosed frontend dependency. Native
Core n4a, producer SDK corrections and service authentication/shared verifier
qualification remain under separate owners. No full ecosystem migration is
claimed.

### V2 commands and observed qualification

Each command below uses the bounded guard: all BLAS/OpenMP thread counts are 1,
unique temporary stores/caches, a 1,000,000,000-byte scratch limit, and a
2,684,354,560-byte free-space floor with 64 MiB reserve. Suites run sequentially.

```sh
.venv/bin/python -B scripts/check_arena_offline.py --v2-history-bootstrap
.venv/bin/python -B scripts/check_arena_offline.py --v2-regressions
.venv/bin/python -B scripts/check_arena_offline.py --v2-history
.venv/bin/python -B scripts/check_arena_offline.py --ruff
.venv/bin/python -B scripts/check_arena_offline.py --mypy
```

The final results are recorded below after execution; previous v1 suites/probes
above are historical checkpoints and are not v2 acceptance. Volatile external
review scripts are not a reproducible dependency: their wrappers skip cleanly
when no explicit source is supplied. The self-contained v2 regressions use
synthetic owner-shaped SQLite fixtures; they do not execute either producer
engine or perform fits. No owner success, attestation, public visibility or
canonical scientific identity is inferred from these checks.

Final executed checks for this v2 batch:

| Command option to the guard above | Observed result | Maximum scratch bytes | Minimum free bytes |
|---|---|---:|---:|
| `--v2-history-bootstrap` | Two paths/seeds agreed on the pinned JSONL digest | 5,607,424 | 4,658,106,368 |
| `--v2-regressions` | 90 passed (new v2 cases plus existing identity/scoring tests) | 19,980,288 | 4,442,816,512 |
| `--v2-history` | 7 passed, including the full 10,824-record transaction and publication fault rollback | 481,947,648 | 4,081,364,992 |
| `--ruff` | `ruff check src tests`: passed | 24,576 | 4,460,482,560 |
| `--mypy` | No issues in 60 source files | 49,225,728 | 4,411,084,800 |

The earlier full-pack attempt committed the scratch data but failed its last
privacy assertion because the unknown readmodel omitted its visibility field.
That field was added. A subsequent rerun stopped at the disk reserve after shared
free space fell; it is not a passing run. The final full-pack run above passed.
The guard now inspects only its scratch with no-atime directory reads, so disk
monitoring cannot itself change the timestamps in refusal fixtures. One installed
Starlette/httpx deprecation warning appeared in the focused tests; dependencies
were not installed or changed.

The regressions include both process and thread deduplication; live-writer
snapshot refusal without file changes; v1/foreign/WIP stores and WAL byte checks;
all eight generation keywords; collisions across whole workspaces and separate
ingests; resolved 1.039/2.389 separation; 0.49478/0.34076 refit level separation;
all twelve query lenses; train/validation/final contexts; missing/duplicate folds;
whitespace pseudo-folds; false/unknown/unsafe safety; strict version/hash/field
refusals; malformed pipeline/probability metadata; partial target arrays; DAG
variant/classification refusal; legacy-file refusal; static Python projection
privacy; residual derivability policy; and the closed publication gate. Partial
arrays are opaque/unranked, duplicate array IDs and missing pipeline structure
refuse, and unsupported nonempty probability shapes cannot be silently dropped.
These are self-contained metadata counterexamples, not owner-engine executions.

Remaining qualification limits are explicit. The entire old pytest suite was not
run as a v2 gate: retained tests/fixtures/proofs that assume v1 defaults or old
ranking semantics are incompatible and are not silently retrofitted. The general
CI Python matrix still invokes that legacy suite, so no whole-CI green status is
claimed. Its Docker smoke recipe now explicitly initializes its own empty v2
anonymous volume before health checks; Docker/CI were not executed. Normal Docker
startup remains uninitialized until explicit init/ingestion. Python 3.10 lacks
the snapshot capability required by this store; Linux/Python 3.11 was exercised,
other runtimes remain unqualified. Authentication, frontend migration, current
producer SDK correction, and the private collector remain separate work. SQLite
facts/receipts have transaction visibility; a failed inline-residual write may
leave an unreferenced, publication-policy-filtered Parquet blob, which is not
served without a committed residual-set row. Frozen history carries no residual
blobs. This batch does not claim cross-filesystem crash atomicity or scientific
validation. Independent Opus WORK review is pending; no commit or publication
was made.

Metadata-only intake does not fill absent scores by running a scorer. A failed
owner record with residuals and no scores is stored and replayed without adding
observations; recomputation requires an explicit policy and still refuses
unsupported owner context. Malformed DAG report/flag types also refuse explicitly.

The final preservation audit verified every original HEAD line in order (590 in
INGESTION.md, 258 in ADAPTERS.md), both immutable source pins, and `git diff
--check`. No frontend/static asset path is changed. The implementation/test/config
freeze contains 53 changed or preexisting dirty paths under `src/`, `tests/`,
`scripts/`, `.github/`, plus Dockerfile/compose/pyproject when changed; documents,
data README and preserved untracked AGENTS/codegraph configuration are excluded.
Its SHA-256 is
`b09c0c0a0affb87c65e5df8621ee7f03c0c86fd5f3c8a2d2835129956aa24ad1`,
computed over compact ASCII JSON of sorted `(relative_path, file_sha256)` pairs.
No implementation change followed this freeze. The independent reviewer should
review this working tree, including the recorded limitations, rather than infer
WORK acceptance from the earlier PLAN acceptance.

### R3–R7 restricted rework (2026-11-04)

This additive checkpoint follows the actual sole-v2 WORK review. It supersedes
only the earlier statements about uncleaned rollback blobs and unexecuted whole
pytest qualification. Earlier checkpoint results, original documentation and
frozen source proofs remain historical records; their freeze does not cover this
new rework. R1/R2 are explicitly frozen for their separate plan review: no WAL
parser, recovery/open ordering, whole-file snapshot or 512,000,000-byte ceiling
change is included here. Healthy stale-tail/crash stores and large stores can
therefore still be refused. This batch cannot claim overall WORK acceptance.

R3 rejects nonregular `arena.sqlite` entries before opening them. The shared file
reader opens nonblocking with no-follow and compares the opened regular inode to
its lstat result, including replacement by a FIFO between check and open. This
is a filesystem guard only, not a change to the frozen store recovery design.

R4 requires an explicit selected variant for supplied DAG metric reports,
prediction caches and explicit prediction blocks. Every supplied block must name
that same variant, including caches overridden by explicit blocks. Unlabelled,
multiple or mismatching variants refuse. A native residual tuple
`(sample_id, scope, fold_id, partition)` must be unique; a different partition
remains a different context. No pipeline identity is fabricated to rescue a
variant. R6 rejects genuine wire fold IDs with leading/trailing whitespace rather
than aliasing them; genuine arbitrary canonical string IDs remain supported and
`source_fold_id` keeps the producer's aggregate label.

R5 journals newly linked private residual inodes within the SQL ingestion
transaction. Rollback/cancellation removes only owned, uncommitted blobs while
the write lock is held. Immutable preexisting blobs are never overwritten; a
conflicting payload refuses. Tests cover a waiting writer, preexisting committed
and unreferenced blobs, late pack collision, cancellation, and interruption just
after durable commit. The latter preserves committed facts/blobs and permits
idempotent replay. A private staging file is removed on ordinary write failure.
This exception journal does not promise process-crash cleanup of staging files or
cross-filesystem crash atomicity. It does not alter the separate R1/R2 recovery
problem. No existing user store is opened, cleaned or migrated.

Allowed legacy backend tests now exercise native v2 semantics: explicit context
and identity basis, nullable evidence, separate legacy-hash refusal, normalized
hash verification, DB receipts, immutable residuals, ranking/publication refusal
and transaction/dedup behavior. Old identity pins are retained as v1 proof, with
separate observed native-v2 synthetic pins. Synthetic query fixtures explicitly
declare their test safety/visibility and record that no owner execution or
attestation was observed. Those declarations do not establish scientific
validity. No compatibility alias, v1 runtime fallback, CI scoping, added skip or
xfail is used to make the suite green. Site tests in the approved file exercise
only the closed backend gate and absence of output, with no asset build.

Whole-pytest execution installs the guards from `tests/conftest.py` through
`sitecustomize` before pytest/project imports, including Python child processes.
They deny owner/scientific runtime imports and numerical fitting entry points,
Internet sockets/DNS, non-Python executables except the exact audited Node
version query, dynamic native-library loads, environment installation, tree copies
and writes outside that run's scratch.
Local in-process ASGI transport remains available. Guard denials are failures,
not skips. Existing skips in untouched tests remain visible in the final report.
These are bounded test-execution controls, not a security sandbox for arbitrary
hostile Python. No actual owner training or native qualification is authorized.

The exact child gates are:

```sh
.venv/bin/python -B -m pytest -p no:cacheprovider -p pytest_cov \
  --basetemp="$ARENA_TEST_SCRATCH/pytest" --cov=nirs4all_benchmarks \
  --cov-report="xml:$ARENA_TEST_SCRATCH/coverage.xml" --tb=short \
  --junitxml="$ARENA_TEST_SCRATCH/report.xml"
.venv/bin/ruff check src tests --output-format=concise
.venv/bin/mypy
```

Before invoking pytest, create a fresh guard directory and run scratch, export
`ARENA_TEST_SCRATCH` and `ARENA_TEST_GUARD_DIR`, and put this in the guard directory's
`sitecustomize.py` (replace the repository path with the current absolute path):

```python
import runpy
runpy.run_path("/home/delete/nirs4all/nirs4all-benchmarks/tests/conftest.py",
              run_name="arena_guard_bootstrap")
```

Prepend the guard directory, repository `src` and `tests` to `PYTHONPATH`. Set
`PYTHONDONTWRITEBYTECODE=1`, `PYTEST_DISABLE_PLUGIN_AUTOLOAD=1`;
`OMP_NUM_THREADS`, `OPENBLAS_NUM_THREADS`, `MKL_NUM_THREADS`, `NUMEXPR_NUM_THREADS`,
`VECLIB_MAXIMUM_THREADS`, `BLIS_NUM_THREADS`, `OMP_THREAD_LIMIT` all to `1`.
Place `TMPDIR`, `TEMP`, `TMP`, `XDG_CACHE_HOME`, `RUFF_CACHE_DIR`, `MYPY_CACHE_DIR`,
`COVERAGE_FILE` and `NIRS4ALL_BENCHMARKS_STORE` under that scratch. The parent
supervisor checks free space and allocated scratch bytes every 0.25 seconds,
uses no-atime directory reads only within its own scratch, and terminates the
child process group if free space falls below 2,684,354,560 + 67,108,864 bytes or
scratch exceeds 1,000,000,000 - 67,108,864 bytes. Stores/caches are removed after
each sequential suite. These commands require this supervisor; an unguarded
whole-suite run is not authorized by this checkpoint. No volatile external
review script is a dependency: the R3–R6 counterexamples are self-contained in
`tests/test_arena_v2_work_review.py` and call production APIs with isolated
synthetic fixtures. They mirror the review's DAG/orphan/FIFO/padded-ID probes;
the original external scripts were read, not executed wholesale (their unrelated
open/recovery and tree-copy sections are outside this batch).

The root's later Node clarification narrows the executable guard: the installed
`/usr/bin/node` is allowed only with the exact arguments
`["-p", "process.versions.node.split('.')[0]"]`, inherited guarded environment and
no `NODE_OPTIONS`/`NODE_PATH` preload. This is a read-only runtime-version query,
not scientific execution. Other Node flags, script paths and shell stubs are
still refused, with positive and negative guard tests. No arbitrary Node eval is
allowed. Descriptor-relative writes resolve through the actual directory fd;
creation/truncation binds and verifies the owned scratch parent and refuses a
replaced final symlink. Descriptor-relative rename/unlink/mkdir/rmdir checks use
their real fd context. A regression verifies permitted scratch operations and
refusal of outside writes. These checks do not loosen the R3 store file guard.

The remaining Node invocations were inspected and reported before any execution:

| Exact argv shape from current tests/source | Purpose / required separate grant |
|---|---|
| `[node, "driver.mjs", view, json.dumps(scenario)]` in `tests/test_view_rendering.py:61` | Imports copied frontend views and calls their render lifecycle against DOM stubs (matrix, leaderboard, statistics, operator-effect, robustness, overview, runs). Requires joint frontend/view and asset-copy authorization; remains blocked before copy/render. |
| `[node, "driver.mjs", str(tmp_path / "site" / "data" / "bundle.json"), queries_json]` in `tests/test_static_engine_parity.py:111` | Static numerical query parity after a site build. Requires a v2 frontend pack and explicit site/asset/numerical-parity grant; not run. The old v1 input also refuses. |
| `[runtime_node, "--input-type=module", "--eval", _CHILD_SCRIPT]` in `qualification/web_wasm_adapter.py:524` and `tests/test_web_wasm_adapter.py:321` | Archive-v2 WASM replay/prediction and a refit-call detection lifecycle. Requires explicit audited WASM runtime/numerical authorization, not granted. The test's alternate executable is a shell fixture named `node`; that is not the audited installed Node binary. |

The original guarded whole-run failure log and JUnit output are preserved
unchanged. Their SHA-256 values are respectively
`c75b4967bb20b78a244d377dc5398037b31f7e4de878a70b9bd8e6731944fbc5` and
`8067c14a9fd610983a3461dd00ea60df927a82c7acbc0d92032016df0cc971b7`.
That diagnostic run had 535 passed, 30 failed, 43 setup errors and one preexisting
skip; its history durability assertion hit the old fd-relative guard refusal.
It is not a green result. Coverage was enabled for that full run and stays
enabled for the rerun; the rerun also retains its coverage XML with its log and
JUnit report. No coverage/test scoping or skip was introduced to hide failures.

The reviewer DAG section and genuine-orphan section were then executed verbatim
inside a new guarded scratch, with only a local import/temp-root preamble.
Two report variants, multiple bundle variants and two prediction-cache variants
refused with `dagml_variant_context_unsupported`; loser-versus-winner reports
refused with `dagml_variant_selection_mismatch`; direct duplicate residual keys
were rejected. The genuine pack collision refused at ordinal 1 with zero
executions, collections, batches, receipts, residual sets and orphan files.
The external scripts themselves remain unmodified. Their unrelated recovery,
whole-conversion, tree-copy and numerical/native sections were not executed.

The first completed run's coverage XML was in its cleaned temporary scratch and
was not retained; no availability of that historical XML is claimed. Its full
failure output and JUnit remain intact, and the coverage flags were not removed.
The current supervisor separately retains the rerun's XML before cleaning stores.

Final restricted-batch results (Linux/Python 3.11, sequential guarded execution):

| Check | Observed result | Peak scratch bytes | Minimum free bytes |
|---|---|---:|---:|
| Focused native-v2 backend ports (17 test files, intermediate checkpoint) | 428 passed | 178,290,688 | 3,801,661,440 |
| Final R3–R6 + guard regressions, `pytest tests/test_arena_v2_work_review.py --tb=short` under the guard above | 22 passed; also passed in the final whole run | 2,347,008 | 4,302,090,240 |
| Exact reviewer DAG/genuine-orphan sections with local import/temp preamble | Typed refusals; late ordinal-1 refusal left 0 facts/receipts/blobs | 372,736 | 4,301,869,056 |
| `ruff check src tests --output-format=concise` | Passed | 0 | 4,301,529,088 |
| `mypy` | Passed, 60 source files | 48,283,648 | 4,252,758,016 |
| Unscoped whole pytest with coverage, exact child flags above | **538 passed, 17 failed, 43 setup errors, 13 skipped** in 417.49 s; exit 1 | 518,053,888 | 3,772,952,576 |

The full 10,824-record converted-history transaction, unranked/restricted facts,
source pins and publication durability-failure cleanup all passed in the final
whole run. Metadata qualification is not scientific validation. Coverage XML was
produced and retained; no threshold or selected-file coverage substitute was
used. Both completed whole-run logs/JUnit outputs remain unchanged. The earlier
profile-based diagnostic was deliberately terminated and is not a passing check.
The installed Node major is actually **12**. Twelve untouched tests independently
skip because they require Node >=18 after the allowed metadata query; the other
existing skip needs `N4A_TEST_NODE`. No Node upgrade/install was attempted, no skip
was added, and no frontend/WASM driver was run.

R7 remains open: whole CI is not green. Remaining work must not weaken strict v2,
open the publication gate, fabricate evidence, or silently scope/skip tests.
The exact files/permissions needed beyond this batch are:

| Current obstruction | Required next scope / execution grant |
|---|---|
| Three cases in `tests/test_arena_targeted_recheck.py` retain old context/residual defaults or expect adapter refusal later | Explicit permission to edit that file (not in the R3–R7 allowlist) and port those cases to native v2; genuine coverage/privacy behavior remains covered in allowed v2 tests. |
| `tests/test_history.py::test_real_charts_recover_from_saved_demo_filters` still uses the v1 browser helper | Joint frontend/browser test retirement or v2 migration authorization. Its browser body was preserved, and cannot publish under the closed gate. |
| `tests/test_site_boot.py`, `tests/test_static_engine_parity.py`, `tests/test_view_rendering.py` | Joint v2 frontend/test migration and explicit browser/view/static numerical/asset/site authorization; also an available compatible Node runtime for tests that require >=18. No such work was run. |
| Six cases in `tests/test_artifact_qualification.py` and four in `tests/test_performance_compare.py` | A separate narrow audit/allowance for the installed `n4a-gate-json-adapter` Python console script and synthetic shebang fixture adapter, or authorized launcher/test changes. The current guard allows the interpreter executable but refuses these executable paths. These are metadata/contract fixtures; their failures do **not** establish a need for actual owner fitting. Neither file is in this batch's edit allowlist. |
| Sixteen setup errors in `tests/test_python_legacy_adapter.py` and 27 in `tests/test_scientific_runtime_adapter.py` | Explicit isolated environment-creation/wheel-fixture setup grant or authorized test refactoring. Setup stops at `environment_installation` before runtime execution. That grant would not itself authorize actual owner training or native computation; those remain separate. |
| Two failures and the opt-in case in `tests/test_web_wasm_adapter.py` | Separate audit/grant for its shell stub named `node` and, if requested, the actual WASM child argv documented above. No WASM replay/refit lifecycle was executed. |
| Store R1/R2 review counterexamples | Separate approved store-open/recovery plan and implementation; these structures were not edited by this batch. |

Exact outstanding node IDs follow, including unavailable existing skips rather
than treating them as passes:

```text
FAILURE
tests/test_arena_targeted_recheck.py::test_cv_fold_series_cannot_be_filled_by_aggregate_or_other_metric
tests/test_arena_targeted_recheck.py::test_workspace_score_only_cv_fold_shape_is_counted
tests/test_arena_targeted_recheck.py::test_publication_flags_are_not_cross_run_or_single_sample_score_privacy
tests/test_artifact_qualification.py::test_json_adapter_qualifies_all_four_explicit_artifacts
tests/test_artifact_qualification.py::test_cli_writes_the_terminal_json_report
tests/test_artifact_qualification.py::test_unfinished_output_is_failed
tests/test_artifact_qualification.py::test_executed_but_incomparable_output_is_refused
tests/test_artifact_qualification.py::test_declared_fallback_is_refused
tests/test_artifact_qualification.py::test_numerical_mismatch_fails_candidate
tests/test_history.py::test_real_charts_recover_from_saved_demo_filters
tests/test_performance_compare.py::test_comparison_uses_same_archive_matrix_and_descriptor_for_all_surfaces
tests/test_performance_compare.py::test_numeric_divergence_fails_surface_without_a_performance_threshold
tests/test_performance_compare.py::test_adapter_identity_drift_is_rejected
tests/test_performance_compare.py::test_module_smoke_writes_one_repeat_report_and_web_handoff
tests/test_site_boot.py::test_static_site_recovers_from_startup_failures
tests/test_web_wasm_adapter.py::test_web_launcher_finalizes_one_multitarget_replay
tests/test_web_wasm_adapter.py::test_web_launcher_refuses_a_missing_child_fit_counter
ERROR
tests/test_python_legacy_adapter.py::test_python_oracle_finalizes_only_explicit_legacy
tests/test_python_legacy_adapter.py::test_python_oracle_refuses_native_and_fallback_before_execution[engine-native-literal engine='legacy']
tests/test_python_legacy_adapter.py::test_python_oracle_refuses_native_and_fallback_before_execution[engine-dag-ml-literal engine='legacy']
tests/test_python_legacy_adapter.py::test_python_oracle_refuses_native_and_fallback_before_execution[allow_fallback-True-explicitly forbid fallback]
tests/test_python_legacy_adapter.py::test_python_oracle_refuses_tampered_wheel_before_execution
tests/test_python_legacy_adapter.py::test_python_oracle_refuses_source_checkout_artifact
tests/test_python_legacy_adapter.py::test_python_oracle_refuses_ambient_runtime_members_before_execution[ambient.py-raise RuntimeError\n-undeclared member]
tests/test_python_legacy_adapter.py::test_python_oracle_refuses_ambient_runtime_members_before_execution[ambient.pth-/unselected/source\n-forbidden ambient member]
tests/test_python_legacy_adapter.py::test_python_oracle_refuses_ambient_runtime_members_before_execution[package/__pycache__/ambient.cpython-311.pyc-pyc-forbidden ambient member]
tests/test_python_legacy_adapter.py::test_python_oracle_refuses_runtime_symlinks_before_execution[file]
tests/test_python_legacy_adapter.py::test_python_oracle_refuses_runtime_symlinks_before_execution[directory]
tests/test_python_legacy_adapter.py::test_python_oracle_refuses_symlink_in_active_site_path
tests/test_python_legacy_adapter.py::test_python_oracle_refuses_installed_member_tamper_before_execution
tests/test_python_legacy_adapter.py::test_python_oracle_refuses_path_digest_swap_before_execution
tests/test_python_legacy_adapter.py::test_python_oracle_refuses_inter_wheel_path_collision
tests/test_python_legacy_adapter.py::test_python_oracle_refuses_runtime_mutation_after_child
tests/test_scientific_runtime_adapter.py::test_core_launcher_produces_finalized_observations
tests/test_scientific_runtime_adapter.py::test_core_launcher_never_executes_site_bootstrap_before_inventory
tests/test_scientific_runtime_adapter.py::test_core_launcher_refuses_extra_installed_file
tests/test_scientific_runtime_adapter.py::test_core_launcher_refuses_wheel_without_public_matrix_api
tests/test_scientific_runtime_adapter.py::test_launcher_refuses_declared_fallback_before_execution
tests/test_scientific_runtime_adapter.py::test_launcher_refuses_missing_explicit_artifact_role
tests/test_scientific_runtime_adapter.py::test_launcher_refuses_obsolete_dag_ml_wheel_role
tests/test_scientific_runtime_adapter.py::test_launcher_refuses_duplicate_expected_artifact_role
tests/test_scientific_runtime_adapter.py::test_launcher_refuses_symlink_scenario
tests/test_scientific_runtime_adapter.py::test_launcher_refuses_oversize_scenario
tests/test_scientific_runtime_adapter.py::test_launcher_refuses_unrecorded_wheel_member
tests/test_scientific_runtime_adapter.py::test_launcher_refuses_duplicate_wheel_member
tests/test_scientific_runtime_adapter.py::test_launcher_refuses_recorded_wheel_symlink
tests/test_scientific_runtime_adapter.py::test_launcher_refuses_false_wheel_record_identity[1-sha256=zN01Foq0dPpXZKUmz7g2ITUeI2gsUHWy4Y1Wvd-WqjA-bytes differ from RECORD]
tests/test_scientific_runtime_adapter.py::test_launcher_refuses_false_wheel_record_identity[2-999999-size differs from RECORD]
tests/test_scientific_runtime_adapter.py::test_launcher_refuses_unsafe_recorded_wheel_path[C:/escape.py]
tests/test_scientific_runtime_adapter.py::test_launcher_refuses_unsafe_recorded_wheel_path[nirs4all_core\\escape.py]
tests/test_scientific_runtime_adapter.py::test_launcher_refuses_symlink_artifact[runtime_python]
tests/test_scientific_runtime_adapter.py::test_launcher_refuses_symlink_artifact[core_wheel]
tests/test_scientific_runtime_adapter.py::test_launcher_refuses_symlink_artifact[pyyaml_wheel]
tests/test_scientific_runtime_adapter.py::test_launcher_refuses_symlink_artifact[archive_v2]
tests/test_scientific_runtime_adapter.py::test_launcher_refuses_symlink_artifact[methods_library]
tests/test_scientific_runtime_adapter.py::test_launcher_refuses_non_regular_artifact
tests/test_scientific_runtime_adapter.py::test_launcher_refuses_component_version_that_differs_from_wheel
tests/test_scientific_runtime_adapter.py::test_core_launcher_forbids_native_facade_masquerading_as_python_oracle
tests/test_scientific_runtime_adapter.py::test_launcher_refuses_installed_bytes_that_differ_from_wheel
tests/test_scientific_runtime_adapter.py::test_launcher_refuses_non_executable_runtime
SKIPPED
tests/test_static_engine_parity.py::test_composition_and_operator_graph_match_python
tests/test_view_rendering.py::test_matrix_keeps_pipelines_and_datasets_with_equal_labels_apart
tests/test_view_rendering.py::test_leaderboard_keeps_equal_labels_apart_and_names_extrema_by_direction
tests/test_view_rendering.py::test_statistics_violins_for_same_named_datasets_do_not_share_a_name
tests/test_view_rendering.py::test_operator_effect_puts_the_best_operator_at_the_top[min]
tests/test_view_rendering.py::test_operator_effect_puts_the_best_operator_at_the_top[max]
tests/test_view_rendering.py::test_robustness_highlights_the_better_corner_for_the_metric_direction[rmse-min-left-min]
tests/test_view_rendering.py::test_robustness_highlights_the_better_corner_for_the_metric_direction[accuracy-max-right-max]
tests/test_view_rendering.py::test_overview_queries_a_score_level_the_store_has[scopes0-cv]
tests/test_view_rendering.py::test_overview_queries_a_score_level_the_store_has[scopes1-test]
tests/test_view_rendering.py::test_overview_queries_a_score_level_the_store_has[scopes2-fold]
tests/test_view_rendering.py::test_run_explorer_requests_every_matching_run
tests/test_web_wasm_adapter.py::test_node_child_refuses_a_refit_call
```

Final preservation/scope audit: every original HEAD documentation line remains
in order (590 INGESTION, 258 ADAPTERS), both immutable source pins match, and the
existing history-browser body is unchanged. All differences against the reviewer
freeze among non-asset paths are in the exact R3–R7 allowlist, plus the authorized
new regression file. Assets were not opened by this audit. No implementation
change follows this checkpoint. The 24 changed implementation/test files
(excluding additive documentation) have SHA-256 `1b69617877116c4e54b9b8ca80ac35a9f6514382c80e3182a64d15e08682433e`,
computed over compact ASCII JSON of sorted `(relative_path, file_sha256)` pairs.
The earlier reviewer manifest/artifacts are preserved, not replaced. Independent
WORK re-review is still required; no acceptance, commit or publication is claimed.

### Approved R1/R2 recovery successor (2026-11-04)

This checkpoint implements only the root-authorized store-open amendment, plan
SHA-256 `f4c35c9f914030e30461f7a0ef9529acb8f400a70ad9618723f6ba8de0a2eb2e`.
The text closure accepts A1–A4 with **all nine test rows**, not eight. The sealed
`arena-r3-checkpoint` remains unchanged: all 207 source entries and 65 changed-file
preimages matched before work. Its raw R3–R6 proofs and previous blocked whole
results are preserved. This successor supersedes the earlier R1/R2 freeze and
snapshot-open rules only; it does not authorize schema 1, migration, publication,
new owner evidence or scientific computation.

The main-file preflight now reads exactly 100 header bytes. It requires a regular
nonsymlink main file, matching lstat/fstat device and inode, nonblocking/no-follow
and close-on-exec flags. `O_NOATIME` is attempted and retried without it on EPERM;
atime is excluded from non-mutation promises. A process-local `(device, inode)`
registry counts live managed SQLite connections. The same process lock covers
header check, connection creation, registration, close and health identification.
While a managed connection is live, another open or `/api/healthz` reuses its
verified header result and performs no raw main-file open/close. Explicit close
and garbage collection release the registry entry; inode reuse after close is
not trusted from a stale cache. Sidecar `-wal`, `-shm` and `-journal` entries must
be absent or regular nonsymlinks before a valid-header SQLite connection.

After a claimed owned-v2 header, normal SQLite opens the escaped `Path.as_uri()`
URI with `mode=rw` (no accidental creation). Busy timeout is set before the first
metadata read. SQLite performs its own recovery of committed WAL frames and
ignores an uncommitted/torn tail according to SQLite semantics. The schema
fingerprint and required metadata are read before WAL mode, directory creation,
blob work or application writes. Busy/locked SQLite failures map to
`arena_store_busy`; other SQLite database failures map to `arena_store_corrupt`.
The handwritten WAL/checksum/snapshot parser, OFD/deserialize snapshot lane,
whole-file copy/read and arbitrary 512 MB opening limit are removed. Existing
atomic fresh provisioning and the R3–R6 adapter/validation/blob fixes remain.

The exact limits are intentional. A matching header is an ownership **claim**.
SQLite recovery/close may rewrite main-file pages and create/delete/checkpoint
sidecars before a forged claim's wrong fingerprint is refused. Such tests assert
no application rows and no arrays/exports, **not byte equality**. The fingerprint
is a schema-version check, not authentication: copied header plus fingerprint can
pass. Unclaimed v1/foreign/empty files refuse before SQLite, with bytes, mtimes
and sidecar set unchanged. The fingerprint hashes `schema.sql` text; that file
remains frozen, and even a comment edit is not claimed compatible. Linux/Python
3.11 is the qualified environment here. Windows/macOS, network filesystems,
read-only WAL recovery and cross-mount hardlink provisioning are unqualified or
unsupported. No generic legacy fallback, runtime migration, actual owner run,
attestation, scientific dataset identity or scientific validation is introduced.

All nine reviewed rows have focused production-path tests in
`tests/test_arena_v2_work_review.py`:

| Reviewed row | Exact coverage |
|---|---|
| 1: crash / torn final frame / second open | Thirty committed ingests then `os._exit`; an uncommitted spilled final frame is truncated by 100 bytes and all thirty durable rows remain; fifteen ingests with a second live open. A torn uncommitted tail is not represented as a durable lost commit. |
| 2: sparse store >512 MB | Opens a 512,000,001-byte sparse fixture, with instrumented header reads exactly 100 bytes. Only that temporary sparse fixture is removed afterward. |
| 3: live connection + repeated open + healthz | Instrumented zero raw main-file opens during repeated store/header checks and real in-process `/api/healthz`; live refcounts and final release checked; URI contains spaces, `#`, `?` and `%`. |
| 4: FIFO / replacement race | Retained R3 timeout-bounded FIFO and lstat/open replacement probes, plus best-effort no-atime and required-flag assertions. |
| 5: unsafe valid-header sidecars | Symlinked WAL, FIFO WAL/SHM and directory journal refuse before any SQLite connect. |
| 6: removal after header | Removal at the connect boundary demonstrates `mode=rw` refuses and creates no empty database. |
| 7: forged matching header / wrong fingerprint | A live crash WAL carries the wrong fingerprint; SQL trace proves no application statements before refusal, no arrays/exports and zero executions. Byte equality is deliberately not asserted. |
| 8: v1 live WAL / foreign / empty | SQLite connect is forbidden by instrumentation; bytes, mtimes and sidecar set stay unchanged. |
| 9: eight-process fresh race | All eight open the same new temporary root successfully, with no provisioning leftovers. |

Additional probes distinguish busy/corrupt refusals and verify registry release
on garbage collection. The three explicitly allowed obsolete cases in
`tests/test_arena_targeted_recheck.py` now supply native-v2 score/residual context,
expect atomic workspace preflight refusal at its current boundary, and retain
cross-run joining and n=1 score-inference coverage. No other case in that file is
retired, skipped or weakened.

The necessary execution-guard amendment grants only the root's named existing
Node tests. Node 22 **is present** at
`/home/delete/.nvm/versions/node/v22.21.1/bin/node`; the earlier `/usr/bin/node`
major-12 observation did not establish absence of Node 22. For approved fake-DOM
cases, the scoped node locator selects that pinned binary and child `PATH` is
prepended with its `bin` directory. Parent PATH is not globally changed. Explicit
fixture executable paths still route to their fixtures. The guard validates the
existing fake-DOM driver text/argv and restricts copies to its read-only views/lib
sources and owned scratch. The two approved shell stubs must exactly match their
existing literal fixture (including archive digest and the deliberately missing
counter variant). The approved fake-JS refit detector must use the exact child
script and fake entrypoint/archive payload; no actual WASM module or native fit
is allowed. Node metadata argv remains narrowly checked. These tests establish
contract behavior of fixtures, never actual scientific execution.

Static engine parity remains collected by whole pytest but is **unexecuted**:
a guard raises `arena_test_unexecuted_gate:joint_frontend_static_engine_parity`
before its body/fixtures, rather than invoking its v1 site build, deselecting it
or reporting a skip/pass. No frontend/history/chart source, CI selector or site
asset is changed. No browser or site generation is run. Other console/wheel
fixture commands were inspected and reported before any further execution grant:

```text
[.venv/bin/n4a-gate-json-adapter]
[scratch/adapter]
[/usr/bin/python3.11, -m, venv, --without-pip, --copies, scratch/runtime]
[runtime_python, -I, -S, -B, -c, _CHILD_SCRIPT]
```

The first two consume synthetic JSON contract fixtures, not actual owner fits;
they still require a separate narrow executable allowance and remain blocked.
The wheel fixtures stop at environment creation; neither fixture setup nor the
isolated runtime argv is authorized by the Node grant. All prior no-fit,
no-network, no-install/native-execution guards remain. There is no new skip,
coverage removal, CI scoping or compatibility branch to manufacture a green run.

Two additional A1/provisioning counterexamples were investigated within this
same approved store file. First, a regular-file replacement inside `os.open`
could redirect the raw descriptor to a different inode with a live managed
SQLite connection. The failing probe intercepts the unsafe close before it can
cancel locks. The fixed path refuses the mismatched inode and defers that raw
fd's close until its last registered SQLite connection closes. Normal repeated
opens/health checks still issue zero raw opens for the live inode. The deferred
queue is drained on explicit/GC close; it is not a WAL snapshot or parser.
Second, an actual eight-process run exposed a stale "database absent" observation:
a peer published before the directory listing, and the loser incorrectly refused
the now-nonempty root. The preflight now recognizes that peer's regular inode and
continues existing-store validation instead of treating it as a fresh empty root.
Three independent eight-process roots must all pass; failures are not retried
until green, and every child is joined/terminated before the case ends. Fresh
provisioning itself and the existing transaction/query methods remain byte-for-
byte the sealed implementations.

The first successor whole run is retained as a diagnostic: 571 passed, 14 failed,
44 errors, zero skips in 441.32 s; peak scratch 517,742,592 bytes and minimum free
3,714,908,160 bytes. The failed descriptor-swap probe and intermittent fresh-race
output are also retained. Neither is relabelled as a passing run. After the fixes,
the combined focused profile passed **97 cases** (all nine rows, R3–R6, three
obsolete-v1 conversions, approved fake-DOM and fake-JS/shell fixtures) in 6.66 s;
peak scratch 18,423,808 bytes and minimum free 4,208,402,432 bytes. The subsequent
whole run keeps coverage and all collected cases, including the explicit
unexecuted static-parity gate. A temporary observer records actual subprocess
argv and launched/refused outcome without changing the execution guard. Per-run
source manifests, safe environment settings, command arrays, logs, JUnit and
coverage are retained outside product files. No observer result attests that a
scientific fit or real owner engine ran.

A second registry lifecycle probe demonstrated a real stale-refcount error when
Python collected the last store reference on a different thread: SQLite's Python
thread-affinity check rejected `close()`. The connection now permits that
serialized lifecycle close under the registry lock; this is not a new shared SQL
or concurrent-transaction API. The fixture verifies no ignored finalizer exception,
no registry/deferred-fd residue and a successful reopen. The observed runtime is
Python 3.11.15 / SQLite 3.37.2, DB-API threadsafety 3. The final focused profile
passed **98 tests** in 7.09 s, observed scratch 18,800,640 bytes and minimum free
4,193,898,496 bytes. Ruff passed across `src tests`; mypy passed all 60 source
files after this correction. The preceding whole run (before this last lifecycle
case) remains recorded as 574 passed, 14 failed and 44 errors, zero skips; it is
not substituted for the final-source whole run.

The next whole run preserved a further genuine failure in row 9: one peer's
open reported `arena_store_busy`. Pytest truncated the child traceback, so the
retained output does not prove the exact failing SQLite statement. WAL-mode lock
upgrade was the implementation hypothesis, not a separately captured observation.
SQLite does not invoke its busy handler for every mode-change lock upgrade. The
opener now retries only BUSY/LOCKED from that PRAGMA within a bounded busy-timeout
budget, then reports `arena_store_busy`; corruption is not retried.
Recovery itself remains SQLite-owned. Deterministic probes inject BUSY, LOCKED,
corruption and an exhausted busy budget, in addition to the real three-root race.
The failing whole result (574 passed, 15 failed, 44 errors, zero skips) remains a
diagnostic, including the row-9 failure; it is not hidden as a flaky skip or test
retry. With this last correction, the focused profile passed **102 tests** in
6.06 s, observed peak scratch 20,295,680 bytes and minimum free 4,182,765,568 bytes.
Final-source Ruff and mypy (60 files) passed before the whole rerun.

Final-source R1/R2 qualification completed without a resource stop:

| Gate | Actual result | Observed maximum scratch | Observed minimum free |
|---|---|---:|---:|
| Focused nine-row/R3–R6/v2-conversion/approved Node profile | 102 passed, 1 installed Starlette/httpx deprecation warning | 20,295,680 bytes | 4,182,765,568 bytes |
| `ruff check src tests --output-format=concise` | Passed | 0 bytes | 4,162,490,368 bytes |
| `mypy` | Passed, 60 source files | 48,922,624 bytes | 4,112,068,608 bytes |
| Whole pytest with coverage, no selectors or skips added | **579 passed, 14 failed, 44 errors, 0 skipped**, 426.66 s, exit 1 | 517,701,632 bytes | 3,609,604,096 bytes |

All new recovery rows (including all three eight-process roots), R3–R6 regressions,
the three authorized v2 test conversions, and every explicitly granted fake-DOM /
fake-JS / shell-stub case passed in the final whole run. The full history metadata
transaction and durability test also passed; no scientific validation is claimed.
The launch observer recorded 26 allowed Node/stub launches: 22 version/driver
calls for eleven fake-DOM cases, two shell stubs plus one fake-JS detector, and the
retained narrow system-Node metadata guard probe. No static-parity body or child
command ran. The static case is an explicit unexecuted-gate error, not a skip.
No actual WASM/native fit, browser lifecycle, site generation or package install
was executed. Source-level frontend/history/chart files and CI selectors are
unchanged.

The exact supervised child commands were (the temporary observer only records
launch/refusal and grants no permission):

```sh
.venv/bin/python -B -m pytest -p no:cacheprovider -p arena_r1_capture \
  --basetemp="$ARENA_TEST_SCRATCH/pytest" \
  tests/test_arena_v2_work_review.py tests/test_arena_targeted_recheck.py \
  tests/test_view_rendering.py tests/test_web_wasm_adapter.py --tb=short \
  --junitxml="$ARENA_TEST_SCRATCH/report.xml"
.venv/bin/ruff check src tests --output-format=concise
.venv/bin/mypy
.venv/bin/python -B -m pytest -p no:cacheprovider -p arena_r1_capture -p pytest_cov \
  --basetemp="$ARENA_TEST_SCRATCH/pytest" --cov=nirs4all_benchmarks \
  --cov-report="xml:$ARENA_TEST_SCRATCH/coverage.xml" --tb=short \
  --junitxml="$ARENA_TEST_SCRATCH/report.xml"
```

The same before-import bootstrap and environment/resource supervisor described
above apply: all seven BLAS/OpenMP controls are 1, stores/caches/default paths
are isolated under a unique owned scratch, monitoring runs every 0.25 seconds
with a 64 MiB reserve before the 2,684,354,560-byte free floor and 1 GB scratch
ceiling. Per-run manifests and result JSON retain the actual resolved command,
environment controls, exit code and observed resource bounds. Coverage XML,
JUnit, raw pytest output, subprocess argv/outcomes and harness sources are
retained, including every preceding failed probe/run. No failure is relabelled
as owner execution success or hidden by loss of coverage.

The whole suite is **not green**. Remaining results are classified as follows;
none authorizes edits outside this batch's five-file allowlist:

| Remaining result | Classification / next authorization needed |
|---|---|
| `tests/test_arena_v2.py::test_foreign_roots_empty_files_symlinks_and_fingerprint` | Fails its old directory-atime equality at line 234. A2 explicitly excludes atime; the later forged-claim byte-equality assertion in the same function must also adopt A4. This file is outside the allowlist. Bytes/mtimes/sidecars for unclaimed headers remain tested in the new row 8. |
| `tests/test_arena_v2.py::test_live_owned_writer_refuses_snapshot_without_modifying_files` | Still demands the removed snapshot refusal. Healthy owned-WAL readers now open under the accepted recovery contract. This test needs an authorized expectation update, not restoration of the rejected opener. |
| Six failures in `tests/test_artifact_qualification.py`; four in `tests/test_performance_compare.py` | Their installed JSON console entrypoint / synthetic shebang adapter are refused before execution by the current guard. These are unavailable contract-fixture launches, not observed owner/numerical failures. Exact argv above and raw refusal records are retained; a separate narrow execution allowance is needed. |
| `tests/test_history.py::test_real_charts_recover_from_saved_demo_filters` | Old v1 browser helper refuses before chart/browser work. Requires a joint frontend/v2 test migration grant; the file/body was not edited. |
| `tests/test_site_boot.py::test_static_site_recovers_from_startup_failures` | Closed publication gate; no site/browser generated. Joint frontend/publication authorization remains separate. |
| 16 setup errors in `tests/test_python_legacy_adapter.py`; 27 in `tests/test_scientific_runtime_adapter.py` | Environment-creation fixture calls are blocked before setup/runtime execution. Any fixture-environment allowance must be explicit; it would not grant actual owner fitting or the unguarded `-I -S` children. |
| `tests/test_static_engine_parity.py::test_composition_and_operator_graph_match_python` | Explicit **unexecuted joint frontend gate**. No v1 site build, numerical JS driver, selector change or skip was performed. |

Exact remaining node IDs from the final JUnit follow:

```text
FAILURES
tests/test_arena_v2.py::test_foreign_roots_empty_files_symlinks_and_fingerprint
tests/test_arena_v2.py::test_live_owned_writer_refuses_snapshot_without_modifying_files
tests/test_artifact_qualification.py::test_json_adapter_qualifies_all_four_explicit_artifacts
tests/test_artifact_qualification.py::test_cli_writes_the_terminal_json_report
tests/test_artifact_qualification.py::test_unfinished_output_is_failed
tests/test_artifact_qualification.py::test_executed_but_incomparable_output_is_refused
tests/test_artifact_qualification.py::test_declared_fallback_is_refused
tests/test_artifact_qualification.py::test_numerical_mismatch_fails_candidate
tests/test_history.py::test_real_charts_recover_from_saved_demo_filters
tests/test_performance_compare.py::test_comparison_uses_same_archive_matrix_and_descriptor_for_all_surfaces
tests/test_performance_compare.py::test_numeric_divergence_fails_surface_without_a_performance_threshold
tests/test_performance_compare.py::test_adapter_identity_drift_is_rejected
tests/test_performance_compare.py::test_module_smoke_writes_one_repeat_report_and_web_handoff
tests/test_site_boot.py::test_static_site_recovers_from_startup_failures
ERRORS (43 blocked setup; 1 unexecuted gate)
tests/test_python_legacy_adapter.py::test_python_oracle_finalizes_only_explicit_legacy
tests/test_python_legacy_adapter.py::test_python_oracle_refuses_native_and_fallback_before_execution[engine-native-literal engine='legacy']
tests/test_python_legacy_adapter.py::test_python_oracle_refuses_native_and_fallback_before_execution[engine-dag-ml-literal engine='legacy']
tests/test_python_legacy_adapter.py::test_python_oracle_refuses_native_and_fallback_before_execution[allow_fallback-True-explicitly forbid fallback]
tests/test_python_legacy_adapter.py::test_python_oracle_refuses_tampered_wheel_before_execution
tests/test_python_legacy_adapter.py::test_python_oracle_refuses_source_checkout_artifact
tests/test_python_legacy_adapter.py::test_python_oracle_refuses_ambient_runtime_members_before_execution[ambient.py-raise RuntimeError\n-undeclared member]
tests/test_python_legacy_adapter.py::test_python_oracle_refuses_ambient_runtime_members_before_execution[ambient.pth-/unselected/source\n-forbidden ambient member]
tests/test_python_legacy_adapter.py::test_python_oracle_refuses_ambient_runtime_members_before_execution[package/__pycache__/ambient.cpython-311.pyc-pyc-forbidden ambient member]
tests/test_python_legacy_adapter.py::test_python_oracle_refuses_runtime_symlinks_before_execution[file]
tests/test_python_legacy_adapter.py::test_python_oracle_refuses_runtime_symlinks_before_execution[directory]
tests/test_python_legacy_adapter.py::test_python_oracle_refuses_symlink_in_active_site_path
tests/test_python_legacy_adapter.py::test_python_oracle_refuses_installed_member_tamper_before_execution
tests/test_python_legacy_adapter.py::test_python_oracle_refuses_path_digest_swap_before_execution
tests/test_python_legacy_adapter.py::test_python_oracle_refuses_inter_wheel_path_collision
tests/test_python_legacy_adapter.py::test_python_oracle_refuses_runtime_mutation_after_child
tests/test_scientific_runtime_adapter.py::test_core_launcher_produces_finalized_observations
tests/test_scientific_runtime_adapter.py::test_core_launcher_never_executes_site_bootstrap_before_inventory
tests/test_scientific_runtime_adapter.py::test_core_launcher_refuses_extra_installed_file
tests/test_scientific_runtime_adapter.py::test_core_launcher_refuses_wheel_without_public_matrix_api
tests/test_scientific_runtime_adapter.py::test_launcher_refuses_declared_fallback_before_execution
tests/test_scientific_runtime_adapter.py::test_launcher_refuses_missing_explicit_artifact_role
tests/test_scientific_runtime_adapter.py::test_launcher_refuses_obsolete_dag_ml_wheel_role
tests/test_scientific_runtime_adapter.py::test_launcher_refuses_duplicate_expected_artifact_role
tests/test_scientific_runtime_adapter.py::test_launcher_refuses_symlink_scenario
tests/test_scientific_runtime_adapter.py::test_launcher_refuses_oversize_scenario
tests/test_scientific_runtime_adapter.py::test_launcher_refuses_unrecorded_wheel_member
tests/test_scientific_runtime_adapter.py::test_launcher_refuses_duplicate_wheel_member
tests/test_scientific_runtime_adapter.py::test_launcher_refuses_recorded_wheel_symlink
tests/test_scientific_runtime_adapter.py::test_launcher_refuses_false_wheel_record_identity[1-sha256=zN01Foq0dPpXZKUmz7g2ITUeI2gsUHWy4Y1Wvd-WqjA-bytes differ from RECORD]
tests/test_scientific_runtime_adapter.py::test_launcher_refuses_false_wheel_record_identity[2-999999-size differs from RECORD]
tests/test_scientific_runtime_adapter.py::test_launcher_refuses_unsafe_recorded_wheel_path[C:/escape.py]
tests/test_scientific_runtime_adapter.py::test_launcher_refuses_unsafe_recorded_wheel_path[nirs4all_core\\escape.py]
tests/test_scientific_runtime_adapter.py::test_launcher_refuses_symlink_artifact[runtime_python]
tests/test_scientific_runtime_adapter.py::test_launcher_refuses_symlink_artifact[core_wheel]
tests/test_scientific_runtime_adapter.py::test_launcher_refuses_symlink_artifact[pyyaml_wheel]
tests/test_scientific_runtime_adapter.py::test_launcher_refuses_symlink_artifact[archive_v2]
tests/test_scientific_runtime_adapter.py::test_launcher_refuses_symlink_artifact[methods_library]
tests/test_scientific_runtime_adapter.py::test_launcher_refuses_non_regular_artifact
tests/test_scientific_runtime_adapter.py::test_launcher_refuses_component_version_that_differs_from_wheel
tests/test_scientific_runtime_adapter.py::test_core_launcher_forbids_native_facade_masquerading_as_python_oracle
tests/test_scientific_runtime_adapter.py::test_launcher_refuses_installed_bytes_that_differ_from_wheel
tests/test_scientific_runtime_adapter.py::test_launcher_refuses_non_executable_runtime
tests/test_static_engine_parity.py::test_composition_and_operator_graph_match_python
```

R1/R2 final freeze: the four implementation/test files have SHA-256 `eaa3b53c296c02d1efaaa711351382f9d89ceea6606454941ef725e10e58d41d`,
computed over compact ASCII JSON of sorted `(relative_path, file_sha256)` pairs
(excluding additive documentation). Against the sealed 207-file checkpoint,
only the exact five authorized paths differ; all 65 saved preimages remain
intact. All 590/258 original HEAD documentation lines remain in order, and this
batch only appends to the sealed INGESTION.md. Fresh provisioning and existing
transaction/query method bodies remain byte-identical. Schema SQL, frozen
history, frontend/static/chart sources, CI and all unrelated dirty work are
unchanged. No implementation change follows this freeze. The independent WORK
review is still pending; no Opus session, commit or publication was launched.

### Narrow synthetic-fixture successor (same author session)

The parent granted only the existing unit fixtures: fresh owned test-temp venvs
with `EnvBuilder(with_pip=False, clear=True)` or the exact base-Python
`-m venv --without-pip --copies` argv; their synthetic fakewheel data and the pinned
JSON/performance console stubs. No pip/ensurepip, real dependency installation,
owner/private runtime, workspace venv target, scientific/native/WASM fitting,
external network, frontend build or real scientific qualification is authorized.
The previous complete source manifest is retained with SHA-256
`182be9108ef4c95b6789a6bae06b4c34b96a640a7ad45d8582e402541b4c4c21`;
all its source entries matched before these edits. Prior raw failures and proofs
are preserved. No fifth whole suite is run with the old guard.

The two old store assertions will be exercised only against these already
accepted R1/R2 behaviors: foreign/v1/empty/unclaimed headers still refuse before
any SQLite connection or application write, preserving bytes/mtimes/sidecars;
atime is excluded by A2. A forged matching owned header may undergo SQLite
recovery/checkpoint/sidecar changes before fingerprint refusal under A4, while
application writes remain forbidden. Healthy owned-WAL readers can open during
another process's live WAL writer; genuine busy/locked expiry returns
`arena_store_busy`, and corruption refuses without WAL-busy retry. The test file
`tests/test_arena_v2.py` is not granted and will not be edited to reintroduce the
old snapshot-refusal/byte-equality contract or to hide failures.

Current admission is 576 MiB (603,979,776 bytes), with a monitored stop at 560 MiB
(587,202,560 bytes), free floor 2,684,354,560 bytes and all seven thread controls
set to 1. One targeted suite runs at a time; fixture creation additionally checks
its projected interpreter-copy footprint before allocation. The parent must
readmit any larger scratch requirement. Proofs/logs are retained; cleanup is
restricted to the newly created disposable fixture scratch, never source,
existing user data/venvs, prior proofs or stores. No whole-suite shortcut or new
skip is used as acceptance evidence.

Synthetic-fixture successor observed checks (all sequential; no whole suite):

| Profile | Observed result | Peak scratch bytes | Minimum free bytes |
|---|---|---:|---:|
| legacy fixtures | 16 passed, 0 failed, 0 errors, 0 skipped; exit 0 | 324,702,256 | 3,527,487,488 |
| synthetic Core fixtures | 27 passed, 0 failed, 0 errors, 0 skipped; exit 0 | 549,826,641 | 3,319,791,616 |
| synthetic artifact/performance | 27 passed, 0 failed, 0 errors, 0 skipped; exit 0 | 872,448 | 3,865,624,576 |
| R1/R3–R6 and guard negatives | 50 passed, 0 failed, 0 errors, 0 skipped; exit 0 | 11,902,976 | 3,852,095,488 |
| fresh-race batch 1 | 3 passed, 0 failed, 0 errors, 0 skipped; exit 0 | 1,875,968 | 3,861,983,232 |
| fresh-race batch 2 | 3 passed, 0 failed, 0 errors, 0 skipped; exit 0 | 2,940,928 | 3,855,089,664 |
| fresh-race batch 3 | 3 passed, 0 failed, 0 errors, 0 skipped; exit 0 | 2,957,312 | 3,852,275,712 |
| unchanged old store assertions | 0 passed, 2 failed, 0 errors, 0 skipped; exit 1 | 405,504 | 3,854,262,272 |
| preserved Node profile | 45 passed, 0 failed, 0 errors, 0 skipped; exit 0 | 5,693,440 | 3,848,876,032 |
| ruff | Passed; exit 0 | 0 | 3,852,656,640 |
| mypy | Passed; exit 0 | 49,295,360 | 3,803,185,152 |

All 43 formerly denied fixture cases ran: 16 legacy closure/negative cases and
27 synthetic Core closure/negative cases. The legacy tests mock the isolated
execution boundary; the synthetic Core implements fixed Python sums and reads
fake witness bytes, never a native library. The 27 artifact/performance cases
comprise 12 artifact-qualification and 15 performance-contract tests, all using
synthetic declarations. These passes are unit-contract evidence only, not real
owner/scientific qualification or measured native performance.

The first scientific-fixture attempt had 26 passes and one guard refusal on the
intentionally tampered fake installed file. That output remains retained. The
narrow correction admits exactly that pinned negative payload as data so the
closed-inventory child can reject its digest before import. The import guard
still refuses to import that tampered source. No unlisted real package or code
was admitted, and no pip/ensurepip or genuinely required unknown execution arose.

Three additional independent race suites each exercised three fresh roots with
eight actual opener processes: nine roots / 72 processes, all successful without
retrying failed tests or weakening assertions. The bounded production retry is
still limited to WAL-mode BUSY/LOCKED; the existing corruption and foreign-header
refusal probes passed. The source opener is unchanged in this fixture successor.

The two store tests were run unchanged after the accepted behaviors were recorded
above. Both failed exactly at the obsolete assertions: `test_foreign_roots_empty_files_symlinks_and_fingerprint`
line 234 compares directory atime (excluded by A2), and
`test_live_owned_writer_refuses_snapshot_without_modifying_files` line 649 expects
the removed `arena_store_not_quiescent` refusal for a healthy live WAL reader.
Permission to edit `tests/test_arena_v2.py` is still required. Its first function
also needs the later forged-header assertion at line 255 to adopt A4: verify no
application writes rather than require byte/sidecar equality. Unclaimed
foreign/v1/empty headers must keep strict pre-SQLite non-mutation checks.
The replacement live-writer case should verify a successful consistent reader
that cannot see uncommitted data; genuine exhausted busy should use the fixed
busy code, and corruption must not enter the WAL-busy retry loop. The file was
not edited, skipped or turned green.

Static engine parity remains an **unexecuted joint frontend gate**: no v1 site
build or numerical frontend driver ran. No fifth whole suite was run, with either
the old or successor guard, and no whole-CI green status is inferred. The two
browser/publication cases from the prior full run remain separate joint frontend
work. Existing source/proofs/raw failures, coverage artifacts and the full parent
manifest are preserved; no source outside the approved allowlist changed.

Actual admitted argv forms are retained in raw process/fixture-operation JSONL:

```text
/usr/bin/python3.11 -m venv --without-pip --copies <owned-case>/runtime
<owned-case>/runtime/bin/python -I -S -B -c <guard-bootstrap + pinned fixture child>
<repo>/.venv/bin/n4a-gate-json-adapter
<owned-case>/adapter
```

The scientific isolated flags are preserved. The prepended bootstrap installs
network/native/fitting/write guards before the pinned child script. Only the
exact fake `nirs4all_core` / empty `yaml` sources under that case may import;
other owner runtimes stay forbidden. Console and performance child bytes are
pinned to the existing unit sources, including the deliberate divergence/identity
negative variants. Parent one-use launch admissions bind the exact argv and real
case inode; child bootstrap checks the actual interpreter argv and owned case.
The direct `EnvBuilder` form checks its pinned fixture caller and exact no-pip,
copy-only options. Any existing target, symlink escape, workspace target,
unsupported options, pip/ensurepip or other fixture subprocess refuses before
execution. The new negative tests exercise these denials. The inherited fake
Node22 grants and outside-write/regular-file guards remain intact.

The author session is `01a1a60b-501f-7b13-bb26-7e7f8b061ea2` (actual
`CODEX_SESSION_ID` / `CODEX_THREAD_ID`). Per-run source manifests, full commands,
controlled environment, exit/resource records, raw logs/JUnit and actual launch
or refusal records are retained. No independent review was launched, no user data
or existing venv was deleted, and no package install, scientific/native/WASM fit,
external network, frontend build or publication was performed. All test stores
and newly created disposable runtimes were isolated to their owned run scratch.

Synthetic-fixture successor freeze: only `tests/conftest.py`,
`tests/test_arena_v2_work_review.py` and additive `docs/INGESTION.md` differ from
the supplied complete parent manifest. The two implementation/test files have
SHA-256 `0d29426b81c42c3ae69f411250e66c576afd2333d2c36c689b9c6c902c3cb84e` over compact ASCII JSON of sorted
`(relative_path, file_sha256)` pairs, excluding additive documentation. All 590/258
original HEAD documentation lines remain in order. The opener, schema, three
previous v2 conversions, frontend/static/history/chart files, CI and unrelated
dirty work are unchanged. All prior source/proofs/raw failures remain preserved.
No implementation change follows this freeze; no whole suite, independent review,
commit or publication was performed in this successor.


## Authorized two-test recovery successor (2026-11-04)

This narrow successor follows the synthetic-fixture freeze above. The supplied
207-file source manifest, complete `test_arena_v2.py` preimage and both function
AST pins were verified before editing. Only `_components` in `arena_store.py`,
the two explicitly granted test functions and this additive documentation change.
The original `snapshot` helper, all other test functions and the fixture guard
remain unchanged. Original documentation and all earlier failures/proofs remain
preserved. The author session is `01a1a60b-501f-7b13-bb26-7e7f8b061ea2`.

The foreign-root failure was an **access-time** change, not a ctime change. The
existing snapshot tuple contains `st_atime_ns`, not `st_ctime_ns`. A production
probe recorded both timestamps independently, with SQLite connection forbidden:
`_components` used ordinary directory `open`/`listdir`, advancing root atime while
content, inode, mode, size, mtime and ctime remained unchanged. Preflight now
requests `O_NOATIME` and retries without it only on `EPERM`, following the existing
header-read policy. The fixed owner-path probe preserves all measured fields;
the simulated EPERM probe still preserves content and ctime and may advance
atime, as qualified by A2. Neither probe opens SQLite for a foreign root. These
are Linux/Python 3.11 synthetic observations, not cross-platform qualification.

`test_foreign_roots_empty_files_symlinks_and_fingerprint` retains strict snapshot
equality for the nonempty foreign root and empty database file, adds explicit
root/file ctime checks, and forbids SQLite connections for those cases and the
symlink. There is no A4 exemption for foreign roots. For the matching claimed
owned header with a wrong fingerprint, A4 permits SQLite-internal recovery. The
test instead checks unchanged complete logical schema/rows, arrays and exports,
including synthetic populated rows and artifact sentinels. The traced opening
executes only the busy-timeout PRAGMA and metadata SELECT before refusal; no
application write or journal-mode PRAGMA occurs.

`test_live_owned_writer_refuses_snapshot_without_modifying_files` retains its
historical test name but now exercises committed-read isolation. A separate
process holds an uncommitted WAL transaction that changes a committed metadata
value and inserts a new one. A concurrent Arena reader sees the old committed
value and no new row; after rollback the old value remains and the new row is
absent. Arrays and exports retain their names, content/targets, mode, size, mtime
and ctime; owned reads may advance atime under A2. A separate exclusive
rollback-journal writer requires the typed `arena_store_busy` refusal, with a
50 ms test budget. After its rollback the store reopens and the committed state
remains intact. No generic not-quiescent refusal is expected for a readable WAL
store. The opener's registry, recovery, fingerprint, WAL retry and provisioning
logic are unchanged by this successor.

The guarded focused commands were run one suite at a time using Python `-B`,
`-m pytest -p no:cacheprovider -p arena_r1_capture`, an owned per-run `--basetemp`,
`--tb=short` and per-run `--junitxml`. The selectors and final results are:

| Selector after those common pytest arguments | Result |
| --- | --- |
| `tests/test_arena_v2.py::test_foreign_roots_empty_files_symlinks_and_fingerprint tests/test_arena_v2.py::test_live_owned_writer_refuses_snapshot_without_modifying_files` | 2 passed |
| `tests/test_python_legacy_adapter.py` | 16 passed |
| `tests/test_scientific_runtime_adapter.py` | 27 passed |
| `tests/test_artifact_qualification.py tests/test_performance_compare.py` | 27 passed |
| `tests/test_arena_v2_work_review.py` | 50 passed |
| `tests/test_arena_v2_work_review.py::test_r1_row9_eight_process_fresh_race` (two additional separate suites) | 3 passed each |

The 50-case suite includes three fresh-root race cases. Together with the two
repeat suites, nine actual fresh roots each admitted eight concurrent processes
(72 openers), retaining integrity and committed-row assertions. Full
`ruff check src tests --output-format=concise` and `mypy` pass. Ruff first reported
a missing raw prefix on a new regex assertion; that syntax-only correction and
its original failure output are retained. Before edits both granted tests failed;
an interim added owned-artifact atime assertion also failed. Its replacement
retains content and explicit ctime checks and applies A2 only to valid owned
reads. All these raw results are preserved rather than overwritten.

The existing guard was installed before imports and remained unchanged. Only
previously authorized no-pip synthetic venv, fake-wheel, console/performance and
store subprocesses executed; no scientific or native/WASM fit, dependency
installation, external network or real store was used. This is synthetic
metadata/store validation, not scientific qualification. Threads remained one.
The measured scratch peak was 549,826,641 bytes; the smallest observed free space
was 3,232,661,504 bytes. Owned retained evidence plus the scratch peak stays below
the 587,202,560-byte stop trigger and 603,979,776-byte admission. Per-run exact
argv/environment, exit/resource records, source manifests, raw logs/JUnit,
fixture/process transcripts, the precise source diff and before/after function
and helper AST pins are retained with the author evidence.

No new whole suite was run. Earlier blocked whole-suite outputs remain evidence;
there is no whole-CI success claim. Frontend publication and static engine parity
remain unexecuted joint gates. Existing Node 22 is present; this successor does
not rerun or broaden the previously granted fake Node tests. Independent final
work re-review remains required and was not launched by the author. No frontend,
static/history/chart source, real store, other repository, commit or publication
is changed by this successor.


## Focused F1–F3 author corrections after c833 review (2026-11-04)

The actual `opus-arena-final-work.json` review (SHA-256
`92a803854ee2c6c75226e322b6d850187a02666e28380dc3d388d4f440bb53f2`),
its 24-entry probe manifest and the 207-file source checkpoint were verified
before editing. Eleven preimages were verified. The author remains session
`01a1a60b-501f-7b13-bb26-7e7f8b061ea2`. Existing dirty work, prior raw failures,
review proofs, original documentation and frozen history remain preserved.

**Fresh publication (F1).** The second `_components` check may correctly find a
peer's newly published candidate. Provisioning now validates those components
and attempts the existing no-clobber link, rather than refusing that candidate.
The opener still independently checks ownership and fingerprint. Deterministic
interleaves verify that the peer's salt and sentinel survive, while peer v1 and
foreign contents retain their bytes and directory ctime and refuse without
application directories. The reviewer a1 section 1 now reports a successful first
open and retry. Restoring the old condition in memory makes the new owned-peer
regression fail with `arena_store_root_not_empty` (pytest exit 1); the mutant is
killed without changing any on-disk source.

**Write contention (F2).** The public store transaction boundary now maps SQLite
storage errors after transaction and residual cleanup. BEGIN, body and COMMIT
errors, including nested atomic-pack writes, share the opener's path-free
`arena_store_busy` / `arena_store_corrupt` distinction. Integrity and programming
errors keep the ingestion layer's existing metadata-indexing behavior. The
production timeout remains 30,000 ms. Tests configure a 30 ms budget and the
reviewer's relocated a3 probe configures 400 ms; neither changes the production
default. There is no new retry or fabricated success on exhaustion.

The API maps typed busy refusals to HTTP 503 and `{"detail":"arena_store_busy"}`.
The CLI's existing store boundary now covers command-body writes too, prints
`rejected: arena_store_busy` and exits 2. Held-writer tests exercise direct
transactions, single and atomic ingest, export and pipeline uploads, API multipart
cleanup and CLI ingestion. After refusal there are no partial rows, receipts or
blobs; normal retries succeed after release. Real rollback-journal reader locks
also force COMMIT exhaustion after rows and a new blob have been staged: direct
and atomic ingestion roll back the new work while preserving the preexisting
committed blob. Busy/locked result codes remain distinct from corruption, and the
existing corrupt-store and R3–R6 cleanup regressions pass. No changes were needed
in `ingestion/ingest.py` or `ingestion/upload.py`.

**Bare recovery tests and Python floor (F3).** The crashing child now receives a
JSON synthetic record from its parent instead of importing `test_arena_v2` in a
fresh interpreter. Both original row-1 cases pass with no source or tests entry
in PYTHONPATH. The only entry is the unchanged pre-import execution-guard
bootstrap; it supplies no test helpers. Source metadata now requires Python
`>=3.11`, removes the 3.10 classifier, and uses Ruff `py311` and mypy `3.11`.
The CI matrix removes only Python 3.10; its other jobs, dependencies, whole pytest
command and frontend gate are preserved. Python 3.12 remains declared but
unexecuted. Runtime qualification here is Linux Python 3.11.15 / SQLite 3.37.2
only. Installed editable metadata still reports `Requires-Python: >=3.10` and
version `0.1.6`; no install/build or package-version change refreshed it. The source
Python floor and the existing environment metadata must not be conflated.

Focused results and exact command families:

- `.venv/bin/python -B -m pytest -p no:cacheprovider -p arena_r1_capture` with
  owned `--basetemp`, `-vv --tb=short` and JUnit: 41 focused cases passed, including
  both bare-import crash recovery cases, full small ingestion/service test modules
  and the F1/F2 regressions. Another explicit selection of 42 small recovery and
  R3–R6 regression cases passed. No skip or xfail was introduced.
- The reviewer a1 probe was limited to section 1, and a3 was relocated only to the
  owned scratch root; their original scripts and outputs remain unchanged.
  Their new outputs show successful publication, HTTP 503, CLI exit 2 and typed
  atomic busy refusal. A separate in-memory mutation run intentionally exits 1.
- `.venv/bin/python -B -m mypy --cache-dir=/dev/null`: no issues in 60 source files.
- `.venv/bin/ruff check src tests --no-cache`: exit 1, three `UP017` findings exposed
  by the required Python 3.11 target, at `qualification/runner.py:312`,
  `soak_probe.py:351` and `store/arena_store.py:42`. Those timestamp aliases are
  unrelated baseline lines; the first two files are outside this batch's edit
  allowlist. They remain explicit follow-up work, without rule suppression or a
  misleading lint-success claim.

The first focused run retained two failures caused by a new test helper rebinding
its child-script variable; that helper was corrected and all 41 cases then passed.
All command argv, child launches, PIDs, exits, source-before/after hashes and full
raw logs/JUnit are retained with the author evidence. A conservative monitor counts
the entire new scratch directory plus all eleven allowed source-file sizes,
polls every second, and stops at 48 MiB or before the free-disk floor. Its observed
peak was 29,384,259 bytes; free space stayed at or above 3,567,009,792 bytes.
The incremental envelope is 64 MiB. Only newly created disposable test scratch is
removed after evidence capture. No cache, prior proof or user data was deleted.

This is a focused synthetic backend check. No full suite, frozen-history ingest,
large sparse fixture, venv copy, native compiler, installation, external network,
scientific/native/WASM fitting, frontend build, site generation, real dataset or
real store was executed. The three exact joint frontend IDs remain unexecuted:
`tests/test_history.py::test_real_charts_recover_from_saved_demo_filters`,
`tests/test_site_boot.py::test_static_site_recovers_from_startup_failures`, and
`tests/test_static_engine_parity.py::test_composition_and_operator_graph_match_python`.
R7/whole CI and publication remain unqualified. Other platforms, Python 3.12,
Docker and real owner execution are unexecuted. Independent focused Opus recheck
remains pending and is not launched by the author.


## UTC-only lint closure under Resource09

The parent authorized the two additional timestamp owners and the store UTC
reference after the focused F1–F3 freeze. The prior seal and all 207 source hashes
were verified first, together with `utc-preimages.json` SHA-256
`c6608ba97a311bd5bdf9a52e1d257ec98fefb99c650238136cd27fe6747f2a0b`.
Only the three `timezone.utc` references now use the equivalent Python 3.11 `UTC`
alias; the qualification and soak modules import `UTC` instead of `timezone`.
Exact byte-substitution checks establish that no other code changed in those
files. This additive note records closure of the three UP017 findings above;
the original failing Ruff log and all preceding proofs remain preserved.

Full `.venv/bin/ruff check src tests --no-cache` passes. Full
`.venv/bin/python -B -m mypy --cache-dir=/dev/null` passes for 60 source files.
Nine small fixed-clock checks compare the exact timestamp strings for fractional
seconds, leap-day midnight and the epoch across the store, soak formatter and
qualification refusal-report path. `datetime.UTC is datetime.timezone.utc` is
verified; report adapters are explicitly unavailable and adapter execution is
forbidden in that test. No scientific qualification is implied. All nine checks
pass. Nineteen existing focused F1/F2 and crash-recovery import cases also pass,
with no source/test-path addition to the guard-only PYTHONPATH and no test edits.

The final Resource09 runs monitor five known active/retained scratch roots,
including the original reviewer scratch, plus conservatively counting all thirteen
allowed source-file sizes. The combined observed peak is 30,153,373 bytes, below
the 48 MiB trigger and 64 MiB admission; every monitored command remains above the
2,684,354,560-byte free floor. Polling is once per second. Only newly created
disposable test scratch is removed after raw logs, PIDs, exits, JUnit and
source-before/after manifests are retained. Earlier four-root results are retained;
the same tiny checks and both full lint/type gates were repeated with all five
known roots included. No prior cache or proof is deleted.

Only the three UTC source files and this additive documentation differ from the
supplied F123 freeze. The CI, test and frontend sources are unchanged. No broad
backend suite, history/large/venv fixture lane, install, native compiler, network,
scientific fit, real dataset/store, frontend build, site generation or publication
runs in this follow-up. Python 3.12, other platforms, Docker, whole CI and all three
joint frontend IDs listed above remain unexecuted and unqualified. Independent
Opus work recheck remains pending and is not launched by the author.

Store schema 3 binds the first accepted semantic source fragments for provenance,
dataset card and dataset specification reference to the exact accepted export. Field
presence, nulls and client assertions are preserved; equivalent replays do not replace
the first form. These fragments are private project data, distinct from the existing
sanitized receipt, and do not establish origin, licensing, scientific validity or rights.
ArenaRunExport wire schema remains version 2. Existing store schemas 1 and 2 are refused;
there is no automatic migration or fallback.

Store schema 3 is unreleased. Its current fingerprint includes the TEXT seed column; earlier schema 3 fingerprints are refused without migration. Seeds are stored as canonical decimal text. Receipt digests bind normalized provenance and dataset card to cached metadata for partial consistency checks. These unkeyed hashes do not authenticate a database against an operator able to rewrite its facts.

Split and CV rows hold declared facts without a per-run RNG field. Reuse of an adopted ID with conflicting facts is refused, including split parameters; equal owner assignment IDs from different generation conditions can therefore be refused. Arena does not rewrite opaque IDs or assert universal foreign assignment semantics.

New submissions retain a private per-export context card alongside the existing
submitted metadata fragments. Replay preserves the first representation and
checks card values against the presented export; older rows are not backfilled.
A conflicting task unit is refused before staging writes.

After same-export replay, a new export is refused with `metric_unit_conflict`
when its final observations disagree with stored units for the same task and
metric. This applies to all execution statuses and is transactional for packs.
An internally mixed export is refused before replay as `metric_unit_contradiction`.


### Data-free existing-site route (9 November 2026)

The maintainer's existing-site redeployment authorization permits a separate,
explicit `build-site --unavailable-results` page. This narrows the earlier blanket
Pages refusal only for a data-free status artifact. The ordinary result builder
still raises `publication_gate_closed` before store access/output. The existing
manual Pages workflow builds only the status artifact; conversion and history
intake remain separately invoked offline commands.

This profile reads no store, requires no scientific runs, and copies only the
unavailable-results HTML, existing stylesheet and Arena icon, plus optional CNAME
and `.nojekyll`. It ships no SPA, generated runtime, data bundle, run documents,
upload UI or analysis snapshots. Old hash routes therefore have no executable
ranking/import path. Nonempty output is refused to prevent stale data publication.
It is an explicit unavailable public view, not acceptance of zero scientific runs.

The 10,824 frozen records and 32,286 observations retain their original facts,
visibility and provenance. All remain unranked; neither the 5,826 restricted
records nor the 4,998 unreported-visibility records enter this artifact. The
canonical uncompressed JSONL digest remains
`d29af7685161345775d69ac03a1c0f86120a7442bb99cc7921461e67176d4945`.
No missing identity, safety, completion, fold or variant context is supplied.
Arena wire version 2 is independent of an owner's ScientificContextV1 version;
accepting metadata does not establish scientific eligibility in either contract.

Publishing scientific results still needs genuinely eligible public evidence,
exact task/metric/unit and fold/variant context, an owner-reviewed public
projection with its frontend consumer, and Root's execution decision. Private
per-export cards, local APIs, project-token services and hosting acceptance stay
separate. Earlier conversion/recovery proofs and refusals remain historical
records. This section defines the bounded route, not an executed validation or
hosted deployment claim.
