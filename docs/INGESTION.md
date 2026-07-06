# Ingestion

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

- **Adopted** (engine-guaranteed, the Arena cannot recompute them without the data): `dataset_fingerprint`, the io `schema`/`relation`/`plan` fingerprints, `cv_instance_hash` (= dag-ml `fold_set_fingerprint`), `split_instance_hash`, `engine_graph_fingerprint`, `target_hash`. Where a producer omits an instance hash, it is recomputed from the spec hash + summary as a fallback.
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
