# ArenaRunExport v1 — the ingestion contract

The one weights-free, content-addressed bundle the Arena ingests: one `ArenaRunExport` per `execution`. The engine (dag-ml driving a nirs4all run, with nirs4all-io supplying the dataset) emits it; the Arena ingests it with zero engine-internal knowledge. This page is the reference for the wire shape and how it is validated and keyed.

The authoritative freeze contract is the JSON Schema at `src/nirs4all_benchmarks/contract/schema/arena_run_export.schema.json`. The pydantic models in `src/nirs4all_benchmarks/contract/arena_run_export.py` are the typed, validated Python view. The "why" lives in `DATA_MANAGEMENT.md` §4; do not read this page as a replacement for it.

## Contents

- [Status and versioning](#status-and-versioning)
- [Top-level shape](#top-level-shape)
- [Blocks](#blocks)
  - [producer](#producer)
  - [dataset](#dataset)
  - [task](#task)
  - [dataset_variant](#dataset_variant)
  - [pipeline](#pipeline)
  - [split](#split)
  - [cv](#cv)
  - [rng](#rng)
  - [refit](#refit)
  - [execution](#execution)
  - [leakage_attestation](#leakage_attestation)
  - [scores](#scores)
  - [residuals](#residuals)
  - [provenance](#provenance)
- [The residuals.parquet column contract](#the-residualsparquet-column-contract)
- [Inline residuals](#inline-residuals)
- [Minimal valid manifest](#minimal-valid-manifest)
- [Richer manifest with inline residuals](#richer-manifest-with-inline-residuals)
- [arena_export_hash — the idempotency key](#arena_export_hash--the-idempotency-key)
- [validate_manifest](#validate_manifest)

## Status and versioning

The schema is **frozen at v1**. `arena_export_schema_version` MUST be the integer `1` (the JSON Schema pins it with `"const": 1`). At ingest, an export whose `arena_export_schema_version` is anything other than `ARENA_EXPORT_SCHEMA_VERSION` (currently `1`) is rejected with code `unsupported_export_version` before schema validation runs. Bumping that constant signals an incompatible wire change.

Every block is `extra="allow"` (pydantic) and the JSON Schema does not set `additionalProperties: false`, so a future producer can attach fields without breaking ingestion. The Arena validates only the fields it keys on. Unknown keys ride along in the stored manifest but do not affect identity beyond their contribution to `arena_export_hash`.

Convention used in the field tables below:

- **`<sha256>`** — a lowercase 64-hex SHA-256 string (`^[0-9a-f]{64}$`).
- **Required?** — "yes" means the JSON Schema lists the field in a `required` array; the export is rejected at VERIFY if it is absent. "no" means optional; the typed view supplies the default shown.

## Top-level shape

```
ArenaRunExport
├─ arena_export_schema_version : int = 1     (required, const 1)
├─ arena_export_hash           : <sha256>    (idempotency key; see below)
├─ run_condition_hash          : <sha256>    (informational; the Arena recomputes its own)
├─ producer            : object
├─ dataset             : object              (required)
├─ task                : object              (required)
├─ dataset_variant     : object              (required)
├─ pipeline            : object              (required)
├─ split               : object
├─ cv                  : object
├─ rng                 : object
├─ refit               : object
├─ execution           : object
├─ leakage_attestation : object
├─ scores              : object
├─ residuals           : object
└─ provenance          : object | null
```

JSON Schema `required` at the root: `arena_export_schema_version`, `dataset`, `task`, `dataset_variant`, `pipeline`. Everything else has a typed default, so a valid export can omit `split`, `cv`, `rng`, `refit`, `execution`, `leakage_attestation`, `scores`, `residuals`, `provenance`, `producer`, `run_condition_hash`, and `arena_export_hash`.

| Field | Type | Required? | Meaning |
|---|---|---|---|
| `arena_export_schema_version` | int (`const 1`) | yes | Contract version. Must equal `ARENA_EXPORT_SCHEMA_VERSION`. |
| `arena_export_hash` | `<sha256>` \| null | no | The export's idempotency key. If omitted, the Arena computes it; if present it must be the fixed point (see below). |
| `run_condition_hash` | `<sha256>` \| null | no | Producer's view of `H(dataset_variant, split, cv, rng, pipeline_dag, refit)`. Recorded but the Arena **recomputes** its own from the six component hashes. |
| `producer` | object | no | Capsule + producer library versions. |
| `dataset` | object | yes | Dataset identity + card. |
| `task` | object | yes | Target / task-type identity. |
| `dataset_variant` | object | yes | View/subsample/aggregation of the dataset for the task. |
| `pipeline` | object | yes | Canonical pipeline DAG (graph and/or nodes). |
| `split` | object | no | External train/test split spec + instance. |
| `cv` | object | no | Cross-validation spec + instance. |
| `rng` | object | no | Seeds and determinism flags. |
| `refit` | object | no | Selection + refit policy. |
| `execution` | object | no | One concrete run's outcome and environment. |
| `leakage_attestation` | object | no | OOF / leakage honesty claims. |
| `scores` | object | no | Versioned metric observations. |
| `residuals` | object | no | Sample-keyed residual reference or inline rows. |
| `provenance` | object \| null | no | Optional pass-through to PROV / RO-Crate / OpenLineage. |

## Blocks

### producer

The capsule and the producer-library versions. Recorded on the `executions` row; not part of any identity hash.

| Field | Type | Required? | Default | Meaning |
|---|---|---|---|---|
| `capsule` | string | no | `"python"` | Producing capsule (e.g. `python`, `studio`, `cluster`, `fixture`). |
| `nirs4all_version` | string \| null | no | `null` | nirs4all version that produced the run. |
| `dag_ml_version` | string \| null | no | `null` | dag-ml version. |
| `dag_ml_data_version` | string \| null | no | `null` | dag-ml-data version. |
| `io_version` | string \| null | no | `null` | nirs4all-io version. |

### dataset

Dataset identity from the io `CoordinatorDataPlanEnvelope` + `DatasetSpec`. `dataset_fingerprint` is the canonical dataset identity and the only required field; the Arena **adopts** it (it cannot recompute it without the data). JSON Schema `required`: `dataset_fingerprint`.

| Field | Type | Required? | Default | Meaning |
|---|---|---|---|---|
| `dataset_fingerprint` | `<sha256>` | yes | — | Canonical dataset identity (= io `schema_fingerprint`). Adopted as the dimension key. |
| `schema_fingerprint` | `<sha256>` \| null | no | `null` | io schema fingerprint, recorded for provenance. |
| `relation_fingerprint` | `<sha256>` \| null | no | `null` | io relation fingerprint. |
| `plan_fingerprint` | `<sha256>` \| null | no | `null` | io plan fingerprint. |
| `dataset_card` | object | no | `{}` | Identity card (name, modality, signal type, axis, sources, …). Distilled into `dataset_cards`. |
| `dataset_spec_ref` | any \| null | no | `null` | Embedded `DatasetSpec` or a reference by hash (the envelope is lossy). |
| `visibility` | enum: `public` \| `restricted` \| `private` \| `anonymized` | no | `"public"` | Publication policy for raw X/y. |
| `n_samples` | int (≥ 0) \| null | no | `null` | Sample count. Used in VALIDATE to catch `n_folds > n_samples`. |
| `n_features` | int (≥ 0) \| null | no | `null` | Feature count. |

### task

Target + encoding identity. `task_hash` is required by the schema but the Arena **recomputes** its own `task_hash` over `{dataset_fingerprint, task_type, target_name, target_hash, encoding}` for cross-producer dedup. JSON Schema `required`: `task_hash`.

| Field | Type | Required? | Default | Meaning |
|---|---|---|---|---|
| `task_hash` | `<sha256>` | yes | — | Producer's task identity. Recorded; the Arena recomputes its own. |
| `task_type` | enum: `regression` \| `binary` \| `multiclass` \| `multilabel` | no | `"regression"` | Task family. An unknown value is rejected at VALIDATE (`bad_task_type`). |
| `target_name` | string \| null | no | `null` | Target/property name. |
| `target_unit` | string \| null | no | `null` | Target unit (e.g. `g/100g`). |
| `target_hash` | `<sha256>` \| null | no | `null` | Hash of the target vector, if publishable. |
| `encoding` | object | no | `{}` | Classification target encoding metadata. Feeds `task_hash`. |

### dataset_variant

A view/subsample/aggregation of a dataset for a task. The Arena recomputes `dataset_variant_hash` over `{dataset_fingerprint, task_hash, variant_spec}`. JSON Schema `required`: `dataset_variant_hash`.

| Field | Type | Required? | Default | Meaning |
|---|---|---|---|---|
| `dataset_variant_hash` | `<sha256>` | yes | — | Producer's variant identity. The Arena recomputes its own. |
| `variant_spec` | object | no | `{}` | The variant definition. `size` (default `"all"`) and `aggregation` (default `"none"`) become `size_label` / `aggregation` columns. |
| `sample_manifest_hash` | `<sha256>` \| null | no | `null` | Identity of the exact sample manifest, recorded on the variant row. |

### pipeline

The canonical pipeline DAG — the source of truth. Either `graph` (a `GraphSpec` dict) or `nodes` should be present; if both are absent the Arena falls back to a single opaque node so identity stays stable. The Arena always recomputes `pipeline_dag_hash` as a normalized Merkle graph hash. JSON Schema has **no** required fields in this block, but the block itself is root-required.

| Field | Type | Required? | Default | Meaning |
|---|---|---|---|---|
| `pipeline_dag_hash` | `<sha256>` \| null | no | `null` | If absent, the Arena computes it from `graph` / `nodes` at ingest. Recomputed regardless for dedup. |
| `controller_fingerprint` | `<sha256>` \| null | no | `null` | Operator-version identity from the controller. |
| `engine_graph_fingerprint` | `<sha256>` \| null | no | `null` | dag-ml `graph_fingerprint`, recorded on the `pipeline_dags` row. |
| `nirs4all_identity_hash` | `<sha256>` \| null | no | `null` | nirs4all-side pipeline identity, recorded. |
| `graph_ref` | string \| null | no | `null` | Path to the canonical `graph.json` if not inlined (e.g. `"graph.json"`). |
| `graph` | object \| null | no | `null` | The canonical `GraphSpec` (source of truth). Preferred over `nodes` when both are present. |
| `nodes` | array of [PipelineNode](#pipelinenode) | no | `[]` | Flat node list, used when `graph` is absent. |
| `human_label` | string \| null | no | `null` | Display label (e.g. `"StdScaler→SNV→PLS(k=12)"`). |

#### PipelineNode

JSON Schema `required` per node: `node_id`.

| Field | Type | Required? | Default | Meaning |
|---|---|---|---|---|
| `node_id` | string | yes | — | Node identifier within the graph. |
| `role` | string | no | `"transform"` | Node role / kind (e.g. `transform`, `model`, `merge`, `input`, `stacking`). Drives `main_model` detection. |
| `operator` | string \| null | no | `null` | Operator entrypoint (e.g. `sklearn.cross_decomposition.PLSRegression`). |
| `operator_version` | string \| null | no | `null` | Operator version. |
| `params` | object | no | `{}` | Operator parameters. Each becomes a `parameter_values` row. |
| `branch_path` | array | no | `[]` | Branch coordinates for non-linear graphs. |
| `fit_scope` | string \| null | no | `null` | Where the node fits (e.g. `fold_train_only`). |
| `source_id` | string \| null | no | `null` | Upstream source identifier. |
| `model_family` | string \| null | no | `null` | Model-family tag for model nodes. |

> Note: when `pipeline.graph` is supplied as a `GraphSpec`, nodes inside it use the engine's native node fields (`kind`, `operator`, `params`, `edges`, …). The `PipelineNode` table above describes the flat `pipeline.nodes` fallback list.

### split

External train/test split. Both hashes are optional; if `split_instance_hash` is absent the Arena derives one from `{spec, partition_summary}`. JSON Schema validates this block via the shared `specInstance` definition.

| Field | Type | Required? | Default | Meaning |
|---|---|---|---|---|
| `split_spec_hash` | `<sha256>` \| null | no | `null` | Producer's split-spec hash. The Arena recomputes its own from `{method, params}`. |
| `split_instance_hash` | `<sha256>` \| null | no | `null` | The exact split instance. Adopted if present, else derived. Component of `run_condition_hash`. |
| `method` | string | no | `"none"` | Split method (e.g. `kennard_stone`, `random`, `predefined`, `group`). |
| `params` | object | no | `{}` | Split parameters (e.g. `{"test_size": 0.25}`). |
| `partition_summary` | object | no | `{}` | Per-partition counts/summary, stored on the instance row. |

### cv

Cross-validation. `cv_instance_hash` equals the dag-ml `fold_set_fingerprint` when supplied and is adopted; otherwise derived from `{spec, fold_summary}`.

| Field | Type | Required? | Default | Meaning |
|---|---|---|---|---|
| `cv_spec_hash` | `<sha256>` \| null | no | `null` | Producer's CV-spec hash. The Arena recomputes its own. |
| `cv_instance_hash` | `<sha256>` \| null | no | `null` | The exact fold assignment (= `fold_set_fingerprint`). Component of `run_condition_hash`. |
| `method` | string | no | `"none"` | CV method (e.g. `kfold`, `stratified`, `group`, `nested`). |
| `n_folds` | int \| null | no | `null` | Number of folds. Rejected at VALIDATE if `> n_samples` (`cv_inconsistent`). |
| `n_repeats` | int \| null | no | `null` | Number of repeats. |
| `within_train_only` | bool | no | `true` | Whether CV runs strictly within the training partition. |
| `nested` | bool | no | `false` | Whether CV is nested. |

### rng

Seeds and determinism. `rng_context_hash` is adopted if present, else derived from `{root_seed, derivation, framework_seeds, determinism_flags}`.

| Field | Type | Required? | Default | Meaning |
|---|---|---|---|---|
| `rng_context_hash` | `<sha256>` \| null | no | `null` | RNG-context identity. Component of `run_condition_hash`. |
| `root_seed` | int \| null | no | `null` | Root seed. |
| `derivation` | string \| null | no | `null` | Seed-derivation policy (e.g. `sha256-hash-chain`). |
| `framework_seeds` | object | no | `{}` | Per-framework seeds (e.g. `{"numpy": 0, "torch": 0, "sklearn": 0}`). |
| `determinism_flags` | object | no | `{}` | Determinism switches (e.g. `{"PYTHONHASHSEED": 0, "cudnn_deterministic": true}`). |

### refit

Selection + refit policy. `refit_strategy_hash` is adopted if present, else derived from `{strategy, selection_scope, train_scope, params}`.

| Field | Type | Required? | Default | Meaning |
|---|---|---|---|---|
| `refit_strategy_hash` | `<sha256>` \| null | no | `null` | Refit-strategy identity. Component of `run_condition_hash`. |
| `strategy` | string | no | `"none"` | Refit strategy (e.g. `global_best_params_full_train`, `per_fold`, `stacking`). |
| `selection_scope` | string \| null | no | `null` | Where selection happens (e.g. `oof`). |
| `train_scope` | string \| null | no | `null` | What the final model trains on (e.g. `full_train`). |
| `selected_variant_id` | string \| null | no | `null` | Identifier of the selected variant. |
| `params` | object | no | `{}` | Strategy parameters. |

### execution

One concrete run's outcome and environment. The Arena derives a content-addressed `execution_hash` from `{run_condition_hash, arena_export_hash, execution_id}`.

| Field | Type | Required? | Default | Meaning |
|---|---|---|---|---|
| `execution_id` | string \| null | no | `null` | Producer's run id (recorded, never a join key). |
| `status` | enum: `ok` \| `failed` \| `cancelled` | no | `"ok"` | Run outcome. |
| `time_ms` | number \| null | no | `null` | Wall time in milliseconds. |
| `peak_mem_mb` | number \| null | no | `null` | Peak memory in MB. |
| `os` | string \| null | no | `null` | Operating system. |
| `hardware` | string \| null | no | `null` | Hardware descriptor. |
| `failure_code` | string \| null | no | `null` | Failure code if `status != ok`. |
| `failure_message` | string \| null | no | `null` | Failure message. |

### leakage_attestation

The engine's OOF / leakage honesty claims. Drives quarantine: when `oof_enforced` is false and the policy quarantines on leakage (default for `benchmark_release` collections), the export is **quarantined** (ingested but excluded from published views) with code `leakage_unattested`; otherwise it is ingested and flagged with a warning.

| Field | Type | Required? | Default | Meaning |
|---|---|---|---|---|
| `oof_enforced` | bool | no | `false` | Whether out-of-fold prediction was enforced. The gate for quarantine. |
| `group_leakage_checked` | bool | no | `false` | Whether group-leakage was checked. |
| `nested_cv_safe` | bool | no | `true` | Whether nested CV was leakage-safe. |
| `unsafe_flags` | array of string | no | `[]` | Engine-reported unsafe conditions. Non-empty adds an `unsafe_flags` warning and is stored on `executions`. |

### scores

Versioned metric facts. The Arena derives a `ScoreComputationSpec` (and `score_computation_hash`) from `{score_version, metric_implementation, score_level, aggregation_policy}`, where `score_level` is `cv` when any observation has `scope == "cv"` (or there are no observations), else the lexicographically first scope present.

| Field | Type | Required? | Default | Meaning |
|---|---|---|---|---|
| `score_computation_hash` | `<sha256>` \| null | no | `null` | Producer's score-spec hash. The Arena recomputes its own. |
| `score_version` | string | no | `"1.0"` | Score-spec version. |
| `metric_implementation` | string \| null | no | `null` (→ `nirs4all_benchmarks.scoring.metrics/1`) | Metric implementation identifier. |
| `aggregation_policy` | string \| null | no | `null` (→ `macro_mean`) | Cross-fold / cross-target aggregation policy. |
| `observations` | array of [ScoreObservation](#scoreobservation) | no | `[]` | Long-format metric rows. Empty triggers a `no_scores` warning; if residuals exist, scores are recomputed from them. |

#### ScoreObservation

JSON Schema `required` per observation: `metric_name`.

| Field | Type | Required? | Default | Meaning |
|---|---|---|---|---|
| `metric_name` | string | yes | — | Metric name (e.g. `rmse`, `r2`). |
| `metric_value` | number \| null | yes (nullable) | — | Metric value. May be `null`. |
| `direction` | enum: `min` \| `max` | no | `"min"` | Whether lower or higher is better. |
| `scope` | enum: `fold` \| `cv` \| `refit` \| `test` \| `view` | no | `"cv"` | Aggregation scope of the value. |
| `fold_id` | string \| null | no | `null` | Fold the value belongs to (when `scope == fold`). |
| `partition` | enum: `train` \| `validation` \| `test` \| `final` | no | `"validation"` | Data partition scored. |
| `aggregation_level` | string | no | `"sample"` | Unit level (`sample` \| `target` \| `group`). |
| `metric_unit` | string \| null | no | `null` | Unit of the metric value. |
| `n_samples` | int \| null | no | `null` | Samples behind the value. |
| `coverage` | number \| null | no | `null` | Fraction of samples covered. |

### residuals

A reference to (or inline carrier of) the sample-keyed residual arrays. See [the column contract](#the-residualsparquet-column-contract) and [inline residuals](#inline-residuals).

| Field | Type | Required? | Default | Meaning |
|---|---|---|---|---|
| `ref` | string \| null | no | `null` | Path to `residuals.parquet`, relative to the manifest's directory (used when ingesting from a file path). |
| `key` | enum: `sample_id` \| `positional` | no | `"sample_id"` | Residual keying. `positional` is accepted but flagged degraded (`degraded_residual_keying`) and excluded from cross-run sample comparison. |
| `pseudonymized` | bool | no | `false` | Whether the producer already pseudonymized ids. The Arena pseudonymizes at ingest regardless. |
| `publishable` | object (string → bool) | no | `{}` | Per-column publication policy, e.g. `{"y_true": true, "y_pred": true, "residual": true}`. Columns set false are dropped at ingest. |
| `inline` | array of object \| null | no | `null` | Inline residual rows (fixtures / small runs / tests). Takes priority over `ref`. |

### provenance

Optional pass-through of standards-conformant provenance documents. The whole block may be `null`. No field is required; all paths are optional.

| Field | Type | Required? | Default | Meaning |
|---|---|---|---|---|
| `prov_jsonld` | string \| null | no | `null` | Path to a W3C PROV-JSON-LD lineage document. |
| `ro_crate` | string \| null | no | `null` | Path to an RO-Crate metadata file. |
| `openlineage` | string \| null | no | `null` | Path to an OpenLineage document. |

## The residuals.parquet column contract

Residuals are stored sample-keyed (DATA_MANAGEMENT.md §3): the row key is the stable `sample_id`, never a positional index. Whether residuals arrive as a `residuals.parquet` sidecar (via `residuals.ref`) or inline (via `residuals.inline`), they are normalized to the exact column set `RESIDUALS_PARQUET_COLUMNS` (in `contract/schema/__init__.py`). The `ResidualStore` writes one Zstd-compressed Parquet file per `residual_set`, fills missing columns with `null`, and drops unknown keys.

| Column | Arrow type | Nullable | Meaning |
|---|---|---|---|
| `sample_id` | `utf8` | no | Stable per-sample key (io `SampleId`). Pseudonymized at ingest. |
| `group_id` | `utf8` | yes | Group key for grouped CV. Pseudonymized at ingest. |
| `origin_sample_id` | `utf8` | yes | Source sample id when a row is derived/augmented. Pseudonymized at ingest. |
| `scope` | `utf8` | no | Prediction scope (`fold` \| `cv` \| `refit` \| `test` \| `view`). Defaults to `cv` if absent. |
| `fold_id` | `utf8` | yes | Fold label for fold/cv-scoped rows. |
| `partition` | `utf8` | no | Data partition (`train` \| `validation` \| `test` \| `final`). Defaults to `validation` if absent. |
| `y_true` | `f64` | yes | Ground-truth value (subject to `publishable.y_true`). |
| `y_pred` | `f64` | yes | Predicted value (subject to `publishable.y_pred`). |
| `y_proba` | `list<f64>` | yes | Class probabilities for classification (subject to `publishable.y_proba`). |
| `residual` | `f64` | yes | Residual. For regression, derived as `y_true - y_pred` if absent and both are publishable. |
| `weight` | `f64` | yes | Sample weight. |

Ingest behavior worth knowing:

- **Publication policy.** `publishable.{y_true,y_pred,y_proba}` each default to `true`. A column set to false is nulled out and recorded in the clean report's dropped fields.
- **Residual derivation.** For `task_type == "regression"`, a missing `residual` is filled with `y_true - y_pred` when both are present and publishable.
- **Row cap.** If the residual rows exceed `IngestionPolicy.residual_row_cap` (default `1_000_000`), residuals are dropped and the run is stored scores-only (`residuals_truncated` in the clean report).
- **Scores from residuals.** If `scores.observations` is empty (or `IngestionPolicy.recompute_scores` is set) and residuals are present, metric observations are recomputed from the residual rows.
- **Idempotency.** The Parquet file is named by the `residual_set` content hash, so re-ingesting identical residuals writes byte-identical rows to the same path.

## Inline residuals

For fixtures, small runs, and tests, residual rows can be embedded directly in the manifest under `residuals.inline` instead of shipping a separate Parquet file. When `inline` is non-empty it takes priority over `ref`. Each inline row is a plain object whose keys are a subset of the [residuals.parquet columns](#the-residualsparquet-column-contract); missing columns are filled with `null` and unknown keys are dropped during normalization.

```json
{
  "residuals": {
    "key": "sample_id",
    "pseudonymized": false,
    "publishable": { "y_true": true, "y_pred": true, "residual": true },
    "inline": [
      { "sample_id": "s0001", "scope": "cv", "fold_id": "fold0", "partition": "validation",
        "y_true": 12.4, "y_pred": 12.1, "residual": 0.3, "weight": 1.0 },
      { "sample_id": "s0002", "scope": "cv", "fold_id": "fold1", "partition": "validation",
        "y_true": 11.8, "y_pred": 12.3, "residual": -0.5, "weight": 1.0 }
    ]
  }
}
```

## Minimal valid manifest

The smallest export the schema accepts: the four required root blocks, each with only its required field. It carries no scores and no residuals, so it ingests with a `no_scores` warning (and, with default user-run policy, a `leakage_unattested` warning because `oof_enforced` defaults to false).

```json
{
  "arena_export_schema_version": 1,
  "dataset": {
    "dataset_fingerprint": "0000000000000000000000000000000000000000000000000000000000000001"
  },
  "task": {
    "task_hash": "0000000000000000000000000000000000000000000000000000000000000002"
  },
  "dataset_variant": {
    "dataset_variant_hash": "0000000000000000000000000000000000000000000000000000000000000003"
  },
  "pipeline": {
    "nodes": [
      { "node_id": "n0", "role": "model", "operator": "sklearn.cross_decomposition.PLSRegression",
        "params": { "n_components": 10 } }
    ]
  }
}
```

Ingest it from Python:

```python
from nirs4all_benchmarks.contract import validate_manifest
from nirs4all_benchmarks.ingestion import IngestionPolicy, ingest_export
from nirs4all_benchmarks.store import ArenaStore

manifest = { ... }  # the JSON above
assert validate_manifest(manifest) == []   # schema-valid

with ArenaStore("arena-store") as store:
    result = ingest_export(store, manifest, policy=IngestionPolicy(collection_id="demo"))
    print(result.status, result.arena_export_hash)
```

## Richer manifest with inline residuals

A complete regression export with producer versions, a linear pipeline, split/cv/rng/refit, a leakage attestation, score observations, and inline residuals. This mirrors what the fixture generator emits (`fixtures/generate.py`). `arena_export_hash` is the canonical hash of the manifest with that field removed (see below).

```json
{
  "arena_export_schema_version": 1,
  "producer": {
    "capsule": "python",
    "nirs4all_version": "0.10.0",
    "dag_ml_version": "0.1.0-alpha"
  },
  "dataset": {
    "dataset_fingerprint": "aa0000000000000000000000000000000000000000000000000000000000corn",
    "dataset_card": {
      "name": "mock_corn", "modality": "spectroscopy", "signal_type": "absorbance",
      "axis": { "unit": "nm", "n": 700, "range": [1100.0, 2500.0] }
    },
    "visibility": "public",
    "n_samples": 120,
    "n_features": 700
  },
  "task": {
    "task_hash": "bb00000000000000000000000000000000000000000000000000000000000task",
    "task_type": "regression",
    "target_name": "reference",
    "target_unit": "g/100g"
  },
  "dataset_variant": {
    "dataset_variant_hash": "cc0000000000000000000000000000000000000000000000000000000000vrnt",
    "variant_spec": { "size": "all", "aggregation": "none" }
  },
  "pipeline": {
    "human_label": "StdScaler→SNV→PLS(k=12)",
    "graph": {
      "nodes": [
        { "node_id": "n0", "kind": "transform", "operator": "sklearn.preprocessing.StandardScaler" },
        { "node_id": "n1", "kind": "transform", "operator": "nirs4all.transform.SNV" },
        { "node_id": "n2", "kind": "model", "operator": "sklearn.cross_decomposition.PLSRegression",
          "params": { "n_components": 12 } }
      ],
      "edges": [
        { "src": "n0", "dst": "n1" },
        { "src": "n1", "dst": "n2" }
      ]
    }
  },
  "split": { "method": "kennard_stone", "params": { "test_size": 0.25 } },
  "cv": {
    "method": "kfold",
    "n_folds": 5,
    "within_train_only": true,
    "cv_instance_hash": "dd0000000000000000000000000000000000000000000000000000000000fold"
  },
  "rng": {
    "root_seed": 1001,
    "derivation": "sha256-hash-chain",
    "framework_seeds": { "numpy": 1001, "sklearn": 1001 },
    "determinism_flags": { "PYTHONHASHSEED": 0 }
  },
  "refit": {
    "strategy": "global_best_params_full_train",
    "selection_scope": "oof",
    "train_scope": "full_train"
  },
  "execution": {
    "execution_id": "exec-pls12-mock_corn",
    "status": "ok",
    "time_ms": 1840.0,
    "peak_mem_mb": 410.0,
    "os": "linux",
    "hardware": "cpu-x86_64"
  },
  "leakage_attestation": {
    "oof_enforced": true,
    "group_leakage_checked": true,
    "nested_cv_safe": true,
    "unsafe_flags": []
  },
  "scores": {
    "score_version": "1.0",
    "observations": [
      { "metric_name": "rmse", "metric_value": 1.05, "direction": "min",
        "scope": "cv", "partition": "validation", "aggregation_level": "sample", "n_samples": 120 },
      { "metric_name": "r2", "metric_value": 0.91, "direction": "max",
        "scope": "cv", "partition": "validation", "aggregation_level": "sample", "n_samples": 120 }
    ]
  },
  "residuals": {
    "key": "sample_id",
    "pseudonymized": false,
    "publishable": { "y_true": true, "y_pred": true, "residual": true },
    "inline": [
      { "sample_id": "mock_corn_0000", "scope": "cv", "fold_id": "fold0", "partition": "validation",
        "y_true": 12.4, "y_pred": 12.1, "residual": 0.3, "weight": 1.0 },
      { "sample_id": "mock_corn_0001", "scope": "cv", "fold_id": "fold1", "partition": "validation",
        "y_true": 11.8, "y_pred": 12.3, "residual": -0.5, "weight": 1.0 }
    ]
  }
}
```

## arena_export_hash — the idempotency key

`arena_export_hash` is the SHA-256 of the canonical manifest and is the idempotency key of an ingested export. Canonical JSON here means `json.dumps(..., sort_keys=True, separators=(",", ":"), ensure_ascii=False, allow_nan=False)` — so two semantically equal manifests serialize byte-identically. `NaN` / `Infinity` are rejected.

The hash excludes the `arena_export_hash` field itself, making it a fixed point: a manifest that carries the field MUST satisfy `manifest["arena_export_hash"] == export_hash(manifest)`.

```python
from nirs4all_benchmarks.identity.hashing import export_hash

manifest = { ... }              # without arena_export_hash
manifest["arena_export_hash"] = export_hash(manifest)
assert manifest["arena_export_hash"] == export_hash(manifest)   # fixed point
```

How it drives ingestion:

- **Dedup key.** The full idempotency key is `(arena_export_hash, target_collection, arena_schema_version)`. If a prior batch with that key is already `committed` or `quarantined`, `ingest_export` returns `status="already_ingested"` and does not write again.
- **If omitted.** Producers may leave `arena_export_hash` out; the Arena computes it from the canonical manifest at VERIFY. Either way, dedup is identical.
- **Execution identity.** The derived `execution_hash` folds in `arena_export_hash`, so two genuinely distinct runs of the same `run_condition` get distinct executions, while a re-ingest of the same export collapses.

> Distinguish this from `run_condition_hash`, the *natural* identity of a condition: `H(dataset_variant_hash, split_instance_hash, cv_instance_hash, rng_context_hash, pipeline_dag_hash, refit_strategy_hash)` over the six component hashes in fixed labelled order (`identity/hashing.py::compose_run_condition_hash`). The Arena recomputes it from its own resolved components rather than trusting the manifest's `run_condition_hash`.

## validate_manifest

`validate_manifest(manifest) -> list[str]` (in `contract/schema/__init__.py`) checks a manifest dict against the frozen JSON Schema using a `Draft202012Validator`. It returns a list of human-readable error strings; an **empty list means valid**.

```python
from nirs4all_benchmarks.contract import validate_manifest

errors = validate_manifest(manifest)
if errors:
    for e in errors:
        print(e)        # e.g. "dataset: 'dataset_fingerprint' is a required property"
else:
    print("schema-valid")
```

Each error string is formatted as `"<json/pointer/path>: <message>"`, with `<root>` used for top-level errors; errors are sorted by their path for stable output.

`validate_manifest` is the **VERIFY** gate inside `ingest_export`: a non-empty list is a hard rejection (`status="rejected"`, code `schema_invalid`), before any identity resolution. It runs after the `arena_export_schema_version` check and is independent of the later semantic VALIDATE step (`ingestion/validate.py`), which is where leakage quarantine, `task_type`, `n_folds`/`n_samples`, and degraded residual keying are decided. Schema-shape errors are caught here; semantic and policy issues are caught there.
