# nirs4all, dag-ml & nirs4all-io persistence formats — and how the Arena ingests them

| Field | Value |
|---|---|
| Status | Draft for review |
| Date | 2026-06-14 |
| Audience | Arena developers, nirs4all maintainers, dag-ml maintainers |
| Scope | The save / serialization / "binarization" mechanisms of `nirs4all` (0.10.0), `dag-ml` (0.1.0-alpha), and `nirs4all-io` (Rust phase-2) — their compatibility, the dataset-identity bridge, and how `nirs4all-benchmarks` should consume them for pipeline×dataset meta-analysis |
| Out of scope | Arena SQLite/Parquet schema details (owned by [`DESIGN.md`](DESIGN.md)), compute scheduling, dataviz |
| Companion | [`DESIGN.md`](DESIGN.md) is the conceptual model + Arena store schema. **This doc is the technical layer beneath it**: it answers the format/compatibility questions left open in `DESIGN.md` §13 ("What exact format of `PipelineDAGSpec` will be shared with dag-ml?"). |

---

## 0. What this document is

`DESIGN.md` already fixes *what* the Arena stores (the dimension/fact model, the `RunCondition` tuple, the "no artifacts" rule, SQLite+Parquet). It deliberately leaves *how we get those rows out of a producer* under-specified, and it flags the dag-ml interop format as an open question.

This document closes that gap. It is the result of reading three code bases directly:

- **nirs4all** — `nirs4all/pipeline/storage/*`, `nirs4all/pipeline/bundle/*`, `nirs4all/pipeline/config/component_serialization.py`, the regression contracts in `tests/regression/`.
- **dag-ml** — `crates/dag-ml-core/src/*` (`bundle.rs`, `provenance.rs`, `graph.rs`, `plan.rs`, `dsl.rs`, `fold.rs`, `oof.rs`, `rng.rs`, `runtime.rs`, …), `docs/COORDINATOR_SPEC.md`, `docs/STATUS.md`, and the `dag-ml-py` / `dag-ml-capi` binding surfaces.
- **nirs4all-io** — the Rust workspace `crates/*` (`nirs4all-io-core`, `nirs4all-io-dagml`, `nirs4all-io-capi`) + the Python MVP/oracle `src/nirs4all_io/*`, `docs/DATASET_CONFIGURATIONS.md`, `docs/RUST_REWRITE_ROADMAP.md`, `docs/PHASE2_GATE.md`. (`dag-ml-data`, io's emit target, is read where it defines `CoordinatorDataPlanEnvelope`.)

All path:line references are in the [Appendix](#appendix-source-reference-index). Both nirs4all's `.n4a` + workspace schemas and dag-ml's bundle/fingerprint contracts are **frozen/versioned contracts** (nirs4all 0.9.x stability note; dag-ml `schema_version` + migration policy), so the facts below are durable enough to design against.

---

## 1. Executive summary (read this first)

**The two run-persistence mechanisms (nirs4all, dag-ml) are conceptually convergent but mechanically incompatible — *not* interchangeable on disk, and they should not be made so. A third library, `nirs4all-io`, supplies the missing piece: stable dataset + sample identity, emitted as dag-ml's data-plane contract. The Arena sits *above* all three as a producer-agnostic sink.**

| Question | Answer |
|---|---|
| Do they use the same on-disk format? | **No.** nirs4all = `store.sqlite` + `arrays/*.parquet` + content-addressed `artifacts/*.joblib` + a `.n4a` ZIP bundle. dag-ml = a single serde **JSON** `ExecutionBundle` (a manifest of *references and fingerprints*) + externally-stored, referenced artifact payloads. |
| Do they model a run the same way? | **Semantically, yes.** Both factor a run into *(canonical pipeline definition + fingerprint) × (dataset identity) × (fold/CV structure) × (predictions/scores) × (referenced fitted artifacts)*. That is exactly the Arena's `RunCondition` tuple. |
| Is there a ready-made `pipeline_dag_hash`? | **dag-ml: yes** — `graph_fingerprint` (SHA-256 over a canonical `GraphSpec` DAG). **nirs4all: only a linear-list hash** (`get_hash`, SHA-256 over the sorted-JSON step list) — stable, but not topology-aware and not equal to dag-ml's. |
| Where does stable dataset + sample identity come from? | **`nirs4all-io`** (§4). Its `to_dag_ml_data` emit hashes a `DatasetSchema` + `SampleRelationTable` into a `CoordinatorDataPlanEnvelope` — three SHA-256 fingerprints (`schema`/`plan`/`relation`) + a `DataPlan` + a *derived* relation set with **stable `SampleId`/`ObservationId`/`GroupId`** (it does **not** carry the original schema/relation table, and drops `repetition_id`). This is the `DatasetCard`/`DatasetFingerprint` source. It keys **dag-ml-native** residuals directly; mapping it onto nirs4all's **positional** `sample_indices` needs an io ordinal→`SampleId` sidecar that does not exist yet (§4.4). |
| Is there a bridge between the producers today? | **Two, both one-directional.** (a) Pipeline: dag-ml's `lower_nirs4all_compat_pipeline_dsl` (`dsl.rs:593`, reachable from Python) parses nirs4all-style pipeline JSON into a `GraphSpec`. (b) Data: `nirs4all-io` already emits dag-ml-data's `CoordinatorDataPlanEnvelope` (EPIC 10, GREEN). nirs4all itself does **not** import or call dag-ml (one TODO docstring aside). |
| Who is the live producer today? | **nirs4all only.** dag-ml is `0.1.0-alpha`, not wired into nirs4all, and its Python surface is validate/compile/fingerprint-only (execution runs through the Rust core / CLI / C-ABI). |

**Headline recommendation:** Make the Arena's canonical pipeline identity equal to **dag-ml's `graph_fingerprint`** — computed by lowering nirs4all's serialized pipeline through dag-ml's compat DSL **and then building an `ExecutionPlan`** (the fingerprint is produced at plan build, *not* by the compile step alone — see §5.3). Two prerequisites make this sound rather than aspirational: (a) an explicit fingerprint API (a `graph_fingerprint_json` helper in `dag-ml-py`, or `build_execution_plan_json` + extract), because the compile binding returns only the `GraphSpec` JSON; and (b) an **Arena-owned normalization of graph/node ids and ordering**, because `graph_fingerprint` hashes the *whole* `GraphSpec` (graph `id` included) — so it is equal across producers only under *the same compiler + the same normalized input*. Use nirs4all's `get_hash` as the v0 fallback and keep it as a recorded secondary id. With those pieces the Arena gets one topology-aware, dedup-able `pipeline_dag_hash` — answering `DESIGN.md` §13 Q1. **Dataset + sample identity** comes from `nirs4all-io`'s `CoordinatorDataPlanEnvelope` (§4) — three SHA-256 fingerprints + stable sample ids. This keys dag-ml-native residuals directly; nirs4all runs store *positional* `sample_indices`, so keying their residuals to io identity needs an io ordinal→`SampleId` sidecar still to be built (§4.4). Everything else (scores, residuals, fold structure) the Arena maps from whichever producer it ingests.

---

## 2. nirs4all — how saving / binarization works

nirs4all has **three distinct persistence layers**. Conflating them is the most common mistake; the Arena cares about different parts of each.

```
workspace/
  store.sqlite            # (Layer 1) durable metadata: runs, pipelines, chains, predictions, artifacts, logs, projects
  arrays/<dataset>.parquet# (Layer 1) prediction arrays: y_true/y_pred/y_proba/sample_indices/weights
  artifacts/<hash>.joblib # (Layer 1) content-addressed fitted binaries (ref-counted)
  runs/<dataset>/NNNN/…    # run manifests (YAML)
  explanations/<uid>/…     # SHAP plots (NOT in SQLite)
  exports/                 # destination for ad-hoc exports
                           # (Layer 2) in-RAM V3 ArtifactRegistry + ExecutionTrace bridge into Layer 1
model.n4a                  # (Layer 3) portable ZIP bundle: recipe + fitted weights (predict-capable)
```

### 2.1 Layer 1 — the durable workspace store (SQLite + Parquet + artifacts)

This is the layer the Arena ingests from. It is a **frozen contract**: `SCHEMA_VERSION = 2`, stamped in `PRAGMA user_version`, with a forward-incompatibility guard that refuses to open a DB stamped newer than the library, and the table layout is asserted by exact-equality in `tests/regression/test_storage_schema_contract.py`.

**SQLite — 7 tables** (`store_schema.py`):

| Table | PK | Key columns (Arena-relevant) |
|---|---|---|
| `runs` | `run_id` (uuid4) | `name, config, datasets, status, created_at, completed_at, summary, project_id` |
| `pipelines` | `pipeline_id` (uuid4) | `run_id→runs`, `name` (contains the identity hash), `expanded_config`, `original_template`, `generator_choices`, `dataset_name`, `dataset_hash`, `best_val`, `best_test`, `metric` |
| `chains` | `chain_id` (uuid4) | `pipeline_id→pipelines`, `steps` (JSON), `model_step_idx`, `model_class`, `preprocessings`, `fold_strategy`, `fold_artifacts` (JSON `{fold_N: artifact_id}`), `shared_artifacts`, `branch_path`, `source_index`, CV/final/`_agg` scores, `relation_replay_*` |
| `predictions` | `prediction_id` (uuid4) | `pipeline_id`, `chain_id`, `dataset_name`, `model_name`, `fold_id`, `partition`, `val_score`, `test_score`, `train_score`, `metric`, `task_type`, `n_samples`, `n_features`, `scores` (multi-metric JSON), `best_params`, `branch_id`, plus 12 relation/lineage columns (`prediction_scope`, `prediction_level`, `evaluation_scope`, `physical_sample_id`, `origin_sample_id`, `derived_unit_id`, `unit_level`, `unit_id`, `row_id`, `sample_influence_weight`, …) |
| `artifacts` | `artifact_id` (uuid4) | `artifact_path`, `content_hash` (SHA-256), `operator_class`, `format` ('joblib'), `size_bytes`, `ref_count`, `chain_path_hash`, `input_data_hash`, `dataset_hash` — **no FK** (shared by reference, ref-counted) |
| `logs` | `log_id` | `pipeline_id`, `step_idx`, `operator_class`, `event`, `duration_ms`, `level` |
| `projects` | `project_id` | `name`, `description`, `color` |

The one UNIQUE business index is the prediction **natural key**: `(pipeline_id, chain_id, fold_id, partition, model_name, branch_id)`. A read-optimized `v_chain_summary` view joins chains⋈pipelines and exposes the score columns + `run_id`.

**Parquet — the prediction arrays** (`array_store.py`, one file per dataset, Zstd level 3, atomic temp+rename, tombstone deletes). Shared Arrow schema:

```python
_PARQUET_SCHEMA = pa.schema([
    ("prediction_id", pa.utf8()),     # join key back to the SQLite predictions row
    ("dataset_name", pa.utf8()), ("model_name", pa.utf8()),
    ("fold_id", pa.utf8()), ("partition", pa.utf8()),
    ("metric", pa.utf8()), ("val_score", pa.float64()), ("task_type", pa.utf8()),
    ("y_true",  pa.list_(pa.float64())),
    ("y_pred",  pa.list_(pa.float64())),
    ("y_proba", pa.list_(pa.float64())), ("y_proba_shape", pa.list_(pa.int32())),
    ("sample_indices", pa.list_(pa.int32())),   # POSITIONAL row indices, NOT stable sample ids
    ("weights", pa.list_(pa.float64())),
    ("sample_metadata", pa.utf8()),              # JSON string
])
```

Important for the Arena: the parquet is **self-describing** (it denormalizes `model_name/fold_id/partition/metric/val_score`), so it is readable with Polars/pyarrow *without* the SQLite file. **Residuals are not stored** — for regression they are `y_true − y_pred`, derivable (not a valid notion for classification — see §6.2). Note `sample_indices` are **positional** indices into the run's dataset ordering, **not globally stable sample identifiers**: stable identity for cross-run joins/pseudonymization must come from the dataset manifest, the `sample_metadata` JSON, or the relation columns (`physical_sample_id`/`origin_sample_id`/`unit_id`) when present.

**Artifacts** are content-addressed (`artifacts/<hash[:2]>/<full_sha256>.joblib`), deduplicated by **ref-count** (a second chain that fits an identical object reuses the row and increments `ref_count`; GC when it hits 0). Three cache-handle columns — `chain_path_hash` (identity of the preprocessing chain up to that step), `input_data_hash` (hash of the data fed in), `dataset_hash` (source dataset content hash) — power **cross-run fit caching** ("has this exact chain on this exact input already been fitted?") and per-dataset cache invalidation.

### 2.2 Layer 2 — the V3 ArtifactRegistry + ExecutionTrace

During a run, an in-RAM registry persists each fitted operator to `artifacts/` in a **framework-aware** way (sklearn→joblib, Keras→`.keras`, torch→`state_dict`, XGBoost→JSON, …, generic→cloudpickle), records an `ArtifactRecord` per-run manifest, and is then bridged into the SQLite `artifacts` table via `register_existing_artifact`. The key Arena-relevant by-product is the **`ExecutionTrace`** (the bundle's `trace.json`): an ordered list of `ExecutionStep`s with `operator_type/class`, `branch_path`, `input_chain_path`, `output_chain_paths`, fold artifacts. **This trace, plus the per-artifact `OperatorChain`, is the closest thing nirs4all serializes to a DAG.**

### 2.3 Layer 3 — the `.n4a` bundle (the "binarization")

`.n4a` is a **ZIP archive** (`BUNDLE_FORMAT_VERSION = "1.0"`, frozen by `test_bundle_contract.py`). Two producers emit slightly different entry sets:

```
model.n4a (ZIP)
├── manifest.json                 # always — bundle_format_version, nirs4all_version, created_at,
│                                 #   pipeline_uid, source_type, model_step_index, fold_strategy,
│                                 #   preprocessing_chain (+ optional original_manifest, trace_id,
│                                 #   partitioner_routing, relation_replay_manifest reference)
├── pipeline.json | chain.json    # the recipe (steps + model_step_idx; chain.json adds fold/shared artifacts)
├── trace.json                    # optional — the ExecutionTrace (closest-to-DAG structure)
├── fold_weights.json             # optional — ensemble averaging weights
├── relation_replay_manifest.json # optional — multi-source materialization plan
└── artifacts/
    ├── step_0_MinMaxScaler.joblib
    └── step_4_fold0_PLSRegression.joblib   # ALWAYS joblib at export (even for DL models)
```

There is also a `.n4a.py` variant: a standalone Python script with artifacts base64+joblib-embedded that runs `predict()` with only numpy+joblib.

**The recipe is separated from the weights**: `pipeline.json`/`chain.json` + `manifest.preprocessing_chain` describe the operators as JSON (import-path canonicalized — see §2.4); the fitted weights are the joblib files. Loading reconstructs a `BundleArtifactProvider` (lazy joblib) and `predict(X)` replays the trace (preprocess → model, averaging CV folds or using the refit model).

**Critical limitations for the Arena:**
- A `.n4a` bundle is fundamentally a **predict-capable package**. It **always tries to embed fitted joblib artifacts** (gated only by whether the run had `save_artifacts=True`); there is **no "without weights" export profile**.
- A bundle carries **no results/metrics/residuals at all** — scores and arrays live only in the workspace store. There is **no "results-rich, weights-free" export profile**.
- Two artifact paths differ: the **resolver** export re-serializes via joblib and `BundleLoader` loads via `joblib.load`; the **store** `export_chain` copies the artifact files **verbatim**, so native `.keras`/`.pt`/`.json` artifacts from the framework-aware registry can end up in a bundle (and would *not* reload through the joblib-only loader — a known bundle limitation). The Arena strips weights regardless, so this is only a producer round-trip caveat.

### 2.4 Pipeline representation & the canonical hash

A nirs4all pipeline is a **flat Python `list` of steps**, not a graph. Branching/merge/stacking are keyword dict steps (`{"branch": …}`, `{"merge": "predictions"}`, `{"model": …}`) dispatched at runtime by controllers; there is no nodes+edges structure in the config. `component_serialization.py` turns each operator into its fully-qualified dotted import path (or `{"class": path, "params": {changed-only}}`) — there is no static class table, it relies on `importlib`.

The canonical pipeline hash:

```python
# pipeline_config.py:354-356  — IDENTITY_HASH_LENGTH = 16
serializable = json.dumps(steps, sort_keys=True, separators=(",", ":")).encode("utf-8")
return hashlib.sha256(serializable).hexdigest()[:16]
```

This is **the** stable, import-path-canonical pipeline identity (embedded in `pipelines.name`). But it is a hash of a **linear list**: it does *not* normalize parallel-branch ordering, merge-input ordering, or template-vs-variant identity, and the JSON *stored* in `expanded_config` is not itself sort-key canonical (only the hash input is). The most DAG-like stable id nirs4all already has is the per-artifact **chain hash** (`sha256[:12]` of the `>`-joined `OperatorChain` path).

> All other identifiers (`run_id/pipeline_id/chain_id/prediction_id/artifact_id`) are **random uuid4**, not content hashes. The Arena therefore must **never trust producer UUIDs** for dedup; it must recompute content hashes on ingest.

### 2.5 What the public API persists, and the export gap

`run()` writes the full `runs→pipelines→chains→predictions(+parquet)→artifacts` graph (artifacts only if `save_artifacts=True`); a refit pass adds `fold_id="final"` predictions. `predict()`/`session().predict()` are read-only. `retrain()` writes a fresh run. **`explain()`/SHAP writes nothing to the relational store** — only PNG/HTML under `workspace/explanations/<uid>/`. The CLI has **no run/bundle export** (only `nirs4all dataset export`, which emits a dataset *config*); `RunResult.export()`, `store.export_run` (YAML, no binaries), and `export_predictions_parquet` are Python-only and must be assembled by hand — there is no single "Arena export" call today.

---

## 3. dag-ml — how saving / lineage / replay works

dag-ml is the "reproducible, traceable, OOF/leakage-safe ML coordinator." Its persistence philosophy is the inverse of nirs4all's bundle: **the core never serializes ML objects or feature buffers — it serializes references and fingerprints.** Phase model: `COMPILE → PLAN → FIT_CV → SELECT → REFIT → PREDICT → EXPLAIN`.

### 3.1 The `ExecutionBundle` — a JSON manifest of references

Not a directory or zip — a single serde-serializable struct (`schema_version = 1`) that round-trips as canonical JSON:

```rust
// bundle.rs:913
pub struct ExecutionBundle {
    pub bundle_id: BundleId,
    pub schema_version: u32,            // 1
    pub plan_id: String,
    pub graph_fingerprint: String,      // 64-hex SHA-256  ← the pipeline_dag_hash
    pub campaign_fingerprint: String,   // CV/variant/policy identity
    pub controller_fingerprint: String, // operator-version identity
    pub selected_variant_id: Option<VariantId>,
    pub selections: BTreeMap<String, SelectionDecision>,
    pub refit_artifacts: Vec<RefitArtifactRecord>,           // ArtifactRef, not bytes
    pub prediction_requirements: Vec<BundlePredictionRequirement>,
    pub prediction_caches: Vec<BundlePredictionCacheRecord>, // materialized OOF blocks
    pub data_requirements: Vec<BundleDataRequirement>,
    pub unsafe_flags: BTreeSet<String>,  // audit trail of leakage opt-ins
    pub metadata: BTreeMap<String, serde_json::Value>,
}
```

Artifacts are **referenced, never stored** — `ArtifactRef { id, kind, controller_id, backend (Joblib|Torch|Onnx|Safetensors|…), uri (relative only), content_fingerprint (SHA-256), size_bytes, plugin, plugin_version }`. `validate_portable` rejects absolute paths / URL schemes / `..` traversal. Two persistence stores exist, **neither of which deserializes a model**: `FileArtifactManifest` is manifest/reference-only (`artifact_manifest.json`, never touches payloads); `FileArtifactPayloadStore` copies the **opaque payload bytes** and verifies their SHA-256 + size *without deserializing them*. Prediction tables and `y_true` cross as JSON (`Vec<Vec<f64>>`); models/buffers/views cross as opaque `HandleRef`. Bundles + prediction caches are migration-policed (`validate_read_version` refuses version 0 and future versions, and old versions without a declared migration edge).

### 3.2 The canonical graph and the single fingerprint primitive

The graph **is** a real DAG: `GraphSpec { id, interface, nodes: Vec<NodeSpec>, edges: Vec<EdgeSpec>, … }` with a published JSON Schema, 20 `NodeKind`s (Transform, Split, Model, Fork, FeatureJoin, PredictionJoin, Aggregator, Tuner, Subgraph, …), typed ports (Data/Target/Prediction/Artifact/Metric/Control), and leakage-aware `EdgeContract { requires_oof, requires_fold_alignment, propagates_lineage, … }`. Validated by Kahn topological sort over a `BTreeSet` (deterministic). The `ExecutionPlan` (immutable post-PLAN) bundles `{graph_plan, campaign, node_plans, controller_manifests, variants, fold_set}` + the three fingerprints.

There is **exactly one** fingerprint algorithm:

```rust
// campaign.rs:20
fn stable_json_fingerprint<T: Serialize + ?Sized>(value: &T) -> Result<String> {
    let json = serde_json::to_vec(value)?;   // compact serde, NOT a JSON canonicalizer
    Ok(to_hex(&Sha256::digest(json)))         // lowercase 64-hex
}
```

Determinism is **structural, not via a canonicalizer**: every map in a fingerprinted struct is a `BTreeMap`/`BTreeSet` and field order is fixed by serde. `graph_fingerprint = stable_json_fingerprint(&graph)`, `campaign_fingerprint`, `controller_fingerprint`, plus order-independent `fold_set_fingerprint` (pre-sorts + strips empties; a locked fixture digest is asserted) and per-variant fingerprints. Stable id newtypes (`SampleId/TargetId/GroupId/ObservationId/NodeId/FoldId/VariantId/…`, charset-validated) are the join keys.

### 3.3 Lineage & research-export formats

`LineageRecord { record_id, run_id, node_id, phase, controller_id+version, variant_id, fold_id, branch_path, input_lineage, artifact_refs, params_fingerprint, seed, unsafe_flags, metrics }` — input lineage is inferred from DAG edges flagged `propagates_lineage`. From a validated plan+bundle+lineage, dag-ml emits, **all implemented and checksum-verified** (`RESEARCH_PROVENANCE_SCHEMA_VERSION = 1`):

- **W3C PROV** (`lineage.prov.jsonld`) — entity/activity/agent/used/wasGeneratedBy/wasDerivedFrom.
- **Workflow Run RO-Crate** (`ro-crate-metadata.json`, RO-Crate 1.1) — bundled as a `ResearchProvenancePackage` of files each with a SHA-256 checksum.
- **OpenLineage** `RunEvent` — with `dagml_reproducibility` (the three fingerprints + variant) and `dagml_oof_safety` facets.

**MLMD is *not* implemented** (despite the ecosystem CLAUDE.md listing it).

### 3.4 Replay, determinism, OOF/leakage

**Replay** (`ReplayPhaseRequest`, `Predict|Explain|Refit` only) needs the bundle + data envelopes (keyed `<node>.<input>`, fingerprint-checked) + refit artifacts (content-fingerprint-checked) + the prediction cache for OOF-dependent refit (REFIT replay is *refused* if requirements exist but no cache is supplied). **Determinism** comes from a SHA-256 **hash-chain seed derivation** (`SeedContext::derive_u64(root_seed, path…, label)`) — the same `(variant, fold, node)` path always yields the same seed; the parallel scheduler is byte-identical to sequential.

**OOF/leakage safety** is enforced by construction: `PredictionBlock { prediction_id, producer_node, partition (Train/Validation/Test/Final), fold_id, sample_ids, values }` joins by `SampleId` (never row position); using non-validation predictions as training features is **refused by default** (`OofLeakage` error). `FoldSet` validation guarantees each sample is validated exactly once, no train/val overlap, and **group non-leakage** (a validation sample's group cannot appear in train); nested CV draws inner folds only from outer-train. Selection is recorded as `SelectionDecision` (policy, chosen candidate, metric, `metric_level`, `EvaluationScope`, ranked candidates), refusing metric-level drift.

### 3.5 Metrics & integration status

Metrics are **regression-only** today (`Mse/Rmse/Mae/R2`), as `RegressionMetricReport { producer_node, partition, fold_id, level, target_width, target_names, metrics }` (macro + per-target). dag-ml **does not depend on nirs4all**; its Python binding is **validate/compile/fingerprint-only** (`validate_*_json`, `compile_pipeline_dsl_graph_json`, `build_execution_plan_json`, `fold_set_fingerprint_json`) — actual `fit_cv/select/refit/predict` run only through the Rust core, CLI, or the C-ABI (which *does* expose full replay via controller/data/artifact/prediction-cache vtables). **dag-ml is a parallel/future engine, not yet wired into nirs4all.**

---

## 4. nirs4all-io — the dataset-identity layer (the data-plane bridge to dag-ml)

`nirs4all-io` is the **dataset-assembly bridge**: `any input (folder/glob/dict/JSON-YAML/arrays/vendor-corpus + reference-table) → RESOLVE → INFER → CONFIGURE → MATERIALIZE → dataset`. It is the third persistence-relevant mechanism, and the one that supplies what the other two lack: **stable dataset + sample identity** — the source of `DatasetCard`/`DatasetFingerprint`, and (via an ordinal→`SampleId` sidecar, §4.4) the path to fixing nirs4all's positional `sample_indices` (§2.1). Since 2026-05 io is a **Rust workspace** (`crates/`) with a C ABI, a CLI, four bindings, and — crucially — a dedicated **`nirs4all-io-dagml`** crate that emits dag-ml's data-side contract. The Python MVP (`src/nirs4all_io/`) is now the byte-parity oracle; the shipping surface is the Rust core + pyo3 wheel.

### 4.1 The `DatasetSpec` contract (a producer-agnostic, hashable dataset declaration)

Everything funnels into a `DatasetSpec` IR — a versioned (`schema_version = 1`), JSON-Schema-validated (`dataset_spec.schema.json`, draft 2020-12), **canonical-JSON** (byte-identical Python≡Rust) declaration of: `sources` (multi-source; each carries a `role` ∈ features/targets/metadata/weights/ignore/mixed, a `kind` table/lookup, a `merge` mode, and a column-selector DSL), relational `join`s (`cardinality` 1:1/m:1/1:m, `coverage` complete/warn/drop/error — **io performs every join itself**), `partitions` (deterministic only — `column`/`index`/`index_file`; **no random/percentage/stratified split**: "it's a loader, not a splitter"), `folds`, `signal_type`, `task_type`, and `sample_index` (declared identity — §4.3). RESOLVE stamps a **stable per-file identity + SHA-256 `content_hash`** on every input (`resolve.rs:41,64`); ordering is deterministic ("retrofitting identity later would break fingerprints").

This is exactly the raw material for the Arena's `DatasetCard` (name / modality / signal_type / axis unit+range / task_type / multi-source layout / declared folds) and `DatasetVariant` (subsampling / aggregation / source selection): a portable, schema-versioned, hashable dataset *declaration* shared by every producer.

### 4.2 The `AssembledDataset` seam → two adapters

`assemble(spec)` produces a **target-agnostic `AssembledDataset`** (per-partition `PartitionBlock`s: multi-source X, y, metadata, headers/units, weights, named processings; `folds` as positional row-index pairs). Two adapters consume it:

```
DatasetSpec ─assemble()─► AssembledDataset ─┬─ to_spectrodataset() ─► nirs4all SpectroDataset   (POSITIONAL ids)
                                            └─ to_dag_ml_data()    ─► dag-ml-data CoordinatorDataPlanEnvelope
                                                                       inputs: DatasetSchema + SampleRelationTable
                                                                       stored: 3 SHA-256 fingerprints + DataPlan +
                                                                               derived relation set (stable SampleId/Group)
```

The `SpectroDataset` path (the live nirs4all input) keys rows **positionally** — `SpectroDataset` has no first-class slot for observation/group ids, so the sample-id column is dropped at materialization. The `to_dag_ml_data` path is where stable identity is born.

### 4.3 The dag-ml emit: stable identity + three fingerprints (resolves the positional-sample gap)

`nirs4all-io-dagml::to_dag_ml_data(&AssembledDataset) -> CoordinatorDataPlanEnvelope` (a single function; depends directly on the real `dag-ml-data` crate — no JSON duplication; workspace-excluded so the published lib stays ecosystem-free). It builds dag-ml-data's data-side contracts:

- **`DatasetSchema`** = `{dataset_id, sample_ids, sources: SourceDescriptor (+ RepresentationSpec/AxisSpec; nm→Wavelength, cm⁻¹→Frequency, signal_type→tags), targets}` — folds/groups deliberately left empty (dag-ml's domain).
- **`SampleRelationTable`** = one `SampleRelation{observation_id, sample_id, group_id, repetition_id}` per row. Identity is synthesized from the **repetition key carried into the `AssembledDataset`** (`spec.repetition` / `sample_index.repetition_id` — the only leakage unit io threads through today; `sample_index.key`/`observation_id`/`group_id` are *parsed* but **not** yet threaded through assembly): same raw value → same collision-safe, ASCII-sanitized `SampleId`; with reps, `group_id = sample` and `repetition_id = rep.N`; **without** such a repetition key, each row is its own 1:1 sample (`obs.N`/`s.N` — i.e. positional).
- **`DataPlan`** = a `Materialize` step per source (+ a `Join` step → `model_input` only when multi-source).
- Final: `CoordinatorDataPlanEnvelope::from_parts(&schema, plan, relations)` **consumes** the schema + relation table to compute **`schema_fingerprint`, `plan_fingerprint`, `relation_fingerprint`** (each 64-hex SHA-256 over explicitly-sorted serde JSON), then **stores only** the three fingerprints, the `DataPlan`, and a *derived* `CoordinatorRelationSet` — the original `DatasetSchema`/`SampleRelationTable` are **not** carried, and the derived relation set **drops `repetition_id`**. The envelope is a *lossy identity receipt*, not a full dataset record.

**This is the Arena's `DatasetFingerprint` source**, and a stable `ResidualSet` key **for dag-ml-native runs** (whose `PredictionBlock`s carry `SampleId` directly). It is the data-side counterpart of dag-ml's `ExternalDataPlanEnvelope` (§3): dag-ml consumes io's envelope (the lossy fingerprints+relations subset, wrapped by a hand-authored `DataBinding` inside a `CampaignSpec`). Because the envelope is lossy, an Arena that needs `DatasetCard` fields or repetition identity must **also capture the `DatasetSpec` + full `DatasetSchema`/`SampleRelationTable`** (or an io sidecar) — not just the envelope. So io supplies dataset+sample *identity*; dag-ml owns folds/OOF/leakage/lineage.

### 4.4 Direction, the "post-dag-ml" framing, and maturity

**Direction = pre-dag-ml data provider.** ADR-0001 (Accepted) gives io ownership of the `SpectroDataset → CoordinatorDataPlanEnvelope` bridge; io does **not** consume any dag-ml output and does **not** build `FoldSet`/`DataBinding`. On the "io as a post-dag-ml intermediary" idea: the *implemented* coupling is upstream, but io's envelope is the **identity spine that bookends a run** — it defines the stable ids going *into* a run, and the Arena reads predictions/residuals back *out* against them. This is **direct for dag-ml-native runs** (`PredictionBlock` carries `SampleId`). For **nirs4all runs it is not wired today**: nirs4all writes *positional* integer `sample_indices` (its indexer auto-numbers rows), and the io→`SpectroDataset` adapter passes only `{partition}`, not stable ids — so nothing links io's `SampleId` to nirs4all's row positions. Closing it needs an explicit **io ordinal→`SampleId` sidecar** (`global_row_ordinal + partition + partition_row → observation/sample/group/repetition id`, emitted from the *same* assembly that built the `SpectroDataset`, row-count/order validated before mapping). A literal post-dag-ml *results* assembler in io would be new scope; the low-cost role is io as the **dataset-identity authority** consulted at both ends — plus that sidecar.

**Maturity (real today vs roadmap).** Real & tested: the `DatasetSpec`/canonical-JSON contract, per-file content hashes, and the `to_dag_ml_data` emit with three fingerprints + per-observation ids (EPIC 10 GREEN), driven by the `emit-dagml` binary. Caveats to design around:
- The `AssembledDataset`/`PartitionBlock` do **not** thread stable per-row ids through (they are positional); ids are reconstructed at emit from the repetition key. Stable ids therefore exist **only via the dag-ml emit**, not in the bare `SpectroDataset`/assembled IR.
- Stable ids materialize **only when a repetition key is carried into the `AssembledDataset`**; otherwise they degrade to positional (`s.N`). `sample_index.key`/`observation_id`/`group_id` are parsed but not yet threaded through assembly.
- **No ordinal link to nirs4all runs yet.** nirs4all residuals are *position-keyed* (`sample_indices`) and the io→`SpectroDataset` adapter does not carry `SampleId`; mapping residuals onto io identity needs the ordinal→`SampleId` sidecar above. nirs4all's aggregated (`_agg`) prediction twins carry `sample_indices=None`, so they must be excluded from a per-sample `ResidualSet` (or modeled at group level).
- io's `unique_sample_id` **sanitizes** raw labels (ASCII-safe, collision-free) but does **not pseudonymize** them; the Arena must pseudonymize sample ids at ingest where `DESIGN.md` §11 requires it.
- The emit is **not exposed via any language binding or the C ABI in v0** — only the `nirs4all-io-dagml` `emit-dagml` binary (the in-tree CLI subcommand is a stub pointing to it); Python `load(target="dag-ml-data")` raises `NotImplementedError`.
- Contract-drift risk: io maps cm⁻¹ → `AxisKind::Frequency` (unit `"cm-1"`) while `dag-ml-data` now ships a first-class `AxisKind::Wavenumber`; the gate doc assumed `Wavenumber`. Since the axis kind feeds `schema_fingerprint`, this divergence must be pinned before the fingerprints are treated as a stable cross-version contract.

## 5. Compatibility analysis

### 5.1 Side-by-side

| Dimension | nirs4all | dag-ml | Compatible? |
|---|---|---|---|
| **On-disk container** | `store.sqlite` + `arrays/*.parquet` + `artifacts/*.joblib`; portable `.n4a` ZIP | one JSON `ExecutionBundle` + externally-referenced payloads + RO-Crate package | ❌ No common container; **not interchangeable** |
| **Pipeline model** | linear step list; branches are runtime keyword dicts | true DAG (`GraphSpec` nodes/edges/ports) | ⚠️ Semantically mappable; structurally different |
| **Pipeline identity** | `sha256(sorted-JSON step list)[:16]` | `sha256(canonical GraphSpec)` = `graph_fingerprint` | ❌ Different inputs → **different hashes** for the "same" pipeline |
| **Hash algorithm** | SHA-256 (xxh128 for *data*) | SHA-256 everywhere | ✅ Same primitive, different scope/canonicalization |
| **Record ids** | random uuid4 | deterministic fingerprints + hash-chain seeds | ❌ nirs4all ids non-reproducible; dag-ml ids reproducible |
| **Fitted artifacts** | embedded joblib (in `.n4a`) or content-addressed files | reference-only (`ArtifactRef`: backend + relative URI + SHA-256), never embedded | ⚠️ Both SHA-256 content-address; storage policy opposite |
| **Predictions** | parquet rows, **position-keyed** via *positional* `sample_indices`, fold/partition tagged; **classification supported** (`task_type`, multi-metric `scores`) | `PredictionBlock`, **sample-keyed** via stable `SampleId`, OOF-validated; **regression only** | ⚠️ Both fold-tagged → both map to `ResidualSet`, but only dag-ml is sample-keyed; nirs4all needs the io ordinal→`SampleId` sidecar (§4.4) |
| **Scores/metrics** | rich, stored in SQLite (`val/test/train`, multi-metric JSON, any task) | `RegressionMetricReport` (regression metrics), computed not yet persisted from a nirs4all run | ⚠️ nirs4all richer *today*; dag-ml stricter provenance |
| **Leakage safety** | fold/partition columns, no enforced OOF invariant in the store | enforced by construction (OOF, group/nested refusal) | ➕ dag-ml strictly stronger |
| **Provenance export** | none (synthesize from store metadata) | W3C PROV + RO-Crate + OpenLineage (checksummed) | ➕ dag-ml free; nirs4all none |
| **Versioning** | `SCHEMA_VERSION=2`, `.n4a` `"1.0"`, frozen by tests | per-artifact `schema_version` + `SchemaMigrationPolicy` | ✅ Both versioned |
| **Used by nirs4all today?** | n/a (it *is* nirs4all) | no | — |

### 5.2 Verdict at three levels

1. **Storage level — incompatible, and that's fine.** Neither can read the other's files. The Arena must not try to unify the containers; it ingests each via its own adapter into the Arena schema (`DESIGN.md` §7).
2. **Semantic level — strongly compatible.** Both decompose a run into the same conceptual entities, which are exactly the Arena's `PipelineDAGSpec / RunCondition / ScoreSet / ResidualSet / provenance`. The Arena model is the common denominator.
3. **Interop level — one concrete bridge exists, with sharp edges.** dag-ml's `lower_nirs4all_compat_pipeline_dsl` (`dsl.rs:593`) parses nirs4all-style pipeline JSON into a `GraphSpec`, reachable from Python via `compile_pipeline_dsl_graph_json`. This is the mechanism that lets the Arena assign **one** pipeline identity to runs from either producer — *but* that binding returns the `GraphSpec` JSON, not the fingerprint (the `graph_fingerprint` is computed at `ExecutionPlan` build, `plan.rs:628`), and the compat lowerer preserves the root graph `id` or defaults it to `dsl-nirs4all-compat` (`dsl.rs:998`). So the bridge needs an explicit fingerprint API **and** an Arena-owned id/order normalization before its output is a stable cross-producer hash (§5.3).

### 5.3 The pipeline-identity decision (answers `DESIGN.md` §13 Q1)

The Arena needs a single `pipeline_dag_hash` that (a) is topology-aware, (b) deduplicates "same pipeline, different syntax," and (c) is identical whether a run came from nirs4all today or native dag-ml later. Options:

| Option | `pipeline_dag_hash` source | Pros | Cons |
|---|---|---|---|
| **v0 fallback** | nirs4all `get_hash` | available now, zero deps | linear-only; ≠ dag-ml; no branch/merge normalization |
| **★ Target (recommended)** | dag-ml `graph_fingerprint`, from lowering `expanded_config` → `GraphSpec` → `ExecutionPlan` | topology-aware, dedup-able, gives a real `PipelineDAGSpec` (= `GraphSpec`); reproducible under same compiler + normalized input | fingerprint is **not** returned by the compile binding (needs `build_execution_plan_json` or a new `graph_fingerprint_json`); equal across producers **only** under Arena-owned graph-id/node-id/order normalization; nirs4all→compat shape must be proven on real `expanded_config` fixtures (hash `expanded_config`/`pipeline.json`, **never** `chain.json`); pulls in the `dag-ml-py` wheel at ingest |
| Arena-defined | a new canonicalizer over `expanded_config`/`ExecutionTrace` | full control | reinvents what dag-ml already does; risk of divergence from dag-ml |

**Recommendation:** adopt the target, but treat it as a contract to *earn*, not assume. Define the Arena's `PipelineDAGSpec` as **dag-ml's `GraphSpec`** and `pipeline_dag_hash` as its `graph_fingerprint`. Compute it at ingest by lowering `pipelines.expanded_config` (or bundle `pipeline.json` — **not** `chain.json`, which carries chain step descriptors + artifact refs, not a DSL) through `dag-ml-py`, building an `ExecutionPlan`, and reading `graph_fingerprint` — after applying an Arena-owned normalization of the graph `id`, node ids and node ordering (otherwise two semantically-equal graphs with different ids/order hash differently). Keep `get_hash` as a recorded secondary id (`nirs4all_identity_hash`) and as the v0 fallback until the compat shape and normalization are proven on real fixtures (§8 step 1). The compile/fingerprint path is available today (it does not require dag-ml to be the execution engine), but the fixture spike is a hard precondition before the hash is promoted to a stable cross-repo contract.

---

## 6. Using these for the Arena

### 6.1 Producer-agnostic ingestion architecture

```
  files / glob / config / arrays / vendor-corpus + reference-table
        │
        ▼  nirs4all-io :  RESOLVE → INFER → CONFIGURE → MATERIALIZE
        ├─► SpectroDataset ............................... (input fed to a run)
        └─► CoordinatorDataPlanEnvelope  =  DATASET IDENTITY
              schema/plan/relation SHA-256 fingerprints + stable Sample/Group/Repetition ids
              ⇒ DatasetCard · DatasetFingerprint · stable ResidualSet sample keys
                        │  (dataset identity, shared by every run of that dataset)
   run producers        ▼
   ┌─────────────────────────────────────────────┐
   │ adapter A: nirs4all-workspace  (LIVE TODAY)  │──┐
   │   SQLite+Parquet → pipeline + scores +       │  │
   │   residuals; strip artifacts; pipeline→hash  │  ├─► ArenaStore  (DESIGN.md §7:
   ├─────────────────────────────────────────────┤  │    store.sqlite + arrays/*.parquet,
   │ adapter B: dag-ml-bundle  (FUTURE)           │──┘    NO artifacts)
   │   JSON bundle + lineage → graph_fingerprint, │
   │   OOF residuals, provenance                  │
   └─────────────────────────────────────────────┘
```

Three inputs, one schema. **`nirs4all-io` supplies the dataset identity** (`DatasetCard`/`DatasetFingerprint` + stable sample ids) consumed by every run of that dataset; **adapter A** (nirs4all workspace, live) and **adapter B** (dag-ml bundle, future) supply the pipeline + scores + residuals. The Arena owns the pipeline canonicalization (`pipeline_dag_hash` from §5.3), keys **dag-ml-native** residuals by io's stable sample ids (nirs4all residuals stay position-keyed until the io ordinal→`SampleId` sidecar exists, §4.4), and **recomputes every content hash on ingest** — producer UUIDs are never trusted (matches `DESIGN.md` §6.18 idempotency on `input_export_hash`).

### 6.2 Mapping tables

**nirs4all → Arena** (adapter A, today):

| Arena entity (`DESIGN.md`) | nirs4all source |
|---|---|
| `PipelineDAGSpec` + `pipeline_dag_hash` | `pipelines.expanded_config` (**not** `chain.json`) → lower via `dag-ml-py` → `GraphSpec` → `graph_fingerprint` via `ExecutionPlan` build + Arena id/order normalization (fallback: `get_hash`) |
| `PipelineNodeSpec`/`OperatorSpec`/`ParameterSpec` | `chains.steps` JSON + `ExecutionTrace`/`OperatorChain`; operators already carry dotted import path + params |
| `RunCondition` / `ExecutionSummary` | `runs` + `pipelines` rows; `nirs4all_version`; producer capsule |
| `ScoreSet` / `MetricObservation` | `predictions.{val,test,train}_score` + multi-metric `scores` JSON + `chains` CV/final/`_agg` summaries; `task_type`, `metric` |
| `ResidualSet` | `arrays/*.parquet` (`y_true/y_pred/y_proba`, `weights`); residual = `y_true − y_pred` (**regression only**). `sample_indices` are **positional**; mapping them to a stable, pseudonymized `sample_pseudo_id` needs the io **ordinal→`SampleId` sidecar** (§4.4) — there is no join key today. Exclude `_agg` twins (their `sample_indices` are `None`) from per-sample residuals |
| `DatasetCard`/`DatasetFingerprint` + stable sample ids | **From `nirs4all-io`** (§4): the `DatasetSpec` gives the card's structure (sources/roles/axis/signal/folds) and the envelope gives the `schema`/`plan`/`relation` SHA-256 fingerprints + stable `SampleId`/`GroupId`. The envelope is **lossy** (no original schema/relation table; `repetition_id` dropped), so also capture the `DatasetSpec` + full `DatasetSchema`/`SampleRelationTable` (or an io sidecar) for card fields + repetition identity. nirs4all's own `dataset_name`/`dataset_hash` is the weaker fallback (positional) when io is not in the loop |
| `RNGContext` | **gap** — not first-class in the nirs4all store today (see `DESIGN.md` Stream H3). io does not carry it either; dag-ml's hash-chain seed (§3.4) is the model to adopt |
| Provenance | **synthesize** from store metadata (no native export) |

**dag-ml → Arena** (adapter B, future, mostly free):

| Arena entity | dag-ml source |
|---|---|
| `PipelineDAGSpec` + `pipeline_dag_hash` | `GraphSpec` + `ExecutionPlan.graph_fingerprint` (native, no reconstruction) |
| `RunCondition` | `ExecutionBundle` (plan + 3 fingerprints + selected variant + policies) + `CampaignSpec` + root seed |
| `ScoreSet` / `MetricObservation` | `RegressionMetricReport` (already node/partition/fold/level-tagged) + `SelectionDecision` |
| `ResidualSet` | `PredictionBlock` / `AggregatedPredictionBlock` (sample-keyed, OOF-validated, materialized in `prediction_caches`) |
| Provenance | `ResearchProvenancePackage` (PROV + RO-Crate + OpenLineage) stored as-is |
| Leakage attestation | `unsafe_flags` + enforced OOF/fold invariants |

What dag-ml gives **for free** that nirs4all's store does not: a topology-aware graph hash, fold-tagged sample-keyed OOF predictions with enforced leakage safety, content-addressed portable artifact refs, schema-versioned migration policy, three standards-conformant provenance exports, and replay determinism.

**Classification vs regression (a required Arena policy).** nirs4all stores classification natively — `task_type`, `y_proba` (+ `y_proba_shape`), and classification metrics in the `scores` JSON. dag-ml's metric model is **regression-only** (`Mse/Rmse/Mae/R2`). Two consequences: (1) the Arena must ingest classification scores **directly from the nirs4all store** and must **not** route them through dag-ml's metric model; (2) `ResidualSet`'s `residual = y_true − y_pred` is a regression concept — for classification, store the predicted class / `y_proba` / an error indicator instead, and compute classification metrics (accuracy, F1, AUC, …) from those. The `pipeline_dag_hash` lowering (§5.3) is orthogonal to task type and applies to both.

### 6.3 The upload flow: with/without artifacts, with/without results

The user's intent — *"upload an `.n4a` (or a richer format), with or without artifacts, with or without results; if already run, store & display, else run on the datasets, store & display"* — maps to this state machine:

```
INGEST(upload, target_datasets)
  1. PARSE → extract the pipeline recipe (from .n4a manifest+pipeline.json/chain.json/trace.json,
             or from a dag-ml GraphSpec/bundle, or from a raw nirs4all pipeline list).
  2. CANONICALIZE → pipeline_dag_hash via dag-ml lowering (§5.3); dataset identity (DatasetFingerprint + stable sample ids) from io's envelope (§4); drop any fitted artifacts (Arena never stores them).
  3. For each target dataset/variant:
       run_condition_hash = H(dataset_variant, split, cv, rng, pipeline_dag, refit)   # DESIGN §6.13
       if EXISTS(run_condition_hash) in ArenaStore:
            → STORE-NOTHING / DISPLAY existing ScoreSet+ResidualSet            # "already run"
       elif upload carries RESULTS for this condition (scores+residuals present):
            → VALIDATE (DESIGN §8.4) → INGEST ScoreSet+ResidualSet → DISPLAY   # "has results"
       else:
            → SCHEDULE a nirs4all run(pipeline, dataset)  (compute is out of scope for storage)
            → on completion, ingest its workspace via adapter A → DISPLAY      # "run then store"
```

Three consequences:
- **"With artifacts"** uploads are accepted but the Arena **strips the weights** at step 2 — it keeps only the recipe + hash (consistent with `DESIGN.md` §2 "no artifacts").
- **"Without results"** uploads trigger the compute path (run on the referenced datasets, then ingest). The Arena store itself does not run compute; it consumes the resulting workspace.
- **"Already run"** detection is exact *because* identity is content-derived (the canonical `pipeline_dag_hash` + the rest of the `run_condition_hash`), never the producer's random UUIDs.

### 6.4 The "richer format" the user wants ≈ a weights-free, results-rich export

Today neither `.n4a` (weights, no results) nor a hand-assembled `export_run` covers what the Arena needs. The "richer format" the user describes is essentially **dag-ml's bundle shape applied to a nirs4all run**: *canonical pipeline (graph) + `pipeline_dag_hash` + dataset card/fingerprint + the full predictions/scores + residuals + RNG/CV/split specs + (optional) provenance — and NO fitted weights.* This should become a first-class **nirs4all "export-arena" profile** (`DESIGN.md` Stream H1), emitting exactly the Arena's ingestion contract in one call. Until it exists, adapter A assembles it from `store.export_run` (YAML) + `export_predictions_parquet` + the SQLite metadata. Note the **dataset-identity half** of that profile (card/fingerprint + stable sample ids) is **already produced by `nirs4all-io`** (§4): the export should reference or embed io's `CoordinatorDataPlanEnvelope` rather than re-derive it, so a run's residuals are keyed by io's stable `SampleId` from the start.

---

## 7. Answers to `DESIGN.md` §13 + recommended decisions

| `DESIGN.md` §13 question | Answer from this analysis |
|---|---|
| What exact format of `PipelineDAGSpec` shared with dag-ml? | **dag-ml `GraphSpec`** (already a published-schema canonical DAG). `pipeline_dag_hash` = its `graph_fingerprint`, obtained by lowering `expanded_config` → `GraphSpec` → `ExecutionPlan` (the compile binding returns the graph; the plan build returns the fingerprint) under Arena-owned graph-id/node-id/order normalization. Validate on real fixtures first (§8 step 1). |
| What residuals can be published per confidentiality level? | dag-ml `PredictionBlock` is sample-keyed (stable `SampleId`); nirs4all parquet is **position-keyed** (`sample_indices`) and needs the io ordinal→`SampleId` sidecar (§4.4) to become sample-keyed. **Pseudonymize** sample ids at ingest (io *sanitizes* but does not pseudonymize); publish `y_true/y_pred`/residuals per the dataset's policy (public → full; restricted/private → aggregated scores ± authorized residuals). |
| Size limit for retaining residuals of large user runs? | Both stores are per-sample float lists; Zstd + the Arena's existing dedup (`target_vector_hash`, sample-id dictionaries) apply. Set a per-`ResidualSet` cap and fall back to scores-only above it (log the truncation). |
| Materialize DAG-derived columns or compute on demand? | dag-ml gives node/operator/param structure cheaply; materialize the high-traffic effect-analysis columns (main model, presence of SNV, `n_components`) at ingest, keep the canonical `GraphSpec` JSON as source of truth. |

**Decisions to carry into the first Arena dev:**
1. **Two adapters, one schema.** Adapter A (nirs4all workspace, live) and Adapter B (dag-ml bundle, future) both write the Arena schema; don't unify the source containers.
2. **`pipeline_dag_hash` = dag-ml `graph_fingerprint`** via compat lowering — with three prerequisites: an explicit fingerprint API in `dag-ml-py` (a `graph_fingerprint_json` helper, or `build_execution_plan_json` + extract, since the compile binding returns only the graph); an Arena-owned normalization of graph/node ids and ordering; and a passing fixture spike on real `expanded_config` (§8 step 1). Hash only `expanded_config`/bundle `pipeline.json`, **never** `chain.json`. Record nirs4all `get_hash` as a secondary id and ship a `get_hash`-only v0 path so ingestion is unblocked meanwhile.
3. **Recompute all content hashes at ingest; never trust producer UUIDs.**
4. **Strip artifacts at ingest, always** (accept `.n4a` with weights, discard the weights).
5. **Push a nirs4all "export-arena" profile** (weights-free, results-rich) — it's the missing single call; until then assemble from existing Python exports.
6. **Treat dag-ml's provenance (PROV/RO-Crate/OpenLineage) as the provenance layer** when runs come from dag-ml; synthesize a minimal equivalent for nirs4all-only runs.
7. **Close the `RNGContext` gap in nirs4all** (`DESIGN.md` Stream H3) — dag-ml already has hash-chain seed derivation; nirs4all's store does not persist a full RNG context.
8. **`nirs4all-io` is the dataset-identity authority.** Take `DatasetCard`/`DatasetFingerprint` + stable sample ids from io's `CoordinatorDataPlanEnvelope` (three fingerprints + derived relation set), not from nirs4all's positional `sample_indices`. The emit is implemented (EPIC 10, GREEN), but the gaps are real: an **io ordinal→`SampleId` sidecar** so nirs4all residuals become sample-keyed (§4.4); exposing the emit beyond the `emit-dagml` binary; threading stable ids through `AssembledDataset`; **pseudonymizing** ids at ingest (io only sanitizes); and pinning the cm⁻¹ axis kind. Until the sidecar exists, ingest nirs4all residuals as position-keyed.
9. **Track schema-version drift across all four contracts** — nirs4all store (`SCHEMA_VERSION` / `.n4a` `1.0`), dag-ml bundle/plan, io `DatasetSpec`/canonical-JSON, and dag-ml-data envelope/fingerprint. Fingerprints are comparable only within matching contract versions; the cm⁻¹→`Frequency`-vs-`Wavenumber` mismatch is the first concrete drift case (it changes `schema_fingerprint`).

---

## 8. Concrete next steps (smallest useful slices)

1. **Spike the compat lowering + fingerprint path**: feed 3–5 real `pipelines.expanded_config` JSON blobs through `dag-ml-py.compile_pipeline_dsl_graph_json`, then through `build_execution_plan_json` (the compile step returns only the `GraphSpec`; the fingerprint comes from the plan), and read `graph_fingerprint`. Define the Arena normalization of graph `id` / node ids / ordering and confirm two equivalent pipelines hash identically. Confirm the input is `expanded_config` (a step list / `{pipeline|steps}`), **not** `chain.json`. Output: the validated `pipeline_dag_hash` recipe, the normalization rule, and a list of nirs4all constructs the compat lowering does not yet accept. *(Single highest-leverage task — it de-risks decision 2 and the whole `PipelineDAGSpec` stream, and tells dag-ml whether to add a `graph_fingerprint_json` helper.)*
2. **Adapter A skeleton**: read a real `workspace/` (SQLite + parquet), emit Arena `RunCondition/ScoreSet/ResidualSet` rows with artifacts stripped, hashes recomputed. No dataviz.
3. **Fixture**: one workspace from a branch/merge pipeline + one from a plain PLS sweep → freeze their `pipeline_dag_hash`es as a regression contract (mirrors `DESIGN.md` Phase 0 I1).
4. **Adapter B contract test (no execution)**: parse a sample dag-ml `ExecutionBundle` + RO-Crate JSON into the same Arena rows, asserting the `graph_fingerprint` from A and B agree for an equivalent pipeline.
5. **io identity spike**: run `nirs4all-io` `emit-dagml` on the two fixture datasets and capture the `CoordinatorDataPlanEnvelope` (three fingerprints + derived relation set). Confirm what the envelope drops (original schema/relation table, `repetition_id`) and surface the cm⁻¹→`Frequency` axis drift (§4.4) and whether the emit needs a binding/C-ABI surface for the Arena to call it directly.
6. **io ordinal→`SampleId` sidecar spike** *(the §4.4 BLOCKER)*: emit a manifest `global_row_ordinal + partition + partition_row → observation/sample/group/repetition id` from the *same* assembly that builds the `SpectroDataset`, and prove a real `arrays/*.parquet` `sample_indices` column joins to io `SampleId` with row counts/order validated (and `_agg` twins excluded). Without it, nirs4all residuals are position-keyed only and cannot be compared cross-dataset by stable sample identity.

---

## Appendix: source reference index

**nirs4all** (`/home/delete/nirs4all/nirs4all/nirs4all/`, v0.10.0):
- `.n4a` bundle: `pipeline/bundle/generator.py` (`BUNDLE_FORMAT_VERSION` :54, `_create_bundle_manifest` :427, `_export_n4a` :305), `pipeline/bundle/loader.py` (`predict` :557, `_predict_with_trace` :608).
- Workspace store: `pipeline/storage/store_schema.py` (`SCHEMA_VERSION` :28, `SCHEMA_DDL` :44, `_migrate_schema` :670), `pipeline/storage/workspace_store.py` (`save_artifact` :1323, `export_chain` :2271, `export_run`/`export_predictions_parquet` :2385), `pipeline/storage/array_store.py` (`_PARQUET_SCHEMA` :124).
- Artifacts: `pipeline/storage/artifacts/artifact_persistence.py`, `artifact_registry.py`, `operator_chain.py` (`compute_chain_hash` :611, `generate_artifact_id_v3` :623), `types.py` (`ArtifactRecord` :128).
- Pipeline hash & serialization: `pipeline/config/pipeline_config.py` (`get_hash` :346), `pipeline/config/component_serialization.py` (`serialize_component` :128); trace `pipeline/trace/execution_trace.py`.
- Contracts: `tests/regression/test_bundle_contract.py`, `tests/regression/test_storage_schema_contract.py`.
- dag-ml reference (prose only): `visualization/pipeline_diagram.py:27`.

**dag-ml** (`/home/delete/nirs4all/dag-ml/`, v0.1.0-alpha):
- Bundle/artifacts: `crates/dag-ml-core/src/bundle.rs` (`ExecutionBundle` :913, `SchemaMigrationPolicy` :40, `validate_read_version` :97), `runtime.rs` (`ArtifactRef` :75, `FileArtifactManifestStore` manifest-only :483, `FileArtifactPayloadStore` opaque payload-copying :564/:571, `LineageRecord` :745).
- Graph/plan/fingerprint: `graph.rs` (`GraphSpec` :186, `NodeKind` :13, `EdgeContract` :87), `plan.rs` (`ExecutionPlan` :203, fingerprints :628), `campaign.rs` (`stable_json_fingerprint` :20), `generation.rs` (variant fingerprints :310), `fold.rs` (`FoldSet` :19, `fold_set_fingerprint` :153), `ids.rs` (:72), `rng.rs` (`derive_u64` :24).
- DSL bridge: `dsl.rs` (`lower_nirs4all_compat_pipeline_dsl` :593, `compile_pipeline_dsl` :552, compat root accepts array/`{pipeline|steps}` :974, root `id` default `dsl-nirs4all-compat` :998); graph `id` field hashed into the fingerprint `graph.rs:187`; Python surface `crates/dag-ml-py/src/lib.rs` (`compile_pipeline_dsl_graph_json` returns `GraphSpec` JSON :116, `build_execution_plan_json` :142, `fold_set_fingerprint_json`).
- OOF/selection/metrics: `oof.rs` (`PredictionBlock` :29, leakage refusal :365), `selection.rs` (`SelectionDecision` :254), `metrics.rs` (`RegressionMetricReport` :100), `aggregation.rs`, `relation.rs` (`SampleRelation` :29).
- Provenance: `provenance.rs` (`build_prov_jsonld` :396, `build_ro_crate_metadata` :765, `build_openlineage_run_event` :294, schema version :17).
- Spec/status: `docs/COORDINATOR_SPEC.md`, `docs/STATUS.md`, `docs/ARCHITECTURE.md`.

**nirs4all-io** (`/home/delete/nirs4all/nirs4all-io/`, Rust phase-2 complete; Python MVP = byte-parity oracle):
- Dataset config IR: Python `src/nirs4all_io/spec/dataset_spec.py` (`DatasetSpec`, `SCHEMA_VERSION=1`) ≡ Rust `crates/nirs4all-io-core/src/spec/dataset_spec.rs` (:865); JSON Schema `spec/json_schema.py` / `dataset_spec.schema.json`.
- Four-phase flow: `resolve/` (`InputItem` stable identity + sha256 `content_hash`, Rust `resolve.rs:33,64`), `infer/`, `conventions/`, `materialize/assemble.py` (`AssembledDataset`/`PartitionBlock`, positional rows).
- dag-ml data-plane emit: `crates/nirs4all-io-dagml/src/lib.rs` (`to_dag_ml_data` :150, `unique_sample_id` :62, axis map nm→Wavelength / cm⁻¹→Frequency :86, `from_parts` call :388); driver `src/bin/emit_dagml.rs`. Python `load(target="dag-ml-data")` → `NotImplementedError` (`src/nirs4all_io/api.py:185`).
- C ABI (`n4io_*`, JSON only — no emit/arrays in v0): `crates/nirs4all-io-capi/src/lib.rs`. ADR-0001 (io owns the `SpectroDataset → CoordinatorDataPlanEnvelope` bridge).
- dag-ml-data sink: `../dag-ml-data/crates/dag-ml-data-core/src/coordinator.rs` (`CoordinatorDataPlanEnvelope::from_parts` :209, three SHA-256 fingerprints).
- Docs: `docs/DATASET_CONFIGURATIONS.md`, `docs/RUST_REWRITE_ROADMAP.md`, `docs/PHASE2_GATE.md`, `docs/STATUS.md`, `CLAUDE.md`.
