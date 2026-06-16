# Identity

Reference for the Arena's identity spine — the content-addressed hashes that key every dimension and make pipeline×dataset runs deduplicable and cross-comparable. Source: `src/nirs4all_benchmarks/identity/`.

## Contents

- [The one hashing rule](#the-one-hashing-rule)
- [`canonical_json` and `sha256_hex`](#canonical_json-and-sha256_hex)
- [`fingerprint`](#fingerprint)
- [`compose_run_condition_hash` — the fixed six-field order](#compose_run_condition_hash--the-fixed-six-field-order)
- [`export_hash` — the idempotency key](#export_hash--the-idempotency-key)
- [The identity table (recomputed vs. adopted)](#the-identity-table-recomputed-vs-adopted)
- [`pipeline_dag_hash` in depth](#pipeline_dag_hash-in-depth)
  - [Merkle-DAG normalization](#merkle-dag-normalization)
  - [Why it is topology-aware and producer-agnostic](#why-it-is-topology-aware-and-producer-agnostic)
  - [Recorded secondary ids](#recorded-secondary-ids)
  - [Linear-list lifting](#linear-list-lifting)
  - [Worked example: id-invariance and merge-order-invariance](#worked-example-id-invariance-and-merge-order-invariance)

---

## The one hashing rule

There is exactly one place the Arena computes identity and exactly one algorithm. Every Arena-owned hash is:

```
sha256(canonical_json(payload))   →   lowercase 64-hex
```

This is the design non-negotiable from `DATA_MANAGEMENT.md` §1.3 / §2: one algorithm, one canonicalization, so two semantically-equal payloads always serialize byte-identically and therefore hash identically. Everything in `identity/hashing.py` and `identity/pipeline_dag.py` is a specialization of this rule.

The module-level constants pin the contract:

```python
HASH_ALGO = "sha256"
HEX_LEN   = 64
```

`is_hash(value)` returns `True` only for a lowercase 64-hex string; `short(value, n=12)` truncates a hash for human-facing labels (never for keys).

## `canonical_json` and `sha256_hex`

`canonical_json` is `json.dumps` with the strictest reproducibility settings:

```python
def canonical_json(payload: Any) -> str:
    return json.dumps(
        payload,
        sort_keys=True,            # key order cannot affect the bytes
        separators=(",", ":"),     # most compact; no incidental whitespace
        ensure_ascii=False,        # UTF-8 text, not \uXXXX escapes
        allow_nan=False,           # NaN/Infinity are not valid JSON — rejected
    )
```

`allow_nan=False` is load-bearing: `NaN`/`Infinity` are not valid JSON and would break cross-language reproducibility, so a payload containing them raises rather than producing a non-portable hash.

`sha256_hex` is the lowercase 64-hex SHA-256 of a `str` (UTF-8 encoded) or `bytes`:

```python
def sha256_hex(data: str | bytes) -> str: ...
```

## `fingerprint`

`fingerprint` is the composition of the two above — the Arena-owned content identity for any dimension whose canonical spec is a dict or list (operators, params, score-computation specs, dataset variants, and the pipeline-DAG node signatures):

```python
def fingerprint(payload: Any) -> str:
    return sha256_hex(canonical_json(payload))
```

By construction `fingerprint(x) == sha256_hex(canonical_json(x))`. Because `canonical_json` sorts keys, dict insertion order never changes the result:

```python
from nirs4all_benchmarks.identity import fingerprint

assert fingerprint({"a": 1, "b": 2}) == fingerprint({"b": 2, "a": 1})
```

## `compose_run_condition_hash` — the fixed six-field order

The natural identity of an experimental condition is **not** a re-hash of nested payloads. It is a hash of the six component hashes in a *fixed labelled order*, so the `run_condition_hash` is reproducible from the components alone and a future reordering or addition is detectable rather than silently colliding.

The order is frozen in `identity/hashing.py` (`_RUN_CONDITION_FIELDS`); changing it is a breaking change to every stored `run_condition_hash`:

1. `dataset_variant_hash`
2. `split_instance_hash`
3. `cv_instance_hash`
4. `rng_context_hash`
5. `pipeline_dag_hash`
6. `refit_strategy_hash`

The signature is keyword-only, so callers cannot accidentally swap two component hashes:

```python
from nirs4all_benchmarks.identity import compose_run_condition_hash

rch = compose_run_condition_hash(
    dataset_variant_hash="...",
    split_instance_hash="...",
    cv_instance_hash="...",
    rng_context_hash="...",
    pipeline_dag_hash="...",
    refit_strategy_hash="...",
)
```

The hashed payload labels each component and is versioned, so the digest changes if the schema ever does:

```json
{
  "kind": "run_condition",
  "v": 1,
  "components": [
    ["dataset_variant_hash", "<sha256>"],
    ["split_instance_hash", "<sha256>"],
    ["cv_instance_hash", "<sha256>"],
    ["rng_context_hash", "<sha256>"],
    ["pipeline_dag_hash", "<sha256>"],
    ["refit_strategy_hash", "<sha256>"]
  ]
}
```

`compose_run_condition_hash` is called during ingestion in `ingestion/resolve.py` after every component hash is resolved/recomputed; producer-supplied UUIDs are never used as join keys.

## `export_hash` — the idempotency key

`export_hash(manifest)` is the SHA-256 of the canonical `ArenaRunExport` manifest and is the idempotency key for ingestion. It excludes the `arena_export_hash` field itself, so the hash is a fixed point:

```python
manifest["arena_export_hash"] == export_hash(manifest)
```

Adapters (`adapters/nirs4all_workspace.py`, `adapters/dagml_bundle.py`, `fixtures/generate.py`) stamp the manifest with `manifest["arena_export_hash"] = export_hash(manifest)` at emit time. Ingestion recomputes it and the dedup-check uses `(arena_export_hash, target_collection, arena_schema_version)` so an export ingested twice produces the same store.

## The identity table (recomputed vs. adopted)

This is the `DATA_MANAGEMENT.md` §2 contract between "what the engine emits" and "how the Arena dedups and queries". The right-hand column shows the engine source under the dag-ml-as-engine premise; the **Owner** column states which hashes the Arena **recomputes** itself (`sha256(canonical_json(...))`) versus which it **adopts** as a dimension key from the engine while recording the engine fingerprint it derived from.

| Arena identity | Definition | Engine source | Owner |
|---|---|---|---|
| `dataset_fingerprint` | identity of the data regime | io `schema_fingerprint` (+ `relation_fingerprint`) from the `CoordinatorDataPlanEnvelope` | Adopted (engine) |
| `sample_id` | stable per-sample key | io `SampleId` (`SampleRelationTable`) | Adopted (engine), pseudonymized at ingest |
| `task_hash` | target + encoding identity | io `DatasetSpec` target + `task_type`; optional `target_vector_hash` | Recomputed |
| `dataset_variant_hash` | view/subsample/aggregation of a dataset for a task | over `{dataset_fingerprint, task, variant_spec}` | Recomputed |
| `pipeline_dag_hash` | topology-aware pipeline identity | dag-ml `graph_fingerprint` (normalized) | **Recomputed** (Arena Merkle hash; engine fingerprint recorded only) |
| `cv_instance_hash` | the exact fold assignment | dag-ml `fold_set_fingerprint` | Adopted (engine) |
| `split_instance_hash` | the exact external train/test split | dag-ml `SplitInvocation` fingerprint | Adopted (engine) |
| `rng_context_hash` | seeds + determinism flags | dag-ml `SeedContext.root_seed` + derivation (+ framework seeds) | Recomputed |
| `refit_strategy_hash` | selection + refit policy | dag-ml `SelectionDecision` + refit slot plan | Recomputed |
| `score_computation_hash` | metric impl + scope + aggregation + version | Arena `ScoreComputationSpec` | Recomputed |
| `run_condition_hash` | the natural identity of a condition | `compose(six component hashes)` | **Recomputed** (composition) |
| `arena_export_hash` | idempotency key of an ingested export | SHA-256 of the canonical `ArenaRunExport` manifest | Recomputed (`export_hash`) |

Even when the Arena **adopts** an engine fingerprint as the dimension key (folds, split, dataset, samples), it still **recomputes** its own composed hashes (`run_condition_hash`) so dedup stays correct across producers that may emit slightly different fingerprints. `pipeline_dag_hash` is deliberately the strongest case of "recompute, do not trust": the Arena computes its own self-contained Merkle hash and only *records* the engine's `graph_fingerprint` for verification/drift — see below.

## `pipeline_dag_hash` in depth

`PERSISTENCE_FORMATS.md` §5.3 sets three goals for the pipeline identity: it must be (a) topology-aware, (b) dedup "same pipeline, different syntax", and (c) be identical whether a run came from nirs4all or native dag-ml. The doc's *target* is to equal dag-ml's `graph_fingerprint` by lowering through `dag-ml-py` — but that requires the wheel at ingest time **and** exact normalization parity, a contract to earn rather than assume.

So the Arena owns a **self-contained canonical identity** that needs no producer library. The entry point is:

```python
from nirs4all_benchmarks.identity import compute_pipeline_dag_hash

identity = compute_pipeline_dag_hash(
    source,                              # GraphSpec dict, Arena nodes projection,
                                         # {"pipeline"|"steps": [...]}, or a linear list
    engine_graph_fingerprint=None,       # dag-ml graph_fingerprint — recorded, never keyed
    steps_for_identity_hash=None,        # original nirs4all step list — records get_hash
)
```

It returns a frozen `PipelineDagIdentity`:

| Field | Meaning |
|---|---|
| `pipeline_dag_hash` | Arena canonical, topology-aware key (the join key for dedup) |
| `normalized_graph` | the id/order-normalized `GraphSpec` dict the Arena stores as source of truth |
| `is_linear` | `True` when derived from a linear step list (degraded DAG) |
| `engine_graph_fingerprint` | dag-ml `graph_fingerprint` when supplied/computed — recorded, not keyed on |
| `nirs4all_identity_hash` | nirs4all `get_hash` (16-hex) when a step list was available |
| `node_signatures` | `node_id → merkle signature`, re-keyed to normalized ids, for per-node tables |

### Merkle-DAG normalization

The canonical key is a Merkle-DAG hash. Producing it is a four-step normalization (`normalize_graph_spec` + `_merkle_signatures` in `identity/pipeline_dag.py`):

1. **Extract** any accepted shape into `(nodes, edges, is_linear)`. Each node is projected by `_coerce_node` onto only the fields that define identity — `node_id`, `kind`, `operator`, `operator_version`, `params`, `fit_scope`, `branch_path`. Aliases are absorbed here: `id`/`node_id`, `kind`/`role`/`type`, `operator`/`class`/`op`/`model`, an operator that is itself a `{"class", "params"}` dict, and edge endpoints under `src`/`from`/`source`/`from_node` (and `dst`/`to`/`target`/`to_node`).

2. **Graph-id stripping.** The producer's top-level graph id is never read into the canonical form; the output schema tag is the fixed `"arena.graph/1"`. Two graphs that differ only by their container id hash equal.

3. **Positional node renaming.** Nodes are topologically sorted (`_topo_order`, Kahn with deterministic tie-break by node id) and renamed `g0, g1, g2, …` by topo index. Edges are rewritten to the new ids and sorted by `(dst, dst_port, src, src_port)`. The producer's node names therefore have no effect on the bytes.

4. **Merkle signatures.** Each node's signature is `fingerprint({content, inputs, order_sensitive})` where `content` is the position-independent intrinsic content (`kind`, `operator`, `operator_version`, `params`, `fit_scope`) and `inputs` is the list of `[dst_port, upstream_signature]` pairs — so a node's signature depends on its own content **and** the signatures of everything upstream of it. The final `pipeline_dag_hash` is `fingerprint({schema, sorted(node signatures), normalized edges})`.

**Order-insensitive merge handling.** For reducer kinds whose input ordering is not semantically meaningful, the rendered input list is sorted before hashing, so the order in which branches feed the reducer does not change the signature. The order-insensitive set (`_ORDER_INSENSITIVE_KINDS`, lowercased `kind`) is:

```
mean, average, weighted_mean, weightedmean, vote, voting,
bagging, bag, sum, aggregator, ensemble
```

For every other kind — `concat`, stacking meta-features, plain transforms — input order is preserved (the rendered inputs are still sorted by `(port, signature)` for stability, but the signature embeds `order_sensitive=True`, so reordering changes the hash). Malformed graphs do not crash: a cycle falls back to deterministic declaration order in `_topo_order` (validation flags cycles separately).

### Why it is topology-aware and producer-agnostic

- **Topology-aware:** because each Merkle signature folds in upstream signatures, a real topology change (rewiring an edge, inserting/removing a node, changing an operator's params) changes the affected node's signature and therefore the root hash. A node moved earlier or later in the graph with different upstream structure hashes differently.
- **Dedups "same pipeline, different syntax":** graph-id stripping + positional renaming + key-sorted canonical JSON make the hash invariant to node naming, dict-key order, and (for order-insensitive reducers) branch ordering.
- **Producer-agnostic:** the same normalizer accepts a native dag-ml `GraphSpec`, an Arena `pipeline.nodes` projection, a `{"pipeline"|"steps": [...]}` wrapper, or a bare nirs4all step list — and a linear pipeline is lifted into a minimal linear `GraphSpec` and run through the *same* normalizer, so a pipeline and its DAG-equivalent converge on one key. No producer library is required at ingest.

### Recorded secondary ids

The canonical Merkle hash is the only join key. Two other ids are *recorded* alongside it for verification and lineage, never used to dedup:

- **`engine_graph_fingerprint`** — any dag-ml `graph_fingerprint` carried in the export (or computed via `dag-ml-py` when installed). Recorded so the Arena can verify/track drift against the engine, but not keyed on, so dedup stays correct across producers that may or may not ship it. It is stored as a column on `pipeline_dags` (see `store/queries.py`).
- **`nirs4all_identity_hash`** — nirs4all's `get_hash`, i.e. `sha256(canonical_json(step_list))[:16]` (16 hex, matching `pipeline_config.py:get_hash`, `IDENTITY_HASH_LENGTH = 16`). Reproduced by `nirs4all_identity_hash(steps)` and recorded as the v0 lineage id. It is set when `steps_for_identity_hash` is passed, or when the source is itself a linear list.

### Linear-list lifting

A bare nirs4all step list is lifted into a minimal linear `GraphSpec` by `_extract_graph`: each step becomes a node, consecutive steps are connected `out → in`, and `is_linear=True` is flagged. Bare operators (a dotted path or string with no dict) become a `kind="transform"` node with that string as `operator`. Wrappers `{"pipeline": [...]}` / `{"steps": [...]}` recurse into the same path. An Arena projection that has `nodes` but no `edges` is also treated as a linear chain in node order. Because the lifted graph then goes through the identical Merkle normalizer, a linear pipeline and the equivalent explicit linear DAG produce the same `pipeline_dag_hash`.

### Worked example: id-invariance and merge-order-invariance

```python
from nirs4all_benchmarks.identity import compute_pipeline_dag_hash

# (1) ID-INVARIANCE: same topology, different producer node names → same key.
graph_a = {
    "id": "producer-graph-001",                       # graph id is stripped
    "nodes": [
        {"node_id": "snv",  "kind": "transform", "operator": "SNV"},
        {"node_id": "pls",  "kind": "model",     "operator": "PLS",
         "params": {"n_components": 10}},
    ],
    "edges": [{"src": "snv", "dst": "pls"}],
}
graph_b = {
    "id": "another-graph-999",                         # different container id
    "nodes": [
        {"node_id": "step_preprocess", "kind": "transform", "operator": "SNV"},
        {"node_id": "regressor",       "kind": "model",     "operator": "PLS",
         "params": {"n_components": 10}},
    ],
    "edges": [{"src": "step_preprocess", "dst": "regressor"}],
}

assert compute_pipeline_dag_hash(graph_a).pipeline_dag_hash \
    == compute_pipeline_dag_hash(graph_b).pipeline_dag_hash

# (2) MERGE-ORDER-INVARIANCE: an order-insensitive reducer ("mean") with its two
#     branches fed in swapped order → same key.
def two_branch_mean(branch_order):
    return {
        "nodes": [
            {"node_id": "a", "kind": "model", "operator": "PLS"},
            {"node_id": "b", "kind": "model", "operator": "Ridge"},
            {"node_id": "m", "kind": "mean",  "operator": "Mean"},   # order-insensitive
        ],
        "edges": [
            {"src": branch_order[0], "dst": "m"},
            {"src": branch_order[1], "dst": "m"},
        ],
    }

assert compute_pipeline_dag_hash(two_branch_mean(["a", "b"])).pipeline_dag_hash \
    == compute_pipeline_dag_hash(two_branch_mean(["b", "a"])).pipeline_dag_hash

# A real topology change (a different reducer kind, or different params) does change it.
concat = two_branch_mean(["a", "b"])
concat["nodes"][2] = {"node_id": "m", "kind": "concat", "operator": "Concat"}  # order-sensitive
assert compute_pipeline_dag_hash(concat).pipeline_dag_hash \
    != compute_pipeline_dag_hash(two_branch_mean(["a", "b"])).pipeline_dag_hash
```

To inspect the canonical identity of a real `.n4a` bundle (weights stripped) from the CLI:

```bash
n4a-benchmarks inspect-n4a path/to/model.n4a
# pipeline_dag_hash: <64-hex>
# nirs4all_identity_hash: <16-hex>
```
