# DAG `TrainingOutcome` test fixture (identity and mapping evidence only)

`training_outcome.json` is an unmodified byte copy of a DAG-ML owner
production-path **test fixture**. It is not an executed scientific run, carries
no numerical oracle, and is used here only to test identity binding and report
mapping in `adapters/dagml_bundle.py` (`training_outcome` path).

| Field | Value |
|---|---|
| Origin | `dag-ml/crates/dag-ml-core/tests/fixtures/artifact_facets/native_role/training_outcome.json` |
| Identical copy | `nirs4all-core/bindings/rust/nirs4all/tests/fixtures/native_package_transport_v1/training_outcome.json` (Core `PROVENANCE.json`: "Unmodified DAG native RolePipeline production-path fixture; no new numerical oracle.") |
| SHA-256 | `41b7de234d2a760e75c9fe43a03a0ed147eca679dc2cc830f072659a9b3ee1f1` |
| License | DAG-ML: `CeCILL-2.1 OR AGPL-3.0-or-later` (dag-ml `LICENSING.md`, `Cargo.toml`), the same terms as this repository's code. |

Content used by the tests: one variant `variant:base` with no parameter patches,
a two-fold `FoldSet` (`fold:0`, `fold:1`), a single relation fingerprint, sample-level
score reports (`validation`, `train`, `train_pool`, `final`, and the pooled
`avg`/`w_avg` labels) and the full graph.

Do not edit, regenerate or rebless this file. Tests may mutate a temporary copy
only to check that owner validation refuses it. Dataset, task and variant
identities, execution status, attestations and score context used with it are
explicit test declarations.
