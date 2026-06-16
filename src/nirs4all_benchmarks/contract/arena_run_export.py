"""Typed pydantic models for the ``ArenaRunExport`` v1 manifest.

These mirror ``manifest.json`` in DATA_MANAGEMENT.md §4 exactly. Blocks are
``extra="allow"`` so a future producer can add fields without breaking ingestion,
but every field the Arena keys on is validated. ``ArenaRunExport`` is the typed
view; the authoritative wire contract is the JSON Schema in ``contract/schema``.
"""

from __future__ import annotations

from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field

from nirs4all_benchmarks import ARENA_EXPORT_SCHEMA_VERSION

Visibility = Literal["public", "restricted", "private", "anonymized"]
TaskType = Literal["regression", "binary", "multiclass", "multilabel"]
Scope = Literal["fold", "cv", "refit", "test", "view"]
Partition = Literal["train", "validation", "test", "final"]
Status = Literal["ok", "failed", "cancelled"]


class _Block(BaseModel):
    model_config = ConfigDict(extra="allow")


class ProducerBlock(_Block):
    capsule: str = "python"
    nirs4all_version: str | None = None
    dag_ml_version: str | None = None
    dag_ml_data_version: str | None = None
    io_version: str | None = None


class DatasetBlock(_Block):
    dataset_fingerprint: str
    schema_fingerprint: str | None = None
    relation_fingerprint: str | None = None
    plan_fingerprint: str | None = None
    dataset_card: dict[str, Any] = Field(default_factory=dict)
    dataset_spec_ref: Any | None = None
    visibility: Visibility = "public"
    n_samples: int | None = None
    n_features: int | None = None


class TaskBlock(_Block):
    task_hash: str
    task_type: TaskType = "regression"
    target_name: str | None = None
    target_unit: str | None = None
    target_hash: str | None = None
    encoding: dict[str, Any] = Field(default_factory=dict)


class DatasetVariantBlock(_Block):
    dataset_variant_hash: str
    variant_spec: dict[str, Any] = Field(default_factory=dict)
    sample_manifest_hash: str | None = None


class PipelineNode(_Block):
    node_id: str
    role: str = "transform"
    operator: str | None = None
    operator_version: str | None = None
    params: dict[str, Any] = Field(default_factory=dict)
    branch_path: list[Any] = Field(default_factory=list)
    fit_scope: str | None = None
    source_id: str | None = None
    model_family: str | None = None


class PipelineBlock(_Block):
    pipeline_dag_hash: str | None = None
    """If absent, the Arena computes it from ``graph`` / ``nodes`` at ingest."""
    controller_fingerprint: str | None = None
    engine_graph_fingerprint: str | None = None
    nirs4all_identity_hash: str | None = None
    graph_ref: str | None = None
    graph: dict[str, Any] | None = None
    """The canonical ``GraphSpec`` (source of truth). May be inlined or referenced."""
    nodes: list[PipelineNode] = Field(default_factory=list)
    human_label: str | None = None


class SplitBlock(_Block):
    split_spec_hash: str | None = None
    split_instance_hash: str | None = None
    method: str = "none"
    params: dict[str, Any] = Field(default_factory=dict)
    partition_summary: dict[str, Any] = Field(default_factory=dict)


class CVBlock(_Block):
    cv_spec_hash: str | None = None
    cv_instance_hash: str | None = None
    method: str = "none"
    n_folds: int | None = None
    n_repeats: int | None = None
    within_train_only: bool = True
    nested: bool = False


class RNGBlock(_Block):
    rng_context_hash: str | None = None
    root_seed: int | None = None
    derivation: str | None = None
    framework_seeds: dict[str, Any] = Field(default_factory=dict)
    determinism_flags: dict[str, Any] = Field(default_factory=dict)


class RefitBlock(_Block):
    refit_strategy_hash: str | None = None
    strategy: str = "none"
    selection_scope: str | None = None
    train_scope: str | None = None
    selected_variant_id: str | None = None
    params: dict[str, Any] = Field(default_factory=dict)


class ExecutionBlock(_Block):
    execution_id: str | None = None
    status: Status = "ok"
    time_ms: float | None = None
    peak_mem_mb: float | None = None
    os: str | None = None
    hardware: str | None = None
    failure_code: str | None = None
    failure_message: str | None = None


class LeakageAttestation(_Block):
    oof_enforced: bool = False
    group_leakage_checked: bool = False
    nested_cv_safe: bool = True
    unsafe_flags: list[str] = Field(default_factory=list)


class ScoreObservation(_Block):
    metric_name: str
    metric_value: float | None
    direction: Literal["min", "max"] = "min"
    scope: Scope = "cv"
    fold_id: str | None = None
    partition: Partition = "validation"
    aggregation_level: str = "sample"
    metric_unit: str | None = None
    n_samples: int | None = None
    coverage: float | None = None


class ScoresBlock(_Block):
    score_computation_hash: str | None = None
    score_version: str = "1.0"
    metric_implementation: str | None = None
    aggregation_policy: str | None = None
    observations: list[ScoreObservation] = Field(default_factory=list)


class ResidualsBlock(_Block):
    ref: str | None = None
    key: Literal["sample_id", "positional"] = "sample_id"
    pseudonymized: bool = False
    publishable: dict[str, bool] = Field(default_factory=dict)
    inline: list[dict[str, Any]] | None = None
    """Optional inline residual rows (used by fixtures / small runs / tests)."""


class ProvenanceBlock(_Block):
    prov_jsonld: str | None = None
    ro_crate: str | None = None
    openlineage: str | None = None


class ArenaRunExport(_Block):
    """One weights-free, content-addressed export per ``execution``."""

    arena_export_schema_version: int = ARENA_EXPORT_SCHEMA_VERSION
    arena_export_hash: str | None = None
    producer: ProducerBlock = Field(default_factory=ProducerBlock)
    dataset: DatasetBlock
    task: TaskBlock
    dataset_variant: DatasetVariantBlock
    pipeline: PipelineBlock
    split: SplitBlock = Field(default_factory=SplitBlock)
    cv: CVBlock = Field(default_factory=CVBlock)
    rng: RNGBlock = Field(default_factory=RNGBlock)
    refit: RefitBlock = Field(default_factory=RefitBlock)
    run_condition_hash: str | None = None
    execution: ExecutionBlock = Field(default_factory=ExecutionBlock)
    leakage_attestation: LeakageAttestation = Field(default_factory=LeakageAttestation)
    scores: ScoresBlock = Field(default_factory=ScoresBlock)
    residuals: ResidualsBlock = Field(default_factory=ResidualsBlock)
    provenance: ProvenanceBlock | None = None

    def to_manifest(self) -> dict[str, Any]:
        """Canonical, JSON-ready dict (``None`` excluded for stable hashing)."""
        return self.model_dump(mode="json", exclude_none=True)
