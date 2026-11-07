"""Strict typed models for the sole ArenaRunExport v2 contract.

The authoritative wire schema validates raw input before this strict typed view.
Unknown consumed fields are refused; declared provenance and parameter content
remain explicit free-form containers.
"""

from __future__ import annotations

from typing import Annotated, Any, Literal

from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    SerializerFunctionWrapHandler,
    field_validator,
    model_serializer,
    model_validator,
)

SEED_MAX = 2**64 - 1
COUNT_MAX = 2**63 - 1

Visibility = Literal["public", "restricted", "private", "anonymized", "unreported"]
TaskType = Literal["regression", "binary", "multiclass", "multilabel"]
Scope = Literal["fold", "cv", "refit", "test", "view", "unreported"]
Partition = Literal["train", "validation", "test", "final", "unreported"]
Status = Literal["ok", "failed", "cancelled", "unreported"]


class _Block(BaseModel):
    model_config = ConfigDict(extra="forbid", strict=True)


class ProducerBlock(_Block):
    capsule: str = "unreported"
    source_schema_version: int | str | None = None
    nirs4all_version: str | None = None
    dag_ml_version: str | None = None
    dag_ml_data_version: str | None = None
    io_version: str | None = None


class DatasetProducerBlock(_Block):
    io_version: str | None = None


class DatasetBlock(_Block):
    dataset_fingerprint: str
    schema_fingerprint: str | None = None
    relation_fingerprint: str | None = None
    plan_fingerprint: str | None = None
    dataset_card: dict[str, Any] = Field(default_factory=dict)
    dataset_spec_ref: Any | None = None
    visibility: Visibility = "unreported"
    visibility_source: Literal["producer_reported", "unreported", "local_policy"] | None = None
    n_samples: int | None = Field(default=None, ge=0, le=COUNT_MAX)
    n_features: int | None = Field(default=None, ge=0, le=COUNT_MAX)
    producer: DatasetProducerBlock | None = None

    @model_validator(mode="before")
    @classmethod
    def require_declared_visibility(cls, value: Any) -> Any:
        if (isinstance(value, dict) and value.get("visibility_source") == "producer_reported"
                and "visibility" not in value):
            raise ValueError("producer-reported visibility requires an explicit value")
        return value


class TaskBlock(_Block):
    task_hash: str
    task_type: TaskType | None = None
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
    kind: str | None = None
    role: str = "unreported"
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
    method: str = "unreported"
    params: dict[str, Any] = Field(default_factory=dict)
    partition_summary: dict[str, Any] = Field(default_factory=dict)


class CVBlock(_Block):
    cv_spec_hash: str | None = None
    cv_instance_hash: str | None = None
    fold_set_fingerprint: str | None = None
    """Declared DAG canonical ``fold_set_fingerprint`` of the FoldSet that scored the CV observations.

    A submitted value is an adopted, unverified producer claim; only the DAG
    ``training_outcome`` adapter path computes it through the owner API. It
    requires ``dataset.relation_fingerprint`` and is omitted from every
    serialization when null, so exports without it keep their existing hash.
    """
    method: str = "unreported"
    n_folds: int | None = Field(default=None, le=COUNT_MAX)
    n_repeats: Annotated[int, Field(json_schema_extra={"minimum": 1})] | None = Field(default=None, le=COUNT_MAX)
    within_train_only: bool | None = None
    nested: bool | None = None

    @model_serializer(mode="wrap")
    def _omit_null_fold_set(self, handler: SerializerFunctionWrapHandler) -> dict[str, Any]:
        """Serialize every field, omitting only a null ``fold_set_fingerprint``.

        This keeps every ``model_dump``/JSON output, and therefore the v2 export hash,
        byte-identical for exports that do not declare a fold set.
        """
        data: dict[str, Any] = handler(self)
        if self.fold_set_fingerprint is None:
            data.pop("fold_set_fingerprint", None)
        return data


class RNGBlock(_Block):
    rng_context_hash: str | None = None
    root_seed: int | None = Field(default=None, ge=0, le=SEED_MAX)
    derivation: str | None = None
    framework_seeds: dict[str, Any] = Field(default_factory=dict)
    determinism_flags: dict[str, Any] = Field(default_factory=dict)


class RefitBlock(_Block):
    refit_strategy_hash: str | None = None
    strategy: str = "unreported"
    selection_scope: str | None = None
    train_scope: str | None = None
    selected_variant_id: str | None = None
    params: dict[str, Any] = Field(default_factory=dict)


class ExecutionBlock(_Block):
    owner_pipeline_name: str | None = None
    execution_id: str | None = None
    status: Status = "unreported"
    terminal_status: Literal["completed", "incomplete", "failed", "cancelled"] | None = None
    time_ms: float | None = None
    peak_mem_mb: float | None = None
    os: str | None = None
    hardware: str | None = None
    failure_code: str | None = None
    failure_message: str | None = None


class LeakageAttestation(_Block):
    oof_enforced: bool | None = None
    group_leakage_checked: bool | None = None
    nested_cv_safe: bool | None = None
    unsafe_flags: list[str] | None = None


class ScoreObservation(_Block):
    metric_name: str = Field(min_length=1)
    metric_value: float | None
    direction: Literal["min", "max"] | None
    direction_source: Literal["producer_reported", "registry", "unreported"]
    scope: Scope | None
    partition: Partition | None
    aggregation_level: Literal["sample", "group", "target", "unreported"] | None
    fold_id: str | None
    source_fold_id: str | None
    metric_unit: str | None = None
    n_samples: int | None = Field(default=None, ge=0, le=COUNT_MAX)
    coverage: float | None = None
    origin: Literal["producer_reported", "recomputed"] | None = None

    @field_validator("metric_value", mode="before")
    @classmethod
    def exact_float(cls, value: Any) -> Any:
        if value is not None and type(value) is not float:
            raise ValueError("metric_value must be a float or null")
        return value


class ScoresBlock(_Block):
    score_computation_hash: str | None = None
    score_version: str = "unreported"
    metric_implementation: str | None = None
    aggregation_policy: str | None = None
    expected_fold_count: int | None = Field(default=None, ge=1, le=COUNT_MAX)
    completed_fold_count: int | None = Field(default=None, ge=0, le=COUNT_MAX)
    origin: Literal["producer_reported", "recomputed"] | None = None
    observations: list[ScoreObservation] = Field(default_factory=list)


class ResidualsBlock(_Block):
    ref: str | None = None
    key: Literal["sample_id", "positional"] | None = None
    pseudonymized: bool | None = None
    publishable: dict[str, bool] = Field(default_factory=dict)
    inline: list[dict[str, Any]] | None = None
    """Optional inline residual rows (used by fixtures / small runs / tests)."""


class ProvenanceBlock(_Block):
    model_config = ConfigDict(extra="allow", strict=True)
    historical_source: dict[str, Any] | None = None
    frozen_source: dict[str, Any] | None = None
    prov_jsonld: str | None = None
    ro_crate: str | None = None
    openlineage: str | None = None


class IdentityBasis(_Block):
    kind: Literal["owner_reported", "arena_derived", "source_declared_unverified"]
    inputs: dict[str, Any] = Field(default_factory=dict)
    owner: str | None = None

    @model_validator(mode="after")
    def derived_inputs(self) -> IdentityBasis:
        if self.kind == "arena_derived" and not self.inputs:
            raise ValueError("Arena-derived identity requires declared inputs")
        return self


class IdentityBases(_Block):
    dataset_fingerprint: IdentityBasis
    task_hash: IdentityBasis
    dataset_variant_hash: IdentityBasis


class ArenaRunExport(_Block):
    """One weights-free, content-addressed export per ``execution``."""

    arena_export_schema_version: Literal[2]
    arena_export_hash: str | None = None
    producer: ProducerBlock = Field(default_factory=ProducerBlock)
    identity_basis: IdentityBases
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

    @field_validator("arena_export_schema_version", mode="before")
    @classmethod
    def exact_version(cls, value: Any) -> Any:
        if type(value) is not int or value != 2:
            raise ValueError("unsupported_export_version")
        return value

    def to_manifest(self) -> dict[str, Any]:
        """The sole v2 hash domain: every normalized field except the self-hash."""
        return self.model_dump(mode="json", exclude={"arena_export_hash"})
