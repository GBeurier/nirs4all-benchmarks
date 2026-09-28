"""Strict wire contract for artifact-driven cross-runtime qualification."""

from __future__ import annotations

import math
from datetime import datetime
from pathlib import Path
from typing import Annotated, Any, Literal

from pydantic import BaseModel, ConfigDict, Field, StringConstraints, field_validator, model_validator

QUALIFICATION_MANIFEST_SCHEMA_VERSION: Literal["nirs4all.qualification-manifest.v1"] = (
    "nirs4all.qualification-manifest.v1"
)
QUALIFICATION_REPORT_SCHEMA_VERSION = "nirs4all.qualification-report.v1"
ADAPTER_PROTOCOL_VERSION: Literal["nirs4all.qualification-adapter.v1"] = "nirs4all.qualification-adapter.v1"

ComponentName = Literal["python_oracle", "core_rust", "studio_sidecar", "web_wasm"]
Disposition = Literal["passed", "refused", "failed"]
Sha256 = Annotated[str, StringConstraints(pattern=r"^[0-9a-f]{64}$")]
GitSha = Annotated[str, StringConstraints(pattern=r"^[0-9a-f]{40}$")]

COMPONENTS: tuple[ComponentName, ...] = (
    "python_oracle",
    "core_rust",
    "studio_sidecar",
    "web_wasm",
)


class _StrictModel(BaseModel):
    model_config = ConfigDict(extra="forbid")


class FileReference(_StrictModel):
    """An explicit file path with its expected content digest."""

    path: Path
    sha256: Sha256

    @field_validator("path")
    @classmethod
    def require_absolute_path(cls, value: Path) -> Path:
        if not value.is_absolute():
            raise ValueError("path must be absolute; discovery relative to a checkout is forbidden")
        return value


class ArtifactReference(FileReference):
    """A named input consumed by a component adapter."""

    role: str = Field(min_length=1, pattern=r"^[a-z][a-z0-9_.-]*$")


class ScenarioReference(_StrictModel):
    """Content-addressed scenario definition shared by every runtime."""

    scenario_id: str = Field(min_length=1)
    definition: FileReference


class Tolerances(_StrictModel):
    """Numerical comparison tolerances against the Python oracle."""

    absolute: float = Field(ge=0)
    relative: float = Field(ge=0)

    @field_validator("absolute", "relative")
    @classmethod
    def require_finite(cls, value: float) -> float:
        if not math.isfinite(value):
            raise ValueError("tolerance must be finite")
        return value


class ExecutableComponent(_StrictModel):
    """One runtime reached only through a verified executable adapter."""

    component: ComponentName
    adapter: Literal["stdio-json-v1"] = "stdio-json-v1"
    executable: FileReference
    artifacts: list[ArtifactReference] = Field(min_length=1)
    version: str = Field(min_length=1)
    commit_sha: GitSha
    timeout_seconds: float = Field(default=120.0, gt=0, le=3600)

    @model_validator(mode="after")
    def require_unique_roles(self) -> ExecutableComponent:
        roles = [artifact.role for artifact in self.artifacts]
        if len(roles) != len(set(roles)):
            raise ValueError("artifact roles must be unique within a component")
        return self


class RequiredComponent(_StrictModel):
    """An unavailable adapter that must remain a visible release refusal."""

    component: ComponentName
    adapter: Literal["required"]
    refusal_reason: str = Field(min_length=1)


ComponentSpecification = Annotated[
    ExecutableComponent | RequiredComponent,
    Field(discriminator="adapter"),
]


class QualificationManifest(_StrictModel):
    """Inputs for one four-runtime qualification run."""

    schema_version: Literal["nirs4all.qualification-manifest.v1"] = QUALIFICATION_MANIFEST_SCHEMA_VERSION
    scenario: ScenarioReference
    tolerances: Tolerances
    components: list[ComponentSpecification] = Field(min_length=4, max_length=4)

    @model_validator(mode="after")
    def require_every_component_once(self) -> QualificationManifest:
        names = [spec.component for spec in self.components]
        missing = sorted(set(COMPONENTS) - set(names))
        duplicates = sorted(name for name in set(names) if names.count(name) > 1)
        if missing or duplicates:
            raise ValueError(
                "components must contain each required runtime exactly once; "
                f"missing={missing}, duplicates={duplicates}"
            )
        return self


class AdapterRequest(_StrictModel):
    """JSON document delivered on stdin to an executable adapter."""

    protocol: Literal["nirs4all.qualification-adapter.v1"] = ADAPTER_PROTOCOL_VERSION
    component: ComponentName
    declared_version: str
    declared_commit_sha: GitSha
    scenario: ScenarioReference
    tolerances: Tolerances
    artifacts: list[ArtifactReference]


class ComponentOutput(_StrictModel):
    """Single finalized JSON document emitted by an adapter on stdout."""

    protocol: Literal["nirs4all.qualification-adapter.v1"] = ADAPTER_PROTOCOL_VERSION
    component: ComponentName
    scenario_id: str
    version: str = Field(min_length=1)
    commit_sha: GitSha
    completed: bool
    fallback_used: bool
    qualification_refusal_reason: str | None = Field(default=None, min_length=1)
    observations: dict[str, list[float]] = Field(default_factory=dict)
    metrics: dict[str, float] = Field(default_factory=dict)
    timings_ms: dict[str, float] = Field(default_factory=dict)

    @field_validator("observations")
    @classmethod
    def require_finite_observations(cls, value: dict[str, list[float]]) -> dict[str, list[float]]:
        if any(not math.isfinite(item) for series in value.values() for item in series):
            raise ValueError("observations must be finite")
        return value

    @field_validator("metrics")
    @classmethod
    def require_finite_metrics(cls, value: dict[str, float]) -> dict[str, float]:
        if any(not math.isfinite(item) for item in value.values()):
            raise ValueError("metrics must be finite")
        return value

    @field_validator("timings_ms")
    @classmethod
    def require_timings(cls, value: dict[str, float]) -> dict[str, float]:
        if any(not math.isfinite(item) or item < 0 for item in value.values()):
            raise ValueError("timings must be finite and non-negative")
        return value

    @model_validator(mode="after")
    def finalized_output_has_observations(self) -> ComponentOutput:
        if self.completed and self.qualification_refusal_reason is not None:
            raise ValueError("a completed output cannot declare a qualification refusal")
        if self.completed and (not self.observations or any(not values for values in self.observations.values())):
            raise ValueError("a completed output must contain at least one non-empty observation series")
        return self


class FileEvidence(_StrictModel):
    """Expected and observed identity of one qualification input."""

    path: str
    expected_sha256: Sha256
    actual_sha256: Sha256 | None


class ArtifactEvidence(FileEvidence):
    """File evidence with its component-specific role."""

    role: str


class ComponentReport(_StrictModel):
    """Terminal qualification disposition for one required runtime."""

    component: ComponentName
    disposition: Disposition
    executed: bool
    reason: str | None
    version: str | None
    commit_sha: GitSha | None
    executable: FileEvidence | None
    artifacts: list[ArtifactEvidence]
    metrics: dict[str, float]
    timings_ms: dict[str, float]
    observation_sha256: Sha256 | None
    observation_values: int = Field(ge=0)


class ScenarioEvidence(FileEvidence):
    """Scenario identity and the exact JSON definition used by adapters."""

    scenario_id: str
    definition: Any


class QualificationReport(_StrictModel):
    """Evidence-complete terminal report for the four-runtime gate."""

    schema_version: Literal["nirs4all.qualification-report.v1"] = "nirs4all.qualification-report.v1"
    generated_at: datetime
    manifest: FileEvidence
    scenario: ScenarioEvidence
    tolerances: Tolerances
    overall_disposition: Disposition
    components: list[ComponentReport] = Field(min_length=4, max_length=4)
