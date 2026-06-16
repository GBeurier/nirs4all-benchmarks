"""The Arena ingestion contract — ``ArenaRunExport`` v1.

This is the one weights-free, content-addressed bundle the Arena ingests, one per
``execution`` (DATA_MANAGEMENT.md §4). The engine (dag-ml driving nirs4all, with
io supplying the dataset) emits it; the Arena ingests it with **zero** engine-
internal knowledge. The JSON Schema in ``contract/schema/`` is the authoritative
freeze contract; the pydantic models here are the typed, validated Python view.
"""

from __future__ import annotations

from nirs4all_benchmarks.contract.arena_run_export import (
    ArenaRunExport,
    CVBlock,
    DatasetBlock,
    DatasetVariantBlock,
    ExecutionBlock,
    LeakageAttestation,
    PipelineBlock,
    PipelineNode,
    ProducerBlock,
    RefitBlock,
    ResidualsBlock,
    RNGBlock,
    ScoreObservation,
    ScoresBlock,
    SplitBlock,
    TaskBlock,
)
from nirs4all_benchmarks.contract.schema import (
    ARENA_RUN_EXPORT_SCHEMA,
    RESIDUALS_PARQUET_COLUMNS,
    validate_manifest,
)

__all__ = [
    "ARENA_RUN_EXPORT_SCHEMA",
    "RESIDUALS_PARQUET_COLUMNS",
    "ArenaRunExport",
    "CVBlock",
    "DatasetBlock",
    "DatasetVariantBlock",
    "ExecutionBlock",
    "LeakageAttestation",
    "PipelineBlock",
    "PipelineNode",
    "ProducerBlock",
    "RNGBlock",
    "RefitBlock",
    "ResidualsBlock",
    "ScoreObservation",
    "ScoresBlock",
    "SplitBlock",
    "TaskBlock",
    "validate_manifest",
]
