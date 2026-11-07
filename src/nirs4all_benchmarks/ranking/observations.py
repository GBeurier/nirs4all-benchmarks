"""Bounded primary-key observation reads shared by native analysis consumers."""

from __future__ import annotations

import json
from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from typing import Any

from nirs4all_benchmarks.ingestion.resolve import fold_set_cv_instance_hash
from nirs4all_benchmarks.store import ArenaStore

# This is a source-scan cap, distinct from each estimator's stricter design or
# matrix cap. Overflow is a refusal, never an invitation to fit a truncated set.
SOURCE_OBSERVATION_LIMIT = 100_000

CV_IDENTITY_SCOPES = frozenset({"cv", "fold"})
BASE_COMPARISON_IDENTITIES = (
    "dataset_fingerprint",
    "dataset_variant_hash",
    "pipeline_dag_hash",
    "task_hash",
    "task_type",
    "score_computation_hash",
)
MIXED_CV_FOLD_GRANULARITY = "mixed_cv_fold_granularity_requires_filter"


def _actual_cv_identity_bound(row: Mapping[str, Any]) -> bool:
    """Accept a stored fold-set identity only when its run-condition key recomputes from it."""
    try:
        summary = json.loads(row["fold_summary_json"] or "{}")
    except (TypeError, ValueError):
        return False
    relation = summary.get("relation_fingerprint") if isinstance(summary, dict) else None
    if not isinstance(relation, str) or not row["cv_spec_hash"]:
        return False
    return bool(
        row["cv_instance_hash"]
        == fold_set_cv_instance_hash(row["cv_spec_hash"], row["engine_fold_set_fingerprint"], relation)
    )


def comparison_identity_reason(
    row: Mapping[str, Any], *, scope: str, partition: str, cv_bindings: dict[str, bool]
) -> str | None:
    """Return why a row lacks matched-comparison identity, or ``None`` when it is complete.

    A positively bound DAG fold set replaces the actual outer-split identity only for
    CV/fold validation rows: it binds the CV universe and each fold's validation
    members. Train (relation-narrowed), refit, test, view and final rows still need
    an actual outer-split identity, which no current producer reports. ``cv_bindings``
    caches the binding check once per distinct CV instance.
    """
    fold_set_scoped = scope in CV_IDENTITY_SCOPES and partition == "validation"
    required = BASE_COMPARISON_IDENTITIES if fold_set_scoped else (*BASE_COMPARISON_IDENTITIES, "split_indices_hash")
    if not all(row[key] for key in required):
        return "missing_comparison_identity"
    if scope in CV_IDENTITY_SCOPES:
        if not row["engine_fold_set_fingerprint"]:
            return "missing_actual_cv_identity"
        key = row["cv_instance_hash"]
        if key not in cv_bindings:
            cv_bindings[key] = _actual_cv_identity_bound(row)
        if not cv_bindings[key]:
            return "actual_cv_identity_requires_reingestion"
    if scope == "fold" and row["fold_id"] is None:
        return "missing_fold_identity"
    if scope in {"refit", "test"} and not row["refit_strategy_hash"]:
        return "missing_refit_identity"
    return None


def cv_fold_granularity(scope: str, rows: Iterable[Mapping[str, Any]]) -> str | None:
    """Name the fold granularity of an accepted CV/fold cohort.

    Returns ``per_fold`` for real fold rows, ``unfolded`` for rows without a fold id,
    ``MIXED_CV_FOLD_GRANULARITY`` when both occur, and ``None`` outside CV scopes.
    Per-fold rows are never the producer's pooled ``avg``/``w_avg`` OOF score.
    """
    if scope not in CV_IDENTITY_SCOPES:
        return None
    folded = {row["fold_id"] is not None for row in rows}
    if folded == {True, False}:
        return MIXED_CV_FOLD_GRANULARITY
    return "per_fold" if True in folded else "unfolded"


@dataclass(frozen=True)
class ObservationBatch:
    rows: list[dict[str, Any]]
    source_count: int
    limit_exceeded: bool
    limit: int

    @property
    def scan(self) -> dict[str, Any]:
        return {"limit": self.limit, "limit_exceeded": self.limit_exceeded, "count_is_lower_bound": self.limit_exceeded}

    @property
    def limit_warning(self) -> str:
        return (
            f"Source observation count is a lower bound: at least {self.source_count}; "
            f"the source limit of {self.limit} was exceeded. No truncated cohort was fitted."
        )


def original_observations(
    store: ArenaStore,
    *,
    metric: str,
    scope: str,
    partition: str,
    dataset: str | None = None,
    task: str | None = None,
    collection: str | None = None,
    protocol: str | None = None,
    aggregation_level: str | None = None,
) -> ObservationBatch:
    """Fetch each score observation once, with no value-based join or fanout.

    A narrow ID scan establishes the source cap before any wide records are
    materialized. The second query repeats the hard bound, so a concurrent writer
    cannot turn a small preflight into an unbounded fetch or a partial fit.
    """
    where = [
        "mo.metric_name=?",
        "ss.scope=?",
        "mo.partition=?",
        "NOT EXISTS (SELECT 1 FROM score_sets newer WHERE newer.supersedes_score_set_id=ss.score_set_id "
        "AND newer.validity_status='valid')",
        # Sole-v2 rankability, as in v_ranked_metrics: unknown identity or attestations never rank.
        "e.identity_rankable=1",
        "e.oof_enforced=1",
        "e.group_leakage_checked=1",
        "e.nested_cv_safe=1",
        "e.unsafe_flags_json='[]'",
    ]
    args: list[Any] = [metric, scope, partition]
    for column, value in (
        ("rc.dataset_fingerprint", dataset),
        ("rc.task_hash", task),
        ("rc.collection_id", collection),
        ("ss.score_computation_hash", protocol),
        ("mo.aggregation_level", aggregation_level),
    ):
        if value is not None:
            where.append(f"{column}=?")
            args.append(value)
    joins = """FROM metric_observations mo
        JOIN score_sets ss ON ss.score_set_id=mo.score_set_id
        JOIN executions e ON e.execution_hash=ss.execution_hash
        JOIN run_conditions rc ON rc.run_condition_hash=e.run_condition_hash"""
    bounded = " WHERE " + " AND ".join(where) + " ORDER BY mo.metric_observation_id LIMIT ?"
    limit = SOURCE_OBSERVATION_LIMIT
    bound_args = [*args, limit + 1]
    ids = store.conn.execute("SELECT mo.metric_observation_id " + joins + bounded, bound_args).fetchmany(limit + 1)
    if len(ids) > limit:
        return ObservationBatch([], limit + 1, True, limit)
    rows = store.query(
        """SELECT mo.metric_observation_id, mo.metric_name, mo.metric_value, mo.direction,
                  mo.fold_id, mo.partition, mo.aggregation_level, mo.n_samples,
                  ss.score_set_id, ss.scope AS score_scope, ss.validity_status AS score_validity,
                  ss.score_computation_hash, e.execution_hash, e.run_condition_hash,
                  e.validity_status AS execution_validity, e.status AS execution_status,
                  e.unsafe_flags_json, rc.collection_id, rc.pipeline_dag_hash,
                  rc.dataset_fingerprint, rc.dataset_variant_hash, rc.task_hash,
                  rc.split_instance_hash, rc.cv_instance_hash, rc.refit_strategy_hash,
                  si.split_indices_hash, si.split_spec_hash,
                  ci.engine_fold_set_fingerprint, ci.cv_spec_hash, ci.fold_summary_json, ts.task_type """
        + joins
        + """ LEFT JOIN split_instances si ON si.split_instance_hash=rc.split_instance_hash
             LEFT JOIN cv_instances ci ON ci.cv_instance_hash=rc.cv_instance_hash
             LEFT JOIN task_specs ts ON ts.task_hash=rc.task_hash"""
        + bounded,
        bound_args,
    )
    if len(rows) > limit:
        return ObservationBatch([], limit + 1, True, limit)
    return ObservationBatch(rows, len(rows), False, limit)


def pipeline_rows(store: ArenaStore, table: str, pipeline_ids: set[str]) -> list[dict[str, Any]]:
    """Read only selected pipeline metadata, with portable SQLite batch sizes."""
    if table not in {"pipeline_dags", "pipeline_nodes", "pipeline_edges"}:
        raise ValueError("unsupported pipeline metadata table")
    ids = sorted(pipeline_ids)
    rows = []
    for first in range(0, len(ids), 500):
        chunk = ids[first : first + 500]
        placeholders = ",".join("?" for _ in chunk)
        rows.extend(store.query(f"SELECT * FROM {table} WHERE pipeline_dag_hash IN ({placeholders})", chunk))
    return rows
