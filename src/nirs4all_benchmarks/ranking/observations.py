"""Bounded primary-key observation reads shared by native analysis consumers."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from nirs4all_benchmarks.store import ArenaStore

# This is a source-scan cap, distinct from each estimator's stricter design or
# matrix cap. Overflow is a refusal, never an invitation to fit a truncated set.
SOURCE_OBSERVATION_LIMIT = 100_000


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
                  ci.engine_fold_set_fingerprint, ci.cv_spec_hash, ts.task_type """
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
