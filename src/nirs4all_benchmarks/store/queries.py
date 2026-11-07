"""Serving queries — meta-analysis over the normalized store (DATA_MANAGEMENT.md §6).

Every query is a plain SQL join on the §2 content hashes. The store is normalized
precisely so these are cheap: leaderboards/matrices join ``run_conditions ⋈
score_sets ⋈ metric_observations``; operator/parameter effects join the materialized
``pipeline_nodes`` / ``pipeline_node_params`` tables; the residual explorer joins
``residual_sets`` by stable ``sample_id`` so two pipelines compare on the *same*
samples — the capability sample-keying unlocks.

The :class:`Queries` facade is the single read API the service and CLI consume.
"""

from __future__ import annotations

import json
import statistics
from typing import Any

from nirs4all_benchmarks.identity.hashing import is_hash
from nirs4all_benchmarks.scoring import METRIC_DIRECTION
from nirs4all_benchmarks.store.arena_store import ArenaStore

_VALID_SCOPES = {"fold", "cv", "refit", "test", "view"}
_VALID_PARTITIONS = {"train", "validation", "test", "final"}


def _dataset_filter(dataset_fingerprint: str | None) -> tuple[str, list[Any]]:
    """SQL fragment and parameter restricting ``v_run_metrics mo`` rows to one dataset (or none)."""
    if not dataset_fingerprint:
        return "", []
    return " AND mo.dataset_fingerprint = ?", [dataset_fingerprint]


def _safe_scope(scope: str) -> str:
    if scope not in _VALID_SCOPES:
        raise ValueError(f"invalid scope '{scope}'")
    return scope


def _score_partition(scope: str, partition: str | None) -> str:
    """Select one score context; refit has no scientifically generic mean."""
    _safe_scope(scope)
    if partition is None:
        if scope == "refit":
            raise ValueError("score_partition_required_for_refit")
        partition = "test" if scope == "test" else "validation"
    if partition not in _VALID_PARTITIONS:
        raise ValueError("invalid_score_partition")
    return partition


def _context_summary(rows: list[dict[str, Any]], withheld: int = 0) -> dict[str, Any]:
    contexts = sorted({(row["task_hash"], row["metric_unit"]) for row in rows},
                      key=lambda pair: (pair[0], pair[1] is not None, pair[1] or ""))
    return {"status": "withheld" if withheld else "empty" if not contexts else
            "single" if len(contexts) == 1 else "partitioned",
            "contexts": [{"task_hash": task, "metric_unit": unit} for task, unit in contexts[:100]],
            "contexts_truncated": len(contexts) > 100,
            "withheld": {"count": withheld, "reason": "mixed_score_context"}}


class Queries:
    """Read-only meta-analysis facade over an :class:`ArenaStore`."""

    def __init__(self, store: ArenaStore) -> None:
        self.store = store
        self.metrics_view = "v_ranked_metrics"

    def _score_view(
        self, partition: str, *, scope: str, aggregation_level: str = "sample",
        source_fold_id: str | None = None, task_hash: str | None = None
    ) -> str:
        if partition not in _VALID_PARTITIONS:
            raise ValueError("invalid_score_partition")
        source = self.metrics_view
        if aggregation_level not in {"sample", "group", "target"}:
            raise ValueError("invalid_aggregation_level")
        if scope == "view" and source_fold_id is None:
            raise ValueError("source_fold_selection_required_for_view")
        if aggregation_level != "sample" and source_fold_id is None:
            raise ValueError("source_fold_selection_required_for_aggregate")
        if source_fold_id is not None:
            if not isinstance(source_fold_id, str) or "\x00" in source_fold_id:
                raise ValueError("invalid_source_fold_selection")
            if source_fold_id.strip().endswith("_agg") and aggregation_level != "group":
                raise ValueError("aggregate_level_selection_required")
            # Quote the literal, including embedded apostrophes; never interpolate raw SQL.
            label = "source_fold_id = '" + source_fold_id.replace("'", "''") + "'"
        elif scope == "refit" and aggregation_level == "sample":
            label = "source_fold_id = 'final'"
        else:
            label = "source_fold_id IS NULL"
        if task_hash is not None and not is_hash(task_hash):
            raise ValueError("invalid_task_hash")
        task_sql = f" AND v.task_hash = '{task_hash}'" if task_hash is not None else ""
        selection = (f"v.partition = '{partition}' AND v.aggregation_level = '{aggregation_level}' "
                     f"AND v.{label}{task_sql}")
        # The view omits observation IDs and SQLite has no unique constraint for
        # this composite key. Refuse ambiguous legacy rows instead of multiplying them.
        ambiguous = self.store.query_one(
            f"SELECT 1 FROM {source} v WHERE {selection} GROUP BY v.score_set_id, v.metric_name, "
            "v.partition, v.aggregation_level, v.fold_id, v.source_fold_id HAVING COUNT(*) > 1 LIMIT 1"
        )
        if ambiguous:
            raise ValueError("stored_score_context_ambiguous")
        return ("(SELECT v.*, o.metric_unit, "
                "v.task_hash || ':' || COALESCE('u:' || o.metric_unit, 'n') AS score_context "
                f"FROM {source} v JOIN metric_observations o ON o.score_set_id=v.score_set_id "
                "AND o.metric_name=v.metric_name AND o.partition IS v.partition "
                "AND o.aggregation_level IS v.aggregation_level AND o.fold_id IS v.fold_id "
                "AND o.source_fold_id IS v.source_fold_id "
                f"WHERE {selection})")

    def _direction(self, metric: str) -> str:
        if metric.lower() in METRIC_DIRECTION:
            return METRIC_DIRECTION[metric.lower()]
        values = {row["direction"] for row in self.store.query(
            f"SELECT DISTINCT direction FROM {self.metrics_view} WHERE metric_name = ?", (metric,)
        )}
        if len(values) != 1 or None in values:
            raise ValueError("metric_direction_unreported_or_conflicting")
        return values.pop()

    def _selected_contexts(self, metrics_view: str, metric: str, scope: str,
                           dataset_fingerprint: str | None, *,
                           include_quarantined: bool = False) -> list[dict[str, Any]]:
        ds_sql, ds_params = _dataset_filter(dataset_fingerprint)
        validity = "" if include_quarantined else " AND mo.execution_validity='valid'"
        return self.store.query(
            f"SELECT DISTINCT mo.task_hash, mo.metric_unit FROM {metrics_view} mo "
            f"WHERE mo.metric_name=? AND mo.score_scope=?{ds_sql} "
            f"AND mo.score_validity='valid'{validity}", [metric, scope, *ds_params])

    def _evidence(self, execution: dict[str, Any] | None) -> dict[str, Any]:
        from nirs4all_benchmarks.ingestion.metadata_bundle import read_stored_metadata
        from nirs4all_benchmarks.ingestion.submitted_metadata import binding_consistent
        if execution is None:
            return {"status": "metadata_unavailable", "reason": "execution_missing"}
        stored = self.store.get("executions", "execution_hash", execution["execution_hash"])
        if stored is None or stored["arena_export_hash"] != execution["arena_export_hash"]:
            return {"status": "metadata_unavailable", "reason": "stored_binding_inconsistent"}
        execution = stored
        export_hash = execution["arena_export_hash"]
        receipt = self.store.get("metadata_receipts", "arena_export_hash", export_hash)
        if receipt is None:
            return {"status":"metadata_unavailable", "reason":"receipt_unreported"}
        row = self.store.get("submitted_metadata", "arena_export_hash", export_hash)
        if not row or not binding_consistent(execution, row, receipt):
            return {"status": "metadata_unavailable", "reason": "stored_binding_inconsistent"}
        return read_stored_metadata(None, export_hash, stored_json=receipt["receipt_json"])

    @staticmethod
    def _attestations(metadata: dict[str, Any]) -> dict[str, Any]:
        att = metadata.get("leakage_attestation") or {}
        return {name: att.get(name) for name in ("oof_enforced", "group_leakage_checked", "nested_cv_safe")}

    # ── catalogues ─────────────────────────────────────────────────────
    def overview(self) -> dict[str, Any]:
        s = self.store
        valid = s.query_one(f"SELECT COUNT(DISTINCT execution_hash) AS n FROM {self.metrics_view}")
        score_sets = s.query_one(f"SELECT COUNT(DISTINCT score_set_id) AS n FROM {self.metrics_view}")
        assert valid is not None and score_sets is not None  # COUNT without GROUP BY always returns one row.
        return {
            "datasets": s.count("dataset_fingerprints"),
            "dataset_cards": s.count("dataset_cards"),
            "tasks": s.count("task_specs"),
            "pipelines": s.count("pipeline_dags"),
            "operators": s.count("operator_specs"),
            "parameters": s.count("parameter_values"),
            "run_conditions": s.count("run_conditions"),
            "executions": s.count("executions"),
            "valid_executions": valid["n"],
            "unreported_executions": s.count("executions", "status IN ('ok', 'unreported')"),
            "include_unreported": False,
            "quarantined_executions": s.count("executions", "validity_status = 'quarantined'"),
            "score_sets": score_sets["n"],
            "metric_observations": s.count(self.metrics_view),
            "total_metric_observations": s.count("metric_observations"),
            "residual_sets": s.count("residual_sets"),
            "collections": s.count("collections"),
            "metrics": self.metrics_available(),
            "score_scopes": [r["scope"] for r in s.query(
                f"SELECT DISTINCT score_scope AS scope FROM {self.metrics_view} ORDER BY scope"
            )],
            "schema_version": int(s.get_meta("arena_schema_version") or 0),
        }

    def metrics_available(self) -> list[str]:
        rows = self.store.query(f"SELECT DISTINCT metric_name FROM {self.metrics_view} ORDER BY metric_name")
        return [r["metric_name"] for r in rows]

    def collections(self) -> list[dict[str, Any]]:
        return self.store.query("SELECT * FROM collections ORDER BY created_at")

    def datasets(self) -> list[dict[str, Any]]:
        rows = self.store.query(
            """
            SELECT df.dataset_fingerprint, df.privacy_level, df.n_samples, df.n_features, df.task_type,
                   dc.name, dc.domain, dc.modality, dc.signal_type, dc.axis_unit, dc.axis_min, dc.axis_max,
                   (SELECT COUNT(DISTINCT rc.run_condition_hash) FROM run_conditions rc
                      WHERE rc.dataset_fingerprint = df.dataset_fingerprint) AS n_run_conditions
            FROM dataset_fingerprints df
            LEFT JOIN dataset_cards dc ON dc.dataset_card_hash = df.dataset_card_hash
            ORDER BY dc.name, df.dataset_fingerprint
            """
        )
        return rows

    def datasets_api(self) -> list[dict[str, Any]]:
        return [{**row, "card_scope": "first_accepted_fingerprint"} for row in self.datasets()]

    def pipelines(self) -> list[dict[str, Any]]:
        return self.store.query(
            """
            SELECT pd.pipeline_dag_hash, pd.human_label, pd.main_model, pd.n_nodes, pd.is_linear,
                   pd.nirs4all_identity_hash, pd.engine_graph_fingerprint,
                   (SELECT COUNT(DISTINCT rc.run_condition_hash) FROM run_conditions rc
                      WHERE rc.pipeline_dag_hash = pd.pipeline_dag_hash) AS n_run_conditions
            FROM pipeline_dags pd
            ORDER BY pd.main_model, pd.human_label
            """
        )

    def operators(self) -> list[dict[str, Any]]:
        return self.store.query(
            """
            SELECT os.operator_spec_hash, os.entrypoint, os.library, os.version, os.role, os.family,
                   COUNT(DISTINCT pn.pipeline_dag_hash) AS n_pipelines
            FROM operator_specs os
            LEFT JOIN pipeline_nodes pn ON pn.operator_spec_hash = os.operator_spec_hash
            GROUP BY os.operator_spec_hash ORDER BY n_pipelines DESC, os.entrypoint
            """
        )

    def sweepable_parameters(self) -> list[dict[str, Any]]:
        return self.store.query(
            """
            SELECT pv.name, COUNT(DISTINCT pv.param_value_hash) AS n_values,
                   SUM(pv.is_numeric) > 0 AS numeric
            FROM parameter_values pv WHERE pv.is_sweepable = 1
            GROUP BY pv.name ORDER BY n_values DESC, pv.name
            """
        )

    # ── leaderboard ────────────────────────────────────────────────────
    def leaderboard(
        self,
        *,
        metric: str = "rmse",
        scope: str = "cv",
        partition: str | None = None,
        aggregation_level: str = "sample", source_fold_id: str | None = None,
        dataset_fingerprint: str | None = None,
        task_hash: str | None = None,
        collection_id: str | None = None,
        include_quarantined: bool = False,
        limit: int = 200,
    ) -> dict[str, Any]:
        """Configurable leaderboard — no canonical baseline imposed (DESIGN.md §9.2)."""
        partition = _score_partition(scope, partition)
        metrics_view = self._score_view(
            partition, scope=scope, aggregation_level=aggregation_level, source_fold_id=source_fold_id,
            task_hash=task_hash)
        where = ["mo.metric_name = ?", "mo.score_scope = ?", "mo.score_validity = 'valid'"]
        params: list[Any] = [metric, scope]
        if not include_quarantined:
            where.append("mo.execution_validity = 'valid'")
        if partition:
            where.append("mo.partition = ?")
            params.append(partition)
        if dataset_fingerprint:
            where.append("mo.dataset_fingerprint = ?")
            params.append(dataset_fingerprint)
        if task_hash:
            where.append("mo.task_hash = ?")
            params.append(task_hash)
        if collection_id:
            where.append("mo.collection_id = ?")
            params.append(collection_id)
        sql = f"""
            SELECT mo.run_condition_hash, mo.pipeline_dag_hash, mo.pipeline_label, mo.main_model,
                   mo.dataset_fingerprint, MIN(mo.task_hash) AS task_hash, MIN(mo.metric_unit) AS metric_unit,
                   COUNT(DISTINCT mo.score_context) AS n_score_contexts,
                   CASE WHEN COUNT(DISTINCT mo.score_context)=1 THEN AVG(mo.metric_value) END AS mean,
                   CASE WHEN COUNT(DISTINCT mo.score_context)=1 THEN MIN(mo.metric_value) END AS min,
                   CASE WHEN COUNT(DISTINCT mo.score_context)=1 THEN MAX(mo.metric_value) END AS max,
                   COUNT(*) AS n_obs
            FROM {metrics_view} mo
            WHERE {' AND '.join(where)}
            GROUP BY mo.run_condition_hash
        """
        rows = self.store.query(sql, params)
        context_rows = self.store.query(
            f"SELECT DISTINCT mo.task_hash, mo.metric_unit FROM {metrics_view} mo WHERE {' AND '.join(where)}", params)
        withheld = sum(row["n_score_contexts"] != 1 for row in rows)
        rows = [row for row in rows if row["n_score_contexts"] == 1]
        direction = self._direction(metric)
        # Unscored (NULL-mean) rows must always sink to the bottom, regardless of
        # direction — a +inf sentinel under reverse=True would otherwise rank them #1.
        worst = float("-inf") if direction == "max" else float("inf")
        rows.sort(key=lambda r: (r["task_hash"], r["metric_unit"] is not None, r["metric_unit"] or "",
                                 (-(r["mean"] if r["mean"] is not None else worst)
                                  if direction == "max" else (r["mean"] if r["mean"] is not None else worst))))
        ranks: dict[tuple[str, str | None], int] = {}
        for r in rows[:limit]:
            key = (r["task_hash"], r["metric_unit"])
            ranks[key] = ranks.get(key, 0) + 1
            r["rank"] = ranks[key]
            r["rank_scope"] = "task_metric_unit"
        return {"include_unreported": False, "partition": partition,
                "aggregation_level": aggregation_level, "source_fold_id": source_fold_id,
                "metric": metric, "scope": scope,
                "direction": direction, "rows": rows[:limit],
                "score_context": _context_summary(context_rows, withheld)}

    # ── pipeline × dataset matrix ──────────────────────────────────────
    def matrix(
        self, *, metric: str = "rmse", scope: str = "cv", partition: str | None = None,
        aggregation_level: str = "sample", source_fold_id: str | None = None,
        dataset_fingerprint: str | None = None, task_hash: str | None = None,
        include_quarantined: bool = False,
    ) -> dict[str, Any]:
        partition = _score_partition(scope, partition)
        metrics_view = self._score_view(
            partition, scope=scope, aggregation_level=aggregation_level, source_fold_id=source_fold_id,
            task_hash=task_hash)
        where = ["mo.metric_name = ?", "mo.score_scope = ?", "mo.score_validity = 'valid'"]
        params: list[Any] = [metric, scope]
        if not include_quarantined:
            where.append("mo.execution_validity = 'valid'")
        if dataset_fingerprint:
            where.append("mo.dataset_fingerprint = ?")
            params.append(dataset_fingerprint)
        rows = self.store.query(
            f"""
            SELECT mo.pipeline_dag_hash, mo.pipeline_label, mo.dataset_fingerprint,
                   MIN(mo.task_hash) AS task_hash, MIN(mo.metric_unit) AS metric_unit,
                   COUNT(DISTINCT mo.score_context) AS n_score_contexts,
                   CASE WHEN COUNT(DISTINCT mo.score_context)=1 THEN AVG(mo.metric_value) END AS value,
                   COUNT(DISTINCT mo.execution_hash) AS coverage
            FROM {metrics_view} mo WHERE {' AND '.join(where)}
            GROUP BY mo.pipeline_dag_hash, mo.dataset_fingerprint
            """,
            params,
        )
        datasets = {r["dataset_fingerprint"]: None for r in rows}
        ds_meta = {d["dataset_fingerprint"]: d for d in self.datasets()}
        pipelines: dict[str, str] = {}
        cells: dict[tuple[str, str], dict[str, Any]] = {}
        for r in rows:
            pipelines[r["pipeline_dag_hash"]] = r["pipeline_label"] or r["pipeline_dag_hash"][:10]
            cells[(r["pipeline_dag_hash"], r["dataset_fingerprint"])] = {
                "value": r["value"],
                "coverage": r["coverage"],
                "score_context_status": "single" if r["n_score_contexts"] == 1 else "mixed",
                "task_hash": r["task_hash"] if r["n_score_contexts"] == 1 else None,
                "metric_unit": r["metric_unit"] if r["n_score_contexts"] == 1 else None,
            }
        context_rows = self.store.query(
            f"SELECT DISTINCT mo.task_hash, mo.metric_unit FROM {metrics_view} mo WHERE {' AND '.join(where)}", params)
        return {
            "include_unreported": False, "partition": partition,
                "aggregation_level": aggregation_level, "source_fold_id": source_fold_id,
            "metric": metric,
            "scope": scope,
            "direction": self._direction(metric),
            "datasets": [
                {"dataset_fingerprint": d, "label": (ds_meta.get(d) or {}).get("name") or d[:10]}
                for d in datasets
            ],
            "pipelines": [{"pipeline_dag_hash": p, "label": lbl} for p, lbl in pipelines.items()],
            "cells": [{"pipeline_dag_hash": k[0], "dataset_fingerprint": k[1], **v} for k, v in cells.items()],
            "score_context": _context_summary(context_rows, sum(r["n_score_contexts"] != 1 for r in rows)),
        }

    # ── run explorer ───────────────────────────────────────────────────
    def run_explorer(
        self,
        *,
        metric: str = "rmse",
        scope: str = "cv", partition: str | None = None,
        aggregation_level: str = "sample", source_fold_id: str | None = None,
        dataset_fingerprint: str | None = None, task_hash: str | None = None,
        pipeline_dag_hash: str | None = None,
        operator: str | None = None,
        include_quarantined: bool = True,
        limit: int = 500,
    ) -> list[dict[str, Any]]:
        partition = _score_partition(scope, partition)
        metrics_view = self._score_view(
            partition, scope=scope, aggregation_level=aggregation_level, source_fold_id=source_fold_id,
            task_hash=task_hash)
        where, params = ["1=1"], [metric, scope]
        for value, column in ((dataset_fingerprint, "rc.dataset_fingerprint"),
                              (task_hash, "rc.task_hash"),
                              (pipeline_dag_hash, "rc.pipeline_dag_hash")):
            if value:
                where.append(column + " = ?")
                params.append(value)
        if not include_quarantined:
            where.append("e.validity_status = 'valid'")
        if operator:
            where.append("EXISTS (SELECT 1 FROM pipeline_nodes pn "
                         "WHERE pn.pipeline_dag_hash = rc.pipeline_dag_hash AND pn.operator = ?)")
            params.append(operator)
        order = "DESC" if self._direction(metric) == "max" else "ASC"
        rows = self.store.query(f"""
            SELECT e.execution_hash, e.run_condition_hash, rc.pipeline_dag_hash, pd.human_label AS pipeline_label,
                   pd.main_model, rc.dataset_fingerprint, e.validity_status AS execution_validity,
                   CASE WHEN e.status = 'ok' THEN 'unreported' ELSE e.status END AS execution_status,
                   e.producer_capsule, e.time_ms, rc.task_hash,
                   CASE WHEN COUNT(DISTINCT mo.score_context)=1 THEN MIN(mo.metric_unit) END AS metric_unit,
                   COUNT(DISTINCT mo.score_context) AS n_score_contexts,
                   CASE WHEN COUNT(DISTINCT mo.score_context)=1 THEN AVG(mo.metric_value) END AS metric_value
            FROM executions e
            JOIN run_conditions rc ON rc.run_condition_hash = e.run_condition_hash
            LEFT JOIN pipeline_dags pd ON pd.pipeline_dag_hash = rc.pipeline_dag_hash
            LEFT JOIN {metrics_view} mo ON mo.execution_hash = e.execution_hash
                 AND mo.metric_name = ? AND mo.score_scope = ? AND mo.score_validity = 'valid'
            WHERE {' AND '.join(where)} GROUP BY e.execution_hash
            ORDER BY rc.task_hash, MIN(mo.metric_unit) IS NOT NULL, MIN(mo.metric_unit),
                     metric_value IS NULL, metric_value {order}, e.execution_hash LIMIT ?
        """, [*params, limit])
        for row in rows:
            row["score_partition"] = partition
            execution = self.store.get("executions", "execution_hash", row["execution_hash"])
            row.update(self._attestations(self._evidence(execution)))
        return rows

    # ── operator / parameter effects ──────────────────────────────────
    def operator_effect(
        self, *, metric: str = "rmse", scope: str = "cv", partition: str | None = None,
        aggregation_level: str = "sample", source_fold_id: str | None = None,
        dataset_fingerprint: str | None = None, task_hash: str | None = None
    ) -> dict[str, Any]:
        """Score distribution per operator presence — what each operator does."""
        partition = _score_partition(scope, partition)
        metrics_view = self._score_view(
            partition, scope=scope, aggregation_level=aggregation_level, source_fold_id=source_fold_id,
            task_hash=task_hash)
        ds_sql, ds_params = _dataset_filter(dataset_fingerprint)
        rows = self.store.query(
            f"""
            SELECT pn.operator AS operator, pn.role AS role, mo.metric_value AS value,
                   mo.task_hash, mo.metric_unit
            FROM pipeline_nodes pn
            JOIN {metrics_view} mo ON mo.pipeline_dag_hash = pn.pipeline_dag_hash
            WHERE mo.metric_name = ? AND mo.score_scope = ?{ds_sql} AND mo.execution_validity = 'valid'
                  AND pn.operator IS NOT NULL AND pn.role != 'input'
            """,
            [metric, scope, *ds_params],
        )
        groups: dict[str, list[float]] = {}
        contexts: dict[str, set[tuple[str, str | None]]] = {}
        roles: dict[str, str] = {}
        for r in rows:
            groups.setdefault(r["operator"], []).append(r["value"])
            contexts.setdefault(r["operator"], set()).add((r["task_hash"], r["metric_unit"]))
            roles[r["operator"]] = r["role"]
        series: list[dict[str, Any]] = [
            {
                "operator": op,
                "role": roles[op],
                "task_hash": next(iter(contexts[op]))[0],
                "metric_unit": next(iter(contexts[op]))[1],
                "n": len(vals),
                "mean": statistics.fmean(vals),
                "median": statistics.median(vals),
                "stdev": statistics.pstdev(vals) if len(vals) > 1 else 0.0,
                "min": min(vals),
                "max": max(vals),
                "values": vals,
            }
            for op, vals in groups.items()
            if len(contexts[op]) == 1
        ]
        direction = self._direction(metric)
        series.sort(key=lambda s: (s["task_hash"], s["metric_unit"] is not None, s["metric_unit"] or "",
                                   -s["mean"] if direction == "max" else s["mean"]))
        return {"include_unreported": False, "partition": partition,
                "aggregation_level": aggregation_level, "source_fold_id": source_fold_id,
                "metric": metric, "scope": scope,
                "direction": self._direction(metric), "series": series,
                "score_context": _context_summary(rows, sum(len(x) > 1 for x in contexts.values()))}

    def parameter_effect(
        self, param_name: str, *, metric: str = "rmse", scope: str = "cv", partition: str | None = None,
        aggregation_level: str = "sample", source_fold_id: str | None = None,
        dataset_fingerprint: str | None = None, task_hash: str | None = None
    ) -> dict[str, Any]:
        """Metric vs a (numeric or categorical) parameter — e.g. PLS n_components."""
        partition = _score_partition(scope, partition)
        metrics_view = self._score_view(
            partition, scope=scope, aggregation_level=aggregation_level, source_fold_id=source_fold_id,
            task_hash=task_hash)
        ds_sql, ds_params = _dataset_filter(dataset_fingerprint)
        # Read the ``param:<name>`` run facets, like Playground and Parallel and like the static engine:
        # a run contributes once per distinct value of the parameter across its stages.
        rows = self.store.query(
            f"""
            SELECT rf.facet_num AS num, rf.facet_value AS raw,
                   mo.metric_value AS value, mo.dataset_fingerprint AS df, mo.pipeline_label AS label,
                   mo.task_hash, mo.metric_unit
            FROM run_facets rf
            JOIN {metrics_view} mo ON mo.run_condition_hash = rf.run_condition_hash
            WHERE rf.facet_key = ? AND mo.metric_name = ? AND mo.score_scope = ?{ds_sql}
                  AND mo.execution_validity = 'valid'
            """,
            [f"param:{param_name}", metric, scope, *ds_params],
        )
        points = [
            {"param": r["num"] if r["num"] is not None else r["raw"], "numeric": r["num"], "metric_value": r["value"],
             "dataset_fingerprint": r["df"], "pipeline_label": r["label"],
             "task_hash": r["task_hash"], "metric_unit": r["metric_unit"]}
            for r in rows
        ]
        return {"include_unreported": False, "partition": partition,
                "aggregation_level": aggregation_level, "source_fold_id": source_fold_id, "param_name": param_name,
                "metric": metric, "scope": scope,
                "direction": self._direction(metric), "points": points, "score_context": _context_summary(rows)}

    # ── run detail ─────────────────────────────────────────────────────
    def run_detail(self, execution_hash: str) -> dict[str, Any] | None:
        s = self.store
        execution = s.get("executions", "execution_hash", execution_hash)
        if not execution:
            return None
        execution["status"] = "unreported" if execution["status"] == "ok" else execution["status"]
        if execution.get("failure_message") is not None:
            execution["failure_message"] = "[redacted]"
        metadata = self._evidence(execution)
        execution.update(self._attestations(metadata))
        rc = s.get("run_conditions", "run_condition_hash", execution["run_condition_hash"]) or {}
        pipeline = s.get("pipeline_dags", "pipeline_dag_hash", rc.get("pipeline_dag_hash")) or {}
        nodes = s.query(
            "SELECT * FROM pipeline_nodes WHERE pipeline_dag_hash = ?", (rc.get("pipeline_dag_hash"),)
        )
        edges = s.query(
            "SELECT src, src_port, dst, dst_port FROM pipeline_edges WHERE pipeline_dag_hash = ?",
            (rc.get("pipeline_dag_hash"),),
        )
        node_params = s.query(
            """SELECT pnp.node_id, pv.name, pv.value_json FROM pipeline_node_params pnp
               JOIN parameter_values pv ON pv.param_value_hash = pnp.param_value_hash
               WHERE pnp.pipeline_dag_hash = ?""",
            (rc.get("pipeline_dag_hash"),),
        )
        scores = s.query(
            """SELECT ss.score_set_id, ss.scope, ss.validity_status, mo.metric_name, mo.metric_value, mo.metric_unit,
                      mo.fold_id, mo.source_fold_id, mo.aggregation_level,
                      mo.partition, mo.direction
               FROM score_sets ss JOIN metric_observations mo ON mo.score_set_id = ss.score_set_id
               WHERE ss.execution_hash = ? ORDER BY mo.metric_name, mo.fold_id""",
            (execution_hash,),
        )
        residual_set = s.query_one("SELECT * FROM residual_sets WHERE execution_hash = ?", (execution_hash,))
        graph = json.loads(pipeline["graph_json"]) if pipeline.get("graph_json") else None
        return {
            "execution": execution,
            "metadata": metadata,
            "run_condition": rc,
            "pipeline": {**pipeline, "graph": graph},
            "nodes": nodes,
            "edges": edges,
            "node_params": node_params,
            "scores": scores,
            "residual_set": residual_set,
            "dataset": s.query_one(
                "SELECT * FROM dataset_fingerprints WHERE dataset_fingerprint = ?", (rc.get("dataset_fingerprint"),)
            ),
            "cv": s.get("cv_instances", "cv_instance_hash", rc.get("cv_instance_hash")),
            "rng": s.get("rng_contexts", "rng_context_hash", rc.get("rng_context_hash")),
            "refit": s.get("refit_strategies", "refit_strategy_hash", rc.get("refit_strategy_hash")),
        }

    def submission_context(self, execution_hash: str) -> dict[str, Any]:
        """Return private per-export context only when its cached binding is consistent."""
        from nirs4all_benchmarks.ingestion.metadata_bundle import read_stored_metadata
        from nirs4all_benchmarks.ingestion.submitted_metadata import binding_consistent

        execution = self.store.get("executions", "execution_hash", execution_hash)
        export_hash = execution["arena_export_hash"] if execution else None
        row = self.store.get("submitted_metadata", "arena_export_hash", export_hash) if export_hash else None
        receipt = self.store.get("metadata_receipts", "arena_export_hash", export_hash) if export_hash else None
        unavailable = {"state": "unavailable", "value": None}
        result: dict[str, Any] = {"association_scope": "this_export", "dataset_card_hash": None,
                                  "context_status": "unavailable",
                                  **{key: dict(unavailable) for key in (
                                      "pipeline_human_label", "target_unit", "engine_graph_fingerprint",
                                      "sample_manifest_hash")}}
        if not execution or not export_hash or not row or not receipt:
            return result
        if not binding_consistent(execution, row, receipt):
            result["context_status"] = "binding_inconsistent"
            return result
        clean = read_stored_metadata(None, export_hash, stored_json=receipt["receipt_json"])
        if clean.get("status") == "metadata_unavailable":
            result["context_status"] = "binding_inconsistent"
            return result
        result["dataset_card_hash"] = row["dataset_card_hash"]
        context = json.loads(row["source_metadata_json"]).get("arena_context")
        if context is not None:
            result["context_status"] = "available"
            for output, block, key in (("pipeline_human_label", "pipeline", "human_label"),
                                       ("target_unit", "task", "target_unit")):
                values = context[block]
                value = values.get(key)
                state = ("absent" if key not in values else
                         "normalized_null" if value is None and row["representation_kind"] == "model_normalized" else
                         "declared_null" if value is None else "reported")
                result[output] = {"state": state, "value": value}
        for output, block, key in (("engine_graph_fingerprint", "pipeline", "engine_graph_fingerprint"),
                                   ("sample_manifest_hash", "dataset_variant", "sample_manifest_hash")):
            value = clean.get(block, {}).get(key)
            result[output] = {"state": "reported" if value is not None else "not_reported", "value": value}
        return result

    def run_api_detail(self, execution_hash: str, *, project: bool = False) -> dict[str, Any] | None:
        detail = self.run_detail(execution_hash)
        if detail is None:
            return None
        dataset = detail.get("dataset")
        if dataset is not None:
            dataset["first_accepted_dataset_card_hash"] = dataset.pop("dataset_card_hash")
            dataset["association_scope"] = "dataset_fingerprint_first_accepted"
        export_hash = detail["execution"]["arena_export_hash"]
        row = self.store.get("submitted_metadata", "arena_export_hash", export_hash)
        detail["submission"] = {"association_scope": "this_export",
                                "dataset_card_hash": row["dataset_card_hash"] if row else None}
        if project:
            detail["submission"] = self.submission_context(execution_hash)
            pipeline = detail["pipeline"]
            for key in ("human_label", "engine_graph_fingerprint", "nirs4all_identity_hash"):
                if key in pipeline:
                    pipeline["first_accepted_" + key] = pipeline.pop(key)
            pipeline["association_scope"] = "pipeline_first_accepted"
        else:
            detail["submission"]["context_status"] = "not_exposed"
            detail["pipeline"]["association_scope"] = "pipeline_first_accepted"
        return detail

    def fold_scores(self, execution_hash: str, *, metric: str = "rmse") -> list[dict[str, Any]]:
        # Fold-level observations are identified by a non-null fold_id (scope lives
        # on score_sets, not on metric_observations).
        return self.store.query(
            """SELECT mo.fold_id, mo.source_fold_id, mo.aggregation_level,
                      mo.partition, mo.metric_value, mo.metric_unit
               FROM score_sets ss JOIN metric_observations mo ON mo.score_set_id = ss.score_set_id
               WHERE ss.execution_hash = ? AND mo.metric_name = ? AND mo.fold_id IS NOT NULL
                     AND ss.validity_status = 'valid'
               ORDER BY mo.fold_id""",
            (execution_hash, metric),
        )

    # ── residuals & complementarity ────────────────────────────────────
    def residuals(
        self, execution_hash: str, *, partition: str | None = None, scope: str | None = None
    ) -> list[dict[str, Any]]:
        if partition is not None and partition not in _VALID_PARTITIONS:
            raise ValueError("invalid_score_partition")
        if scope is not None:
            _safe_scope(scope)
        rset = self.store.query_one("SELECT * FROM residual_sets WHERE execution_hash = ?", (execution_hash,))
        if not rset:
            return []
        if not rset["pseudonymized"]:
            raise ValueError("legacy_unsanitized_residuals_refused")
        rows = self.store.residuals.read(rset["residual_set_id"])
        publication = json.loads(rset["publishable_json"])
        for row in rows:
            for key in ("y_true", "y_pred", "y_proba"):
                if not publication.get(key, True):
                    row[key] = None
            if not all(publication.get(key, True) for key in ("y_true", "y_pred", "residual")):
                row["residual"] = None
            if not publication.get("residual", True):
                row["y_pred"] = None
                row["y_proba"] = None
        if partition:
            rows = [r for r in rows if r["partition"] == partition]
        if scope:
            rows = [r for r in rows if r["scope"] == scope]
        return rows

    def residual_compare(
        self, execution_a: str, execution_b: str, *, partition: str = "validation"
    ) -> dict[str, Any]:
        """Compare two runs' residuals on the *same* samples (sample-keyed join)."""
        contexts = []
        for execution_hash in (execution_a, execution_b):
            rset = self.store.query_one("SELECT key FROM residual_sets WHERE execution_hash = ?", (execution_hash,))
            if rset and rset["key"] != "sample_id":
                raise ValueError("positional_residual_comparison_unsupported")
            contexts.append(self.store.query_one(
                "SELECT rc.dataset_fingerprint, rc.task_hash FROM executions e "
                "JOIN run_conditions rc ON rc.run_condition_hash = e.run_condition_hash WHERE e.execution_hash = ?",
                (execution_hash,),
            ))
        if all(contexts) and contexts[0] != contexts[1]:
            raise ValueError("residual_comparison_identity_mismatch")
        a = {r["sample_id"]: r for r in self.residuals(execution_a, partition=partition)}
        b = {r["sample_id"]: r for r in self.residuals(execution_b, partition=partition)}
        common = sorted(set(a) & set(b))
        paired = []
        ra, rb = [], []
        for sid in common:
            res_a = a[sid].get("residual")
            res_b = b[sid].get("residual")
            if res_a is None or res_b is None:
                continue
            paired.append({"sample_id": sid, "residual_a": res_a, "residual_b": res_b,
                           "y_true": a[sid].get("y_true")})
            ra.append(res_a)
            rb.append(res_b)
        complementarity = None
        if len(ra) > 1:
            try:
                complementarity = statistics.correlation(ra, rb)
            except statistics.StatisticsError:
                complementarity = None
        return {
            "n_common": len(common),
            "n_paired": len(paired),
            "residual_correlation": complementarity,
            "rmse_a": (sum(x * x for x in ra) / len(ra)) ** 0.5 if ra else None,
            "rmse_b": (sum(x * x for x in rb) / len(rb)) ** 0.5 if rb else None,
            "paired": paired,
        }

    # ── robustness ─────────────────────────────────────────────────────
    def robustness(
        self, *, metric: str = "rmse", scope: str = "cv", partition: str | None = None,
        aggregation_level: str = "sample", source_fold_id: str | None = None,
        dataset_fingerprint: str | None = None, task_hash: str | None = None
    ) -> list[dict[str, Any]]:
        """Variance of a pipeline's score across executions (folds/seeds/splits)."""
        partition = _score_partition(scope, partition)
        metrics_view = self._score_view(
            partition, scope=scope, aggregation_level=aggregation_level, source_fold_id=source_fold_id,
            task_hash=task_hash)
        ds_sql, ds_params = _dataset_filter(dataset_fingerprint)
        rows = self.store.query(
            f"""SELECT mo.pipeline_dag_hash, mo.pipeline_label, mo.dataset_fingerprint, mo.metric_value AS value,
                       mo.task_hash, mo.metric_unit
               FROM {metrics_view} mo
               WHERE mo.metric_name = ? AND mo.score_scope = ?{ds_sql} AND mo.execution_validity = 'valid'""",
            [metric, scope, *ds_params],
        )
        groups: dict[tuple[str, str, str, str | None], list[float]] = {}
        labels: dict[tuple[str, str, str, str | None], str] = {}
        for r in rows:
            key = (r["pipeline_dag_hash"], r["dataset_fingerprint"], r["task_hash"], r["metric_unit"])
            groups.setdefault(key, []).append(r["value"])
            labels[key] = r["pipeline_label"] or r["pipeline_dag_hash"][:10]
        out: list[dict[str, Any]] = []
        for (pdh, df, task, unit), vals in groups.items():
            out.append({
                "partition": partition,
                "aggregation_level": aggregation_level, "source_fold_id": source_fold_id,
                "pipeline_dag_hash": pdh,
                "dataset_fingerprint": df,
                "label": labels[(pdh, df, task, unit)], "task_hash": task, "metric_unit": unit,
                "n": len(vals),
                "mean": statistics.fmean(vals),
                "stdev": statistics.pstdev(vals) if len(vals) > 1 else 0.0,
                "cv_pct": (statistics.pstdev(vals) / statistics.fmean(vals) * 100.0)
                if len(vals) > 1 and statistics.fmean(vals) else 0.0,
            })
        out.sort(key=lambda r: (r["task_hash"], r["metric_unit"] is not None, r["metric_unit"] or "", r["stdev"]))
        return out

    # ── faceting / pivot / playground ──────────────────────────────────
    def facets(self) -> list[dict[str, Any]]:
        """Available facet keys with cardinality + whether they are numeric.

        Powers the pivot/playground group-by selectors (DESIGN.md §9.3).
        """
        return self.store.query(
            """
            SELECT facet_key,
                   COUNT(DISTINCT facet_value) AS n_values,
                   MAX(role) AS role,
                   SUM(CASE WHEN facet_num IS NOT NULL THEN 1 ELSE 0 END) > 0 AS numeric
            FROM run_facets
            GROUP BY facet_key
            HAVING n_values > 1 OR facet_key LIKE 'param:%'
            ORDER BY (facet_key LIKE 'param:%'), facet_key
            """
        )

    def facet_values(self, facet_key: str, *, limit: int = 200) -> list[dict[str, Any]]:
        return self.store.query(
            """SELECT facet_value, MAX(facet_num) AS facet_num, COUNT(*) AS n
               FROM run_facets WHERE facet_key = ? GROUP BY facet_value
               ORDER BY (facet_num IS NULL), facet_num, facet_value LIMIT ?""",
            (facet_key, limit),
        )

    def pivot(
        self,
        *,
        group_by: str,
        color_by: str | None = None,
        metric: str = "rmse",
        scope: str = "cv",
        partition: str | None = None,
        aggregation_level: str = "sample", source_fold_id: str | None = None,
        dataset_fingerprint: str | None = None, task_hash: str | None = None,
        include_quarantined: bool = False,
        agg: str = "mean",
    ) -> dict[str, Any]:
        """Aggregate the metric across executions grouped by one or two facets.

        The heart of the playground: ``group_by`` (and optional ``color_by``) is any
        facet key (split_method, has_augmentation, preprocessing_op, model_family,
        ``param:n_components``, …). A run contributes to each value of a multi-valued
        facet (presence semantics), so "effect of SNV" or "augmentation on/off" work.
        """
        partition = _score_partition(scope, partition)
        metrics_view = self._score_view(
            partition, scope=scope, aggregation_level=aggregation_level, source_fold_id=source_fold_id,
            task_hash=task_hash)
        where = ["mo.metric_name = ?", "mo.score_scope = ?", "mo.score_validity = 'valid'"]
        params: list[Any] = [group_by, metric, scope]  # first ? is the gb.facet_key join
        if not include_quarantined:
            where.append("mo.execution_validity = 'valid'")
        if partition:
            where.append("mo.partition = ?")
            params.append(partition)
        if dataset_fingerprint:
            where.append("mo.dataset_fingerprint = ?")
            params.append(dataset_fingerprint)
        color_join = ""
        select_color = "NULL AS color, NULL AS color_num"
        group_cols = "gb.facet_value, gb.facet_num"
        if color_by:
            color_join = "JOIN run_facets cb ON cb.run_condition_hash = mo.run_condition_hash AND cb.facet_key = ?"
            params.insert(1, color_by)  # second ? is the cb.facet_key join
            select_color = "cb.facet_value AS color, cb.facet_num AS color_num"
            group_cols = "gb.facet_value, gb.facet_num, cb.facet_value, cb.facet_num"
        sql = f"""
            SELECT gb.facet_value AS group_value, gb.facet_num AS group_num, {select_color},
                   COUNT(DISTINCT mo.score_context) AS n_score_contexts,
                   CASE WHEN COUNT(DISTINCT mo.score_context)=1 THEN MIN(mo.task_hash) END AS task_hash,
                   CASE WHEN COUNT(DISTINCT mo.score_context)=1 THEN MIN(mo.metric_unit) END AS metric_unit,
                   CASE WHEN COUNT(DISTINCT mo.score_context)=1 THEN AVG(mo.metric_value) END AS mean,
                   CASE WHEN COUNT(DISTINCT mo.score_context)=1 THEN MIN(mo.metric_value) END AS min,
                   CASE WHEN COUNT(DISTINCT mo.score_context)=1 THEN MAX(mo.metric_value) END AS max,
                   COUNT(DISTINCT mo.execution_hash) AS n
            FROM {metrics_view} mo
            JOIN run_facets gb ON gb.run_condition_hash = mo.run_condition_hash AND gb.facet_key = ?
            {color_join}
            WHERE {' AND '.join(where)}
            GROUP BY {group_cols}
        """
        rows = self.store.query(sql, params)
        context_rows = self.store.query(
            f"SELECT DISTINCT mo.task_hash, mo.metric_unit FROM {metrics_view} mo "
            "JOIN run_facets gb ON gb.run_condition_hash=mo.run_condition_hash AND gb.facet_key = ? "
            f"{color_join} WHERE {' AND '.join(where)}", params)
        withheld = sum(row["n_score_contexts"] != 1 for row in rows)
        rows = [row for row in rows if row["n_score_contexts"] == 1]
        for r in rows:
            r["value"] = r.pop("mean")  # the aggregate the UI plots
        rows.sort(key=lambda r: (r["group_num"] if r["group_num"] is not None else float("inf"),
                                 str(r["group_value"])))
        return {
            "include_unreported": False, "partition": partition,
                "aggregation_level": aggregation_level, "source_fold_id": source_fold_id,
            "group_by": group_by, "color_by": color_by, "metric": metric, "scope": scope,
            "direction": self._direction(metric), "agg": agg, "rows": rows,
            "score_context": _context_summary(context_rows, withheld),
        }

    def parallel(
        self,
        *,
        dimensions: list[str],
        metric: str = "rmse",
        scope: str = "cv", partition: str | None = None,
        aggregation_level: str = "sample", source_fold_id: str | None = None,
        dataset_fingerprint: str | None = None, task_hash: str | None = None,
        include_quarantined: bool = False,
    ) -> dict[str, Any]:
        """Per-execution rows with selected single-valued facet columns + the metric.

        Feeds a parallel-coordinates plot over split/cv/model/params → score.
        """
        partition = _score_partition(scope, partition)
        metrics_view = self._score_view(
            partition, scope=scope, aggregation_level=aggregation_level, source_fold_id=source_fold_id,
            task_hash=task_hash)
        where = ["mo.metric_name = ?", "mo.score_scope = ?", "mo.score_validity = 'valid'"]
        params: list[Any] = [metric, scope]
        if not include_quarantined:
            where.append("mo.execution_validity = 'valid'")
        if dataset_fingerprint:
            where.append("mo.dataset_fingerprint = ?")
            params.append(dataset_fingerprint)
        base = self.store.query(
            f"""SELECT mo.execution_hash, mo.run_condition_hash, mo.pipeline_label,
                       MIN(mo.task_hash) AS task_hash,
                       CASE WHEN COUNT(DISTINCT mo.score_context)=1 THEN MIN(mo.metric_unit) END AS metric_unit,
                       COUNT(DISTINCT mo.score_context) AS n_score_contexts,
                       CASE WHEN COUNT(DISTINCT mo.score_context)=1 THEN AVG(mo.metric_value) END AS metric_value
                FROM {metrics_view} mo WHERE {' AND '.join(where)}
                GROUP BY mo.execution_hash ORDER BY task_hash, metric_unit IS NOT NULL, metric_unit, metric_value""",
            params,
        )
        rch_to_facets: dict[str, dict[str, Any]] = {}
        rchs = {r["run_condition_hash"] for r in base}
        if rchs and dimensions:
            placeholders = ",".join("?" for _ in rchs)
            dim_ph = ",".join("?" for _ in dimensions)
            frows = self.store.query(
                f"""SELECT run_condition_hash, facet_key, facet_value, facet_num FROM run_facets
                    WHERE run_condition_hash IN ({placeholders}) AND facet_key IN ({dim_ph})""",
                [*rchs, *dimensions],
            )
            for fr in frows:
                d = rch_to_facets.setdefault(fr["run_condition_hash"], {})
                # first value wins for single-valued dims (deterministic by query order)
                d.setdefault(fr["facet_key"], fr["facet_num"] if fr["facet_num"] is not None else fr["facet_value"])
        rows = []
        for r in base:
            facet_map = rch_to_facets.get(r["run_condition_hash"], {})
            row = {dim: facet_map.get(dim) for dim in dimensions}
            row["metric"] = r["metric_value"]
            row["pipeline_label"] = r["pipeline_label"]
            row["execution_hash"] = r["execution_hash"]
            row["task_hash"] = r["task_hash"]
            row["metric_unit"] = r["metric_unit"] if r["n_score_contexts"] == 1 else None
            row["n_score_contexts"] = r["n_score_contexts"]
            rows.append(row)
        return {"include_unreported": False, "partition": partition,
                "aggregation_level": aggregation_level, "source_fold_id": source_fold_id,
                "dimensions": [*dimensions, "metric"],
                "metric": metric, "scope": scope,
                "direction": self._direction(metric), "rows": rows,
                "score_context": _context_summary(self._selected_contexts(metrics_view, metric, scope,
                                                                             dataset_fingerprint,
                                                                             include_quarantined=include_quarantined),
                                                  sum(r["n_score_contexts"] != 1 for r in base))}

    def planned(self) -> list[dict[str, Any]]:
        """Planned (not-yet-run) pipeline × dataset conditions awaiting a runner."""
        return self.store.query(
            """SELECT pr.plan_id, pr.pipeline_dag_hash, pd.human_label, pr.dataset_fingerprint,
                      dc.name AS dataset_name, pr.collection_id, 'planned' AS status, pr.source, pr.created_at
               FROM planned_runs pr
               LEFT JOIN pipeline_dags pd ON pd.pipeline_dag_hash = pr.pipeline_dag_hash
               LEFT JOIN dataset_cards dc ON dc.dataset_fingerprint = pr.dataset_fingerprint
               WHERE pr.status IN ('planned', 'fulfilled') AND NOT EXISTS (
                   SELECT 1 FROM executions e JOIN run_conditions rc ON rc.run_condition_hash = e.run_condition_hash
                   WHERE rc.pipeline_dag_hash = pr.pipeline_dag_hash AND rc.dataset_fingerprint = pr.dataset_fingerprint
                         AND e.status = 'completed' AND e.validity_status = 'valid'
               )
               ORDER BY pr.created_at DESC""",
        )

    # ── statistics ─────────────────────────────────────────────────────
    def stats(
        self, *, metric: str = "rmse", scope: str = "cv", partition: str | None = None,
        aggregation_level: str = "sample", source_fold_id: str | None = None,
        dataset_fingerprint: str | None = None, task_hash: str | None = None
    ) -> dict[str, Any]:
        """Distribution summary, per-dataset spread, and facet↔metric correlations.

        Powers the Statistics view: overall + per-dataset summaries (mean/median/std/
        quartiles), the raw value list (for a histogram/violin), and the Pearson
        correlation of every numeric facet (params, n_folds, seed, …) with the score.
        """
        import numpy as np

        partition = _score_partition(scope, partition)
        metrics_view = self._score_view(
            partition, scope=scope, aggregation_level=aggregation_level, source_fold_id=source_fold_id,
            task_hash=task_hash)
        ds_sql, ds_params = _dataset_filter(dataset_fingerprint)
        rows = self.store.query(
            f"""SELECT mo.execution_hash AS eh, mo.dataset_fingerprint AS df,
                      CASE WHEN COUNT(DISTINCT mo.score_context)=1 THEN AVG(mo.metric_value) END AS v
               FROM {metrics_view} mo
               WHERE mo.metric_name = ? AND mo.score_scope = ?{ds_sql} AND mo.score_validity = 'valid'
                     AND mo.execution_validity = 'valid'
               GROUP BY mo.execution_hash""",
            [metric, scope, *ds_params],
        )
        context_rows = self.store.query(
            f"SELECT DISTINCT mo.dataset_fingerprint AS df, mo.task_hash, mo.metric_unit FROM {metrics_view} mo "
            f"WHERE mo.metric_name = ? AND mo.score_scope = ?{ds_sql} AND mo.score_validity = 'valid' "
            "AND mo.execution_validity = 'valid'", [metric, scope, *ds_params])
        all_contexts = {(r["task_hash"], r["metric_unit"]) for r in context_rows}

        def summ(a: Any) -> dict[str, Any]:
            arr = np.asarray([x for x in a if x is not None], dtype=float)
            if arr.size == 0:
                return {"n": 0}
            return {
                "n": int(arr.size), "mean": float(arr.mean()), "median": float(np.median(arr)),
                "std": float(arr.std()), "min": float(arr.min()), "max": float(arr.max()),
                "p25": float(np.percentile(arr, 25)), "p75": float(np.percentile(arr, 75)),
            }

        ds_names = {d["dataset_fingerprint"]: d.get("name") for d in self.datasets()}
        by_dataset = []
        for df in sorted({r["df"] for r in rows}):
            ds_contexts = {(r["task_hash"], r["metric_unit"]) for r in context_rows if r["df"] == df}
            vals = [r["v"] for r in rows if r["df"] == df] if len(ds_contexts) == 1 else []
            ds_task, ds_unit = next(iter(ds_contexts)) if len(ds_contexts) == 1 else (None, None)
            by_dataset.append({"dataset_fingerprint": df, "label": ds_names.get(df) or df[:10],
                               "task_hash": ds_task, "metric_unit": ds_unit,
                               "score_context_status": "single" if len(ds_contexts) == 1 else "mixed",
                               "values": [float(v) for v in vals if v is not None], **summ(vals)})

        # facet↔metric correlations (numeric facets only)
        crows = self.store.query(
            f"""SELECT rf.facet_key AS fk, rf.facet_num AS fn, AVG(mo.metric_value) AS v
               FROM run_facets rf
               JOIN {metrics_view} mo ON mo.run_condition_hash = rf.run_condition_hash
               WHERE rf.facet_num IS NOT NULL AND mo.metric_name = ? AND mo.score_scope = ?{ds_sql}
                     AND mo.score_validity = 'valid' AND mo.execution_validity = 'valid'
               GROUP BY rf.facet_key, mo.execution_hash, rf.facet_num""",
            [metric, scope, *ds_params],
        )
        by_facet: dict[str, list[tuple[float, float]]] = {}
        for r in crows if len(all_contexts) == 1 else []:
            if r["v"] is not None:
                by_facet.setdefault(r["fk"], []).append((float(r["fn"]), float(r["v"])))
        correlations: list[dict[str, Any]] = []
        for fk, pairs in by_facet.items():
            xs = np.asarray([p[0] for p in pairs])
            ys = np.asarray([p[1] for p in pairs])
            if xs.size >= 4 and len({*xs.tolist()}) >= 2 and xs.std() > 0 and ys.std() > 0:
                pearson = float(np.corrcoef(xs, ys)[0, 1])
                correlations.append({"facet": fk, "r": pearson, "abs_r": abs(pearson), "n": int(xs.size)})
        correlations.sort(key=lambda c: -c["abs_r"])

        return {
            "include_unreported": False, "partition": partition,
                "aggregation_level": aggregation_level, "source_fold_id": source_fold_id,
            "metric": metric, "scope": scope, "direction": self._direction(metric),
            "summary": summ([r["v"] for r in rows]) if len(all_contexts) == 1 else {"n": 0},
            "values": [float(r["v"]) for r in rows if r["v"] is not None] if len(all_contexts) == 1 else [],
            "by_dataset": by_dataset, "correlations": correlations,
            "score_context": _context_summary(context_rows, int(len(all_contexts) > 1)),
        }

    # ── graph / network ────────────────────────────────────────────────
    def _pipeline_scores(self, metric: str, scope: str, dataset_fingerprint: str | None,
                         partition: str | None = None, aggregation_level: str = "sample",
                         source_fold_id: str | None = None, task_hash: str | None = None) -> list[dict[str, Any]]:
        partition = _score_partition(scope, partition)
        metrics_view = self._score_view(
            partition, scope=scope, aggregation_level=aggregation_level, source_fold_id=source_fold_id,
            task_hash=task_hash)
        ds_sql, ds_params = _dataset_filter(dataset_fingerprint)
        return self.store.query(
            f"""SELECT mo.pipeline_dag_hash AS pdh, mo.pipeline_label AS label, mo.main_model AS main_model,
                      CASE WHEN COUNT(DISTINCT mo.score_context)=1 THEN AVG(mo.metric_value) END AS score,
                      CASE WHEN COUNT(DISTINCT mo.score_context)=1 THEN MIN(mo.task_hash) END AS task_hash,
                      CASE WHEN COUNT(DISTINCT mo.score_context)=1 THEN MIN(mo.metric_unit) END AS metric_unit,
                      COUNT(DISTINCT mo.score_context) AS n_score_contexts,
                      COUNT(DISTINCT mo.execution_hash) AS n_runs
               FROM {metrics_view} mo
               WHERE mo.metric_name = ? AND mo.score_scope = ?{ds_sql} AND mo.score_validity = 'valid'
                     AND mo.execution_validity = 'valid'
               GROUP BY mo.pipeline_dag_hash""",
            [metric, scope, *ds_params],
        )

    def _pipeline_operator_sets(self, pdhs: list[str]) -> dict[str, set[str]]:
        if not pdhs:
            return {}
        ph = ",".join("?" for _ in pdhs)
        rows = self.store.query(
            f"""SELECT pipeline_dag_hash AS pdh, operator FROM pipeline_nodes
                WHERE pipeline_dag_hash IN ({ph}) AND operator IS NOT NULL AND role != 'input'""",
            pdhs,
        )
        out: dict[str, set[str]] = {}
        for r in rows:
            out.setdefault(r["pdh"], set()).add(r["operator"].split(".")[-1])
        return out

    def pipeline_graph(
        self, *, metric: str = "rmse", scope: str = "cv", partition: str | None = None,
        aggregation_level: str = "sample", source_fold_id: str | None = None,
        dataset_fingerprint: str | None = None, task_hash: str | None = None,
        min_jaccard: float = 0.34, max_nodes: int = 150,
    ) -> dict[str, Any]:
        """A clustered network of pipelines — edges = shared-operator Jaccard.

        Nodes are pipelines (clustered by model family, sized by run count, colored by
        mean score); an edge links two pipelines that share preprocessing/model
        operators above ``min_jaccard``. This is the "mega graph": families of related
        pipelines surface as clusters.
        """
        partition = _score_partition(scope, partition)
        metrics_view = self._score_view(partition, scope=scope, aggregation_level=aggregation_level,
                                        source_fold_id=source_fold_id, task_hash=task_hash)
        rows = sorted(self._pipeline_scores(
            metric, scope, dataset_fingerprint, partition, aggregation_level, source_fold_id, task_hash),
                      key=lambda r: -(r["n_runs"] or 0))[:max_nodes]
        pdhs = [r["pdh"] for r in rows]
        opsets = self._pipeline_operator_sets(pdhs)
        nodes = [
            {"id": r["pdh"], "label": r["label"] or r["pdh"][:10],
             "cluster": (r["main_model"] or "other").split(".")[-1], "size": r["n_runs"], "score": r["score"],
             "task_hash": r["task_hash"], "metric_unit": r["metric_unit"],
             "score_context_status": "single" if r["n_score_contexts"] == 1 else "mixed"}
            for r in rows
        ]
        edges: list[dict[str, Any]] = []
        for i in range(len(pdhs)):
            a = opsets.get(pdhs[i], set())
            if not a:
                continue
            for j in range(i + 1, len(pdhs)):
                b = opsets.get(pdhs[j], set())
                if not b:
                    continue
                union = len(a | b)
                jac = len(a & b) / union if union else 0.0
                if jac >= min_jaccard:
                    edges.append({"source": pdhs[i], "target": pdhs[j], "weight": round(jac, 3)})
        return {"include_unreported": False, "partition": partition,
                "aggregation_level": aggregation_level, "source_fold_id": source_fold_id,
                "kind": "pipelines", "metric": metric,
                "scope": scope, "direction": self._direction(metric),
                "nodes": nodes, "edges": edges,
                "score_context": _context_summary(self._selected_contexts(metrics_view, metric, scope,
                                                                             dataset_fingerprint),
                                                  sum(r["n_score_contexts"] != 1 for r in rows))}

    def operator_graph(
        self, *, metric: str = "rmse", scope: str = "cv", partition: str | None = None,
        aggregation_level: str = "sample", source_fold_id: str | None = None,
        dataset_fingerprint: str | None = None, max_nodes: int = 150, task_hash: str | None = None
    ) -> dict[str, Any]:
        """A clustered network of operators — edges = co-occurrence in the same pipeline.

        Nodes are operators (clustered by stage role, sized by pipeline count, colored
        by the mean score of runs that use them); edges connect operators that appear
        together in a pipeline.
        """
        from nirs4all_benchmarks.indexing import classify_role

        partition = _score_partition(scope, partition)
        metrics_view = self._score_view(
            partition, scope=scope, aggregation_level=aggregation_level, source_fold_id=source_fold_id,
            task_hash=task_hash)
        ds_sql, ds_params = _dataset_filter(dataset_fingerprint)
        stat_rows = self.store.query(
            f"""SELECT pn.operator AS operator, MAX(pn.role) AS role,
                      COUNT(DISTINCT pn.pipeline_dag_hash) AS n_pipes,
                      COUNT(DISTINCT mo.execution_hash) AS n_runs, COUNT(DISTINCT mo.score_context) AS n_score_contexts,
                      CASE WHEN COUNT(DISTINCT mo.score_context)=1 THEN MIN(mo.task_hash) END AS task_hash,
                      CASE WHEN COUNT(DISTINCT mo.score_context)=1 THEN MIN(mo.metric_unit) END AS metric_unit,
                      CASE WHEN COUNT(DISTINCT mo.score_context)=1 THEN AVG(mo.metric_value) END AS score
               FROM pipeline_nodes pn
               JOIN {metrics_view} mo ON mo.pipeline_dag_hash = pn.pipeline_dag_hash
               WHERE pn.operator IS NOT NULL AND pn.role != 'input'
                     AND mo.metric_name = ? AND mo.score_scope = ?{ds_sql} AND mo.score_validity = 'valid'
                     AND mo.execution_validity = 'valid'
               GROUP BY pn.operator ORDER BY n_pipes DESC LIMIT ?""",
            [metric, scope, *ds_params, max_nodes],
        )
        keep = {r["operator"] for r in stat_rows}
        nodes = [
            {"id": r["operator"].split(".")[-1], "operator": r["operator"],
             "cluster": classify_role(r["operator"], r["role"]), "size": r["n_pipes"],
             "score": r["score"], "n_runs": r["n_runs"], "task_hash": r["task_hash"],
             "metric_unit": r["metric_unit"],
             "score_context_status": "single" if r["n_score_contexts"] == 1 else "mixed"}
            for r in stat_rows
        ]
        # co-occurrence within pipelines that have valid runs
        pdhs = [r["pdh"] for r in self._pipeline_scores(
            metric, scope, dataset_fingerprint, partition, aggregation_level, source_fold_id, task_hash)]
        opsets = self._pipeline_operator_sets(pdhs)
        leaf_keep = {op.split(".")[-1] for op in keep}
        pair_counts: dict[tuple[str, str], int] = {}
        for ops in opsets.values():
            present = sorted(o for o in ops if o in leaf_keep)
            for i in range(len(present)):
                for j in range(i + 1, len(present)):
                    pair_counts[(present[i], present[j])] = pair_counts.get((present[i], present[j]), 0) + 1
        edges = [{"source": a, "target": b, "weight": w} for (a, b), w in pair_counts.items()]
        return {"include_unreported": False, "partition": partition,
                "aggregation_level": aggregation_level, "source_fold_id": source_fold_id,
                "kind": "operators", "metric": metric,
                "scope": scope, "direction": self._direction(metric),
                "nodes": nodes, "edges": edges,
                "score_context": _context_summary(self._selected_contexts(metrics_view, metric, scope,
                                                                             dataset_fingerprint),
                                                  sum(r["n_score_contexts"] != 1 for r in stat_rows))}

    def composition(
        self, *, metric: str = "rmse", scope: str = "cv", partition: str | None = None,
        aggregation_level: str = "sample", source_fold_id: str | None = None,
        dataset_fingerprint: str | None = None, task_hash: str | None = None
    ) -> dict[str, Any]:
        """Role → operator usage hierarchy (for a sunburst/treemap), colored by score."""
        from nirs4all_benchmarks.indexing import classify_role

        partition = _score_partition(scope, partition)
        metrics_view = self._score_view(
            partition, scope=scope, aggregation_level=aggregation_level, source_fold_id=source_fold_id,
            task_hash=task_hash)
        ds_sql, ds_params = _dataset_filter(dataset_fingerprint)
        rows = self.store.query(
            f"""SELECT pn.operator AS operator, pn.role AS role,
                      COUNT(DISTINCT pn.pipeline_dag_hash) AS n_pipes,
                      COUNT(DISTINCT mo.execution_hash) AS n_runs, COUNT(DISTINCT mo.score_context) AS n_score_contexts,
                      CASE WHEN COUNT(DISTINCT mo.score_context)=1 THEN MIN(mo.task_hash) END AS task_hash,
                      CASE WHEN COUNT(DISTINCT mo.score_context)=1 THEN MIN(mo.metric_unit) END AS metric_unit,
                      CASE WHEN COUNT(DISTINCT mo.score_context)=1 THEN AVG(mo.metric_value) END AS score
               FROM pipeline_nodes pn
               JOIN {metrics_view} mo ON mo.pipeline_dag_hash = pn.pipeline_dag_hash
               WHERE pn.operator IS NOT NULL AND pn.role != 'input'
                     AND mo.metric_name = ? AND mo.score_scope = ?{ds_sql} AND mo.score_validity = 'valid'
                     AND mo.execution_validity = 'valid'
               GROUP BY pn.operator""",
            [metric, scope, *ds_params],
        )
        for r in rows:
            r["role"] = classify_role(r["operator"], r["role"])
            r["operator_short"] = r["operator"].split(".")[-1]
            r["score_context_status"] = "single" if r["n_score_contexts"] == 1 else "mixed"
        rows.sort(key=lambda r: (r["role"], -(r["n_runs"] or 0)))
        return {"include_unreported": False, "partition": partition,
                "aggregation_level": aggregation_level, "source_fold_id": source_fold_id,
                "metric": metric, "scope": scope,
                "direction": self._direction(metric), "rows": rows,
                "score_context": _context_summary(self._selected_contexts(metrics_view, metric, scope,
                                                                             dataset_fingerprint),
                                                  sum(r["n_score_contexts"] != 1 for r in rows))}


    def unknown_context_rows(self, *, limit: int = 200) -> dict[str, Any]:
        """Unranked, non-score-ordered unknown metadata; never a leaderboard opt-in."""
        if self.store.query_one("SELECT 1 FROM v_run_metrics GROUP BY score_set_id, metric_name, partition, "
                                "aggregation_level, fold_id, source_fold_id HAVING COUNT(*) > 1 LIMIT 1"):
            raise ValueError("stored_score_context_ambiguous")
        rows = self.store.query("""SELECT mo.execution_hash, mo.run_condition_hash, mo.execution_status,
            mo.pipeline_label, mo.metric_name, mo.metric_value, o.metric_unit, mo.task_hash,
            mo.score_scope, mo.partition,
            mo.aggregation_level, mo.fold_id, mo.source_fold_id, mo.visibility
            FROM v_run_metrics mo
            JOIN metric_observations o ON o.score_set_id=mo.score_set_id AND o.metric_name=mo.metric_name
              AND o.partition IS mo.partition AND o.aggregation_level IS mo.aggregation_level
              AND o.fold_id IS mo.fold_id AND o.source_fold_id IS mo.source_fold_id
            JOIN dataset_fingerprints df ON df.dataset_fingerprint=mo.dataset_fingerprint
            WHERE df.privacy_level NOT IN ('private','restricted','anonymized')
              AND mo.visibility NOT IN ('private','restricted','anonymized')
              AND mo.execution_validity='valid'
              AND mo.oof_enforced IS NOT 0 AND mo.group_leakage_checked IS NOT 0 AND mo.nested_cv_safe IS NOT 0
              AND (mo.unsafe_flags_json IS NULL OR mo.unsafe_flags_json IN ('null','[]'))
              AND (mo.execution_status='unreported' OR mo.score_scope IS NULL OR mo.score_scope='unreported'
                   OR mo.partition IS NULL OR mo.partition='unreported' OR mo.aggregation_level IS NULL
                   OR mo.aggregation_level='unreported' OR mo.direction IS NULL OR mo.identity_rankable=0
                   OR (mo.fold_id IS NULL AND mo.source_fold_id IS NULL) OR mo.oof_enforced IS NULL
                   OR mo.group_leakage_checked IS NULL OR mo.nested_cv_safe IS NULL OR mo.unsafe_flags_json='null')
            ORDER BY mo.execution_hash, mo.metric_name, mo.fold_id, mo.source_fold_id LIMIT ?""", (limit,))
        return {"ranked":False, "rows":rows, "reason":"unknown_context", "score_context":_context_summary(rows)}
