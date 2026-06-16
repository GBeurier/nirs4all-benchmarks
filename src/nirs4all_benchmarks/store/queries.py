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

from nirs4all_benchmarks.scoring import direction_of
from nirs4all_benchmarks.store.arena_store import ArenaStore

_VALID_SCOPES = {"fold", "cv", "refit", "test", "view"}
_VALID_PARTITIONS = {"train", "validation", "test", "final"}


def _safe_scope(scope: str) -> str:
    if scope not in _VALID_SCOPES:
        raise ValueError(f"invalid scope '{scope}'")
    return scope


class Queries:
    """Read-only meta-analysis facade over an :class:`ArenaStore`."""

    def __init__(self, store: ArenaStore) -> None:
        self.store = store

    # ── catalogues ─────────────────────────────────────────────────────
    def overview(self) -> dict[str, Any]:
        s = self.store
        return {
            "datasets": s.count("dataset_fingerprints"),
            "dataset_cards": s.count("dataset_cards"),
            "tasks": s.count("task_specs"),
            "pipelines": s.count("pipeline_dags"),
            "operators": s.count("operator_specs"),
            "parameters": s.count("parameter_values"),
            "run_conditions": s.count("run_conditions"),
            "executions": s.count("executions"),
            "valid_executions": s.count("executions", "validity_status = 'valid'"),
            "quarantined_executions": s.count("executions", "validity_status = 'quarantined'"),
            "score_sets": s.count("score_sets"),
            "metric_observations": s.count("metric_observations"),
            "residual_sets": s.count("residual_sets"),
            "collections": s.count("collections"),
            "metrics": self.metrics_available(),
            "schema_version": int(s.get_meta("arena_schema_version") or 0),
        }

    def metrics_available(self) -> list[str]:
        rows = self.store.query("SELECT DISTINCT metric_name FROM metric_observations ORDER BY metric_name")
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
        dataset_fingerprint: str | None = None,
        task_hash: str | None = None,
        collection_id: str | None = None,
        include_quarantined: bool = False,
        limit: int = 200,
    ) -> dict[str, Any]:
        """Configurable leaderboard — no canonical baseline imposed (DESIGN.md §9.2)."""
        _safe_scope(scope)
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
                   mo.dataset_fingerprint,
                   AVG(mo.metric_value) AS mean, MIN(mo.metric_value) AS min, MAX(mo.metric_value) AS max,
                   COUNT(*) AS n_obs
            FROM v_run_metrics mo
            WHERE {' AND '.join(where)}
            GROUP BY mo.run_condition_hash
        """
        rows = self.store.query(sql, params)
        direction = direction_of(metric)
        # Unscored (NULL-mean) rows must always sink to the bottom, regardless of
        # direction — a +inf sentinel under reverse=True would otherwise rank them #1.
        worst = float("-inf") if direction == "max" else float("inf")
        rows.sort(key=lambda r: (r["mean"] if r["mean"] is not None else worst), reverse=(direction == "max"))
        for i, r in enumerate(rows[:limit], start=1):
            r["rank"] = i
        return {"metric": metric, "scope": scope, "direction": direction, "rows": rows[:limit]}

    # ── pipeline × dataset matrix ──────────────────────────────────────
    def matrix(self, *, metric: str = "rmse", scope: str = "cv", include_quarantined: bool = False) -> dict[str, Any]:
        _safe_scope(scope)
        where = ["mo.metric_name = ?", "mo.score_scope = ?", "mo.score_validity = 'valid'"]
        params: list[Any] = [metric, scope]
        if not include_quarantined:
            where.append("mo.execution_validity = 'valid'")
        rows = self.store.query(
            f"""
            SELECT mo.pipeline_dag_hash, mo.pipeline_label, mo.dataset_fingerprint,
                   AVG(mo.metric_value) AS value, COUNT(DISTINCT mo.execution_hash) AS coverage
            FROM v_run_metrics mo WHERE {' AND '.join(where)}
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
            }
        return {
            "metric": metric,
            "scope": scope,
            "direction": direction_of(metric),
            "datasets": [
                {"dataset_fingerprint": d, "label": (ds_meta.get(d) or {}).get("name") or d[:10]}
                for d in datasets
            ],
            "pipelines": [{"pipeline_dag_hash": p, "label": lbl} for p, lbl in pipelines.items()],
            "cells": [{"pipeline_dag_hash": k[0], "dataset_fingerprint": k[1], **v} for k, v in cells.items()],
        }

    # ── run explorer ───────────────────────────────────────────────────
    def run_explorer(
        self,
        *,
        metric: str = "rmse",
        scope: str = "cv",
        dataset_fingerprint: str | None = None,
        pipeline_dag_hash: str | None = None,
        operator: str | None = None,
        include_quarantined: bool = True,
        limit: int = 500,
    ) -> list[dict[str, Any]]:
        _safe_scope(scope)
        where = ["mo.metric_name = ?", "mo.score_scope = ?", "mo.score_validity = 'valid'"]
        params: list[Any] = [metric, scope]
        if dataset_fingerprint:
            where.append("mo.dataset_fingerprint = ?")
            params.append(dataset_fingerprint)
        if pipeline_dag_hash:
            where.append("mo.pipeline_dag_hash = ?")
            params.append(pipeline_dag_hash)
        if not include_quarantined:
            where.append("mo.execution_validity = 'valid'")
        op_join = ""
        if operator:
            op_join = "JOIN pipeline_nodes pn ON pn.pipeline_dag_hash = mo.pipeline_dag_hash AND pn.operator = ?"
            params = [operator, *params]
        sql = f"""
            SELECT mo.execution_hash, mo.run_condition_hash, mo.pipeline_dag_hash, mo.pipeline_label,
                   mo.main_model, mo.dataset_fingerprint, mo.execution_validity, mo.execution_status,
                   mo.producer_capsule, mo.time_ms, AVG(mo.metric_value) AS metric_value
            FROM v_run_metrics mo {op_join}
            WHERE {' AND '.join(where)}
            GROUP BY mo.execution_hash LIMIT ?
        """
        rows = self.store.query(sql, [*params, limit])
        # Sort best-first by the metric's direction (so the table opens on winners).
        worst = float("-inf") if direction_of(metric) == "max" else float("inf")
        rows.sort(
            key=lambda r: (r["metric_value"] if r["metric_value"] is not None else worst),
            reverse=(direction_of(metric) == "max"),
        )
        return rows

    # ── operator / parameter effects ──────────────────────────────────
    def operator_effect(self, *, metric: str = "rmse", scope: str = "cv") -> dict[str, Any]:
        """Score distribution per operator presence — what each operator does."""
        _safe_scope(scope)
        rows = self.store.query(
            """
            SELECT pn.operator AS operator, pn.role AS role, mo.metric_value AS value
            FROM pipeline_nodes pn
            JOIN v_run_metrics mo ON mo.pipeline_dag_hash = pn.pipeline_dag_hash
            WHERE mo.metric_name = ? AND mo.score_scope = ? AND mo.execution_validity = 'valid'
                  AND pn.operator IS NOT NULL
            """,
            [metric, scope],
        )
        groups: dict[str, list[float]] = {}
        roles: dict[str, str] = {}
        for r in rows:
            groups.setdefault(r["operator"], []).append(r["value"])
            roles[r["operator"]] = r["role"]
        series: list[dict[str, Any]] = [
            {
                "operator": op,
                "role": roles[op],
                "n": len(vals),
                "mean": statistics.fmean(vals),
                "median": statistics.median(vals),
                "stdev": statistics.pstdev(vals) if len(vals) > 1 else 0.0,
                "min": min(vals),
                "max": max(vals),
                "values": vals,
            }
            for op, vals in groups.items()
        ]
        series.sort(key=lambda s: s["mean"], reverse=(direction_of(metric) == "max"))
        return {"metric": metric, "scope": scope, "direction": direction_of(metric), "series": series}

    def parameter_effect(self, param_name: str, *, metric: str = "rmse", scope: str = "cv") -> dict[str, Any]:
        """Metric vs a (numeric or categorical) parameter — e.g. PLS n_components."""
        _safe_scope(scope)
        rows = self.store.query(
            """
            SELECT pv.numeric_value AS num, pv.value_json AS raw, pv.is_numeric AS is_numeric,
                   mo.metric_value AS value, mo.dataset_fingerprint AS df, mo.pipeline_label AS label
            FROM pipeline_node_params pnp
            JOIN parameter_values pv ON pv.param_value_hash = pnp.param_value_hash
            JOIN v_run_metrics mo ON mo.pipeline_dag_hash = pnp.pipeline_dag_hash
            WHERE pv.name = ? AND mo.metric_name = ? AND mo.score_scope = ? AND mo.execution_validity = 'valid'
            """,
            [param_name, metric, scope],
        )
        points = []
        for r in rows:
            value_label = r["num"] if r["is_numeric"] else json.loads(r["raw"])
            points.append(
                {"param": value_label, "numeric": r["num"], "metric_value": r["value"],
                 "dataset_fingerprint": r["df"], "pipeline_label": r["label"]}
            )
        return {"param_name": param_name, "metric": metric, "scope": scope,
                "direction": direction_of(metric), "points": points}

    # ── run detail ─────────────────────────────────────────────────────
    def run_detail(self, execution_hash: str) -> dict[str, Any] | None:
        s = self.store
        execution = s.get("executions", "execution_hash", execution_hash)
        if not execution:
            return None
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
            """SELECT ss.score_set_id, ss.scope, ss.validity_status, mo.metric_name, mo.metric_value,
                      mo.fold_id, mo.partition, mo.direction
               FROM score_sets ss JOIN metric_observations mo ON mo.score_set_id = ss.score_set_id
               WHERE ss.execution_hash = ? ORDER BY mo.metric_name, mo.fold_id""",
            (execution_hash,),
        )
        residual_set = s.query_one("SELECT * FROM residual_sets WHERE execution_hash = ?", (execution_hash,))
        graph = json.loads(pipeline["graph_json"]) if pipeline.get("graph_json") else None
        return {
            "execution": execution,
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

    def fold_scores(self, execution_hash: str, *, metric: str = "rmse") -> list[dict[str, Any]]:
        # Fold-level observations are identified by a non-null fold_id (scope lives
        # on score_sets, not on metric_observations).
        return self.store.query(
            """SELECT mo.fold_id, mo.partition, mo.metric_value
               FROM score_sets ss JOIN metric_observations mo ON mo.score_set_id = ss.score_set_id
               WHERE ss.execution_hash = ? AND mo.metric_name = ? AND mo.fold_id IS NOT NULL
                     AND ss.validity_status = 'valid'
               ORDER BY mo.fold_id""",
            (execution_hash, metric),
        )

    # ── residuals & complementarity ────────────────────────────────────
    def residuals(self, execution_hash: str, *, partition: str | None = None) -> list[dict[str, Any]]:
        rset = self.store.query_one("SELECT * FROM residual_sets WHERE execution_hash = ?", (execution_hash,))
        if not rset:
            return []
        rows = self.store.residuals.read(rset["residual_set_id"])
        if partition:
            rows = [r for r in rows if r["partition"] == partition]
        return rows

    def residual_compare(
        self, execution_a: str, execution_b: str, *, partition: str = "validation"
    ) -> dict[str, Any]:
        """Compare two runs' residuals on the *same* samples (sample-keyed join)."""
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
    def robustness(self, *, metric: str = "rmse", scope: str = "cv") -> list[dict[str, Any]]:
        """Variance of a pipeline's score across executions (folds/seeds/splits)."""
        _safe_scope(scope)
        rows = self.store.query(
            """SELECT mo.pipeline_dag_hash, mo.pipeline_label, mo.dataset_fingerprint, mo.metric_value AS value
               FROM v_run_metrics mo
               WHERE mo.metric_name = ? AND mo.score_scope = ? AND mo.execution_validity = 'valid'""",
            [metric, scope],
        )
        groups: dict[tuple[str, str], list[float]] = {}
        labels: dict[tuple[str, str], str] = {}
        for r in rows:
            key = (r["pipeline_dag_hash"], r["dataset_fingerprint"])
            groups.setdefault(key, []).append(r["value"])
            labels[key] = r["pipeline_label"] or r["pipeline_dag_hash"][:10]
        out: list[dict[str, Any]] = []
        for (pdh, df), vals in groups.items():
            out.append({
                "pipeline_dag_hash": pdh,
                "dataset_fingerprint": df,
                "label": labels[(pdh, df)],
                "n": len(vals),
                "mean": statistics.fmean(vals),
                "stdev": statistics.pstdev(vals) if len(vals) > 1 else 0.0,
                "cv_pct": (statistics.pstdev(vals) / statistics.fmean(vals) * 100.0)
                if len(vals) > 1 and statistics.fmean(vals) else 0.0,
            })
        out.sort(key=lambda r: r["stdev"])
        return out
