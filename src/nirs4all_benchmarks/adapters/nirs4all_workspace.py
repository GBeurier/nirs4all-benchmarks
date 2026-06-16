"""Adapter A — a nirs4all workspace (``store.sqlite`` + ``arrays/*.parquet``) → exports.

Reads the *documented* frozen workspace schema (PERSISTENCE_FORMATS.md §2.1): the
``pipelines`` / ``predictions`` tables and the per-dataset prediction parquet. One
:class:`ArenaRunExport` is produced per pipeline run, aggregating its predictions
into scores + residuals. **Weights (``artifacts/``) are ignored entirely.**

Degraded by design (§4.4): nirs4all writes *positional* ``sample_indices`` with no
stable id, so residuals are emitted with ``key="positional"`` and synthetic
``pos_<idx>`` sample ids — queryable per-run, excluded from cross-run sample
comparison until an io ordinal→SampleId sidecar exists. ``oof_enforced`` is reported
honestly as ``False`` (the workspace store carries no OOF attestation).
"""

from __future__ import annotations

import json
import sqlite3
from collections.abc import Iterator
from pathlib import Path
from typing import Any

from nirs4all_benchmarks.contract import ArenaRunExport
from nirs4all_benchmarks.identity import canonical_json, fingerprint
from nirs4all_benchmarks.identity.hashing import export_hash as compute_export_hash
from nirs4all_benchmarks.identity.pipeline_dag import nirs4all_identity_hash
from nirs4all_benchmarks.scoring import direction_of


def _table_columns(conn: sqlite3.Connection, table: str) -> set[str]:
    try:
        return {r[1] for r in conn.execute(f"PRAGMA table_info({table})").fetchall()}
    except sqlite3.OperationalError:
        return set()


def _parse_pipeline_steps(expanded_config: str | None) -> list[Any]:
    if not expanded_config:
        return []
    try:
        data = json.loads(expanded_config)
    except (json.JSONDecodeError, TypeError):
        return []
    if isinstance(data, list):
        return data
    if isinstance(data, dict):
        for key in ("pipeline", "steps"):
            if isinstance(data.get(key), list):
                return data[key]
    return []


class WorkspaceAdapter:
    """Iterate a nirs4all workspace, yielding one ``ArenaRunExport`` per pipeline."""

    def __init__(self, workspace_dir: str | Path) -> None:
        self.root = Path(workspace_dir)
        self.db_path = self.root / "store.sqlite"
        if not self.db_path.exists():
            raise FileNotFoundError(f"no store.sqlite under {self.root}")
        self.arrays_dir = self.root / "arrays"

    def _arrays_for_dataset(self, dataset_name: str) -> dict[str, dict[str, Any]]:
        """Index prediction parquet rows by ``prediction_id`` for a dataset."""
        path = self.arrays_dir / f"{dataset_name}.parquet"
        if not path.exists():
            return {}
        import pyarrow.parquet as pq

        rows = pq.read_table(path).to_pylist()
        return {r["prediction_id"]: r for r in rows if r.get("prediction_id")}

    def iter_exports(self) -> Iterator[ArenaRunExport]:
        conn = sqlite3.connect(str(self.db_path))
        conn.row_factory = sqlite3.Row
        try:
            pipe_cols = _table_columns(conn, "pipelines")
            if not pipe_cols:
                return
            pipelines = conn.execute("SELECT * FROM pipelines").fetchall()
            for pipe in pipelines:
                yield from self._export_for_pipeline(conn, dict(pipe))
        finally:
            conn.close()

    def _export_for_pipeline(self, conn: sqlite3.Connection, pipe: dict[str, Any]) -> Iterator[ArenaRunExport]:
        pipeline_id = pipe.get("pipeline_id")
        dataset_name = pipe.get("dataset_name") or "dataset"
        dataset_hash = pipe.get("dataset_hash")
        steps = _parse_pipeline_steps(pipe.get("expanded_config") or pipe.get("original_template"))
        rows = conn.execute("SELECT * FROM predictions WHERE pipeline_id = ?", (pipeline_id,)).fetchall()
        if not rows:
            return
        preds = [dict(p) for p in rows]
        arrays = self._arrays_for_dataset(dataset_name)
        dataset_fingerprint = fingerprint({"nirs4all_dataset": dataset_name, "dataset_hash": dataset_hash})

        # A nirs4all pipeline can hold several models / branches (DESIGN.md §31);
        # the prediction natural key includes (model_name, branch_id). Emit one
        # ArenaRunExport per (model, branch) so scores and positional residuals from
        # different models never mix into one run.
        groups: dict[tuple[str, str], list[dict[str, Any]]] = {}
        for p in preds:
            groups.setdefault((str(p.get("model_name") or ""), str(p.get("branch_id") or "")), []).append(p)
        multi = len(groups) > 1

        for (model_name, branch_id), gpreds in sorted(groups.items()):
            task_type = gpreds[0].get("task_type") or "regression"
            residual_rows, observations = self._collect_results(gpreds, arrays, task_type)
            fold_ids = {p.get("fold_id") for p in gpreds if p.get("fold_id") not in (None, "final", "_agg")}

            # Append the run's model as a terminal node so each (model, branch) has a
            # distinct, topology-aware pipeline_dag_hash.
            nodes = _steps_to_nodes(steps)
            nodes.append({
                "node_id": "model_terminal", "role": "model", "operator": model_name or "model",
                "branch_path": [branch_id] if branch_id else [],
            })
            label = pipe.get("name") or str(pipeline_id)
            if multi:
                label = f"{label} [{model_name}{'/' + branch_id if branch_id else ''}]"
            is_classification = task_type != "regression"

            manifest: dict[str, Any] = {
                "arena_export_schema_version": 1,
                "producer": {"capsule": "nirs4all-workspace", "nirs4all_version": None},
                "dataset": {
                    "dataset_fingerprint": dataset_fingerprint,
                    "dataset_card": {"name": dataset_name, "task_type": task_type},
                    "visibility": "public",
                    "n_samples": gpreds[0].get("n_samples"),
                    "n_features": gpreds[0].get("n_features"),
                },
                "task": {
                    "task_hash": fingerprint({"d": dataset_fingerprint, "t": task_type}),
                    "task_type": task_type,
                    "target_name": "target",
                },
                "dataset_variant": {
                    "dataset_variant_hash": fingerprint({"d": dataset_fingerprint, "v": "all"}),
                    "variant_spec": {"size": "all", "aggregation": "none"},
                },
                "pipeline": {
                    "graph": {"nodes": nodes},
                    "nodes": nodes,
                    "nirs4all_identity_hash": (
                        nirs4all_identity_hash([*steps, model_name, branch_id]) if steps else None
                    ),
                    "human_label": label,
                },
                "split": {"method": "predefined", "params": {}},
                "cv": {"method": "kfold", "n_folds": len(fold_ids) or None, "within_train_only": True},
                "rng": {"root_seed": None, "derivation": "unknown"},
                "refit": {"strategy": "global_best_params_full_train"
                          if any(p.get("fold_id") == "final" for p in gpreds) else "none"},
                "execution": {"execution_id": f"{pipeline_id}:{model_name}:{branch_id}", "status": "ok"},
                # Honest: the workspace store carries no OOF attestation.
                "leakage_attestation": {"oof_enforced": False, "group_leakage_checked": False,
                                        "nested_cv_safe": True, "unsafe_flags": ["nirs4all_store_no_oof_attestation"]},
                "scores": {"score_version": "1.0", "observations": observations},
                "residuals": {
                    "key": "positional",
                    "pseudonymized": False,
                    "publishable": {"y_true": True, "y_pred": True, "residual": True,
                                    "y_proba": is_classification},
                    "inline": residual_rows,
                },
            }
            manifest["arena_export_hash"] = compute_export_hash(manifest)
            yield ArenaRunExport.model_validate(manifest)

    def _collect_results(
        self,
        preds: list[dict[str, Any]],
        arrays: dict[str, dict[str, Any]],
        task_type: str,
    ) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
        residual_rows: list[dict[str, Any]] = []
        observations: list[dict[str, Any]] = []
        is_regression = task_type == "regression"
        for p in preds:
            fold_id = p.get("fold_id")
            partition = (p.get("partition") or "validation").lower()
            scope = "test" if partition == "test" else ("refit" if fold_id == "final" else "cv")
            # Scores: from multi-metric JSON or val/test/train columns.
            for name, value in _iter_pred_scores(p).items():
                observations.append(
                    {
                        "metric_name": name,
                        "metric_value": value,
                        "direction": direction_of(name),
                        "scope": scope,
                        "fold_id": None if fold_id in ("final", "_agg") else fold_id,
                        "partition": "final" if fold_id == "final" else partition,
                        "aggregation_level": "sample",
                    }
                )
            # Residuals: from the prediction parquet (positional sample_indices).
            prediction_id = p.get("prediction_id")
            arr = arrays.get(str(prediction_id)) if prediction_id is not None else None
            if not arr or fold_id == "_agg":
                continue
            y_true = arr.get("y_true") or []
            y_pred = arr.get("y_pred") or []
            indices = arr.get("sample_indices") or list(range(len(y_pred)))
            proba_vectors = _reshape_proba(arr.get("y_proba"), arr.get("y_proba_shape"), len(indices)) \
                if not is_regression else None
            for j, idx in enumerate(indices):
                yt = y_true[j] if j < len(y_true) else None
                yp = y_pred[j] if j < len(y_pred) else None
                residual = (yt - yp) if (is_regression and yt is not None and yp is not None) else None
                row: dict[str, Any] = {
                    "sample_id": f"pos_{idx}",
                    "scope": scope,
                    "fold_id": None if fold_id in ("final", "_agg") else fold_id,
                    "partition": "final" if fold_id == "final" else partition,
                    "y_true": yt,
                    "y_pred": yp,
                    "residual": residual,
                }
                if proba_vectors is not None and j < len(proba_vectors):
                    row["y_proba"] = proba_vectors[j]
                residual_rows.append(row)
        return residual_rows, observations


def _reshape_proba(flat: Any, shape: Any, n_rows: int) -> list[list[float]] | None:
    """Reshape a flat ``y_proba`` list into per-sample vectors using ``y_proba_shape``.

    The nirs4all parquet stores ``y_proba`` flat with a ``[n, k]`` ``y_proba_shape``
    (PERSISTENCE_FORMATS.md §2.1). Returns ``None`` when probabilities are absent or
    the shape is inconsistent (so the caller simply omits them).
    """
    if not flat:
        return None
    try:
        values = [float(x) for x in flat]
    except (TypeError, ValueError):
        return None
    k = None
    if isinstance(shape, (list, tuple)) and len(shape) == 2 and shape[1]:
        k = int(shape[1])
    elif n_rows and len(values) % n_rows == 0:
        k = len(values) // n_rows
    if not k or len(values) < k * n_rows:
        return None
    return [values[i * k:(i + 1) * k] for i in range(n_rows)]


def _iter_pred_scores(pred: dict[str, Any]) -> dict[str, float]:
    out: dict[str, float] = {}
    raw = pred.get("scores")
    if raw:
        try:
            parsed = json.loads(raw) if isinstance(raw, str) else raw
            if isinstance(parsed, dict):
                for k, v in parsed.items():
                    if isinstance(v, (int, float)):
                        out[str(k).lower()] = float(v)
        except (json.JSONDecodeError, TypeError):
            pass
    if not out:
        metric = (pred.get("metric") or "score").lower()
        if pred.get("val_score") is not None:
            out[metric] = float(pred["val_score"])
    return out


def _steps_to_nodes(steps: list[Any]) -> list[dict[str, Any]]:
    nodes = []
    for i, step in enumerate(steps):
        node: dict[str, Any] = {"node_id": f"n{i}"}
        if isinstance(step, dict):
            if "model" in step:
                node.update({"role": "model", "operator": _op_name(step["model"]), "params": step.get("params", {})})
            elif "class" in step:
                node.update({"role": "transform", "operator": step["class"], "params": step.get("params", {})})
            else:
                node.update({"role": "transform", "operator": _op_name(step)})
        else:
            node.update({"role": "transform", "operator": str(step)})
        nodes.append(node)
    return nodes


def _op_name(value: Any) -> str:
    if isinstance(value, dict):
        return str(value.get("class") or value.get("operator") or value.get("name") or canonical_json(value))
    return str(value)


def workspace_to_exports(workspace_dir: str | Path) -> list[ArenaRunExport]:
    """Convenience: materialize all exports from a workspace into a list."""
    return list(WorkspaceAdapter(workspace_dir).iter_exports())
