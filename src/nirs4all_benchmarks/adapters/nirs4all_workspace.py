"""Adapter A — a nirs4all workspace (``store.sqlite`` + ``arrays/*.parquet``) → exports.

Reads the *documented* frozen workspace schema (PERSISTENCE_FORMATS.md §2.1): the
``pipelines`` / ``predictions`` tables and the per-dataset prediction parquet. One
:class:`ArenaRunExport` is produced per pipeline run, aggregating its predictions
into scores + residuals. **Weights (``artifacts/``) are ignored entirely.**

Degraded by design (§4.4): nirs4all writes *positional* ``sample_indices`` with no
stable id, so residuals are emitted with ``key="positional"`` and synthetic
``pos_<idx>`` sample ids — queryable per-run, excluded from cross-run sample
comparison until an io ordinal→SampleId sidecar exists. Missing safety declarations
remain null. These are Arena record identities and metadata, not scientific proof.
"""

from __future__ import annotations

import hashlib
import os
import sqlite3
from collections.abc import Iterator
from pathlib import Path
from typing import Any

from nirs4all_benchmarks import ARENA_EXPORT_SCHEMA_VERSION
from nirs4all_benchmarks.contract import ArenaRunExport
from nirs4all_benchmarks.contract.folds import is_pseudo_fold
from nirs4all_benchmarks.contract.strict_json import strict_loads
from nirs4all_benchmarks.identity import canonical_json, fingerprint
from nirs4all_benchmarks.identity.hashing import is_hash
from nirs4all_benchmarks.scoring import METRIC_DIRECTION


class WorkspaceAdapterError(ValueError):
    """A workspace refusal with a stable, input-independent public code."""

    def __init__(self, code: str):
        self.code = code
        super().__init__(code)


def _unresolved_generator(value: Any) -> bool:
    if isinstance(value, dict):
        return bool(
            {"_or_", "_range_", "_log_range_", "_grid_", "_zip_", "_chain_", "_sample_", "_cartesian_"}.intersection(
                value
            )
        ) or any(_unresolved_generator(child) for child in value.values())
    return isinstance(value, list) and any(_unresolved_generator(child) for child in value)


def _workspace_partition(value: Any) -> str:
    # Owner: storage/workspace_store.py:save_prediction and
    # controllers/models/base_model.py:_assemble_prediction_result use train/val/test.
    if not isinstance(value, str):
        raise ValueError("workspace_partition_missing_or_unsupported")
    partition = value.strip().lower()
    partition = "validation" if partition == "val" else partition
    if partition not in {"train", "validation", "test", "final"}:
        raise ValueError("workspace_partition_missing_or_unsupported")
    return partition


def _workspace_execution(pipe: dict[str, Any], execution_id: str) -> dict[str, Any]:
    execution = {"execution_id": execution_id,
                 **{k: pipe[k] for k in ("status", "terminal_status") if pipe.get(k) is not None}}
    # Owner: workspace_store.py:complete_pipeline / store_queries.py:COMPLETE_PIPELINE.
    # Only the actual stored declaration supplies completion; fold counts do not.
    if execution.get("status") == "completed":
        if execution.get("terminal_status") not in {None, "completed"}:
            raise ValueError("workspace_status_contradiction")
        execution.update(status="ok", terminal_status="completed")
    elif execution.get("status") not in {None, "ok", "failed", "cancelled"}:
        raise ValueError("workspace_nonterminal_or_unsupported_status")
    return execution


def _table_columns(conn: sqlite3.Connection, table: str) -> set[str]:
    try:
        return {r[1] for r in conn.execute(f"PRAGMA table_info({table})").fetchall()}
    except sqlite3.OperationalError:
        return set()


def _parse_pipeline_steps(expanded_config: str | None) -> list[Any]:
    if not expanded_config:
        return []
    try:
        data = strict_loads(expanded_config)
    except (ValueError, TypeError):
        raise WorkspaceAdapterError("workspace_pipeline_config_malformed") from None
    if _unresolved_generator(data):
        raise WorkspaceAdapterError("workspace_unresolved_generator")
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
        self.array_sha256: dict[str, str] = {}

    def _arrays_for_dataset(self, dataset_name: str) -> dict[str, dict[str, Any]]:
        """Index prediction parquet rows by ``prediction_id`` for a dataset."""
        if dataset_name in {".", ".."} or any(c in dataset_name for c in ("/", "\\", ":")):
            raise ValueError("unsafe_workspace_array_path")
        path = self.arrays_dir / f"{dataset_name}.parquet"
        if self.arrays_dir.is_symlink() or path.is_symlink():
            raise ValueError("unsafe_workspace_array_path")
        if not path.exists():
            return {}
        import pyarrow as pa
        import pyarrow.parquet as pq
        flags = os.O_RDONLY | getattr(os, "O_NOATIME", 0) | getattr(os, "O_NOFOLLOW", 0)
        with os.fdopen(os.open(path, flags), "rb") as stream:
            content = stream.read()
        self.array_sha256[dataset_name] = hashlib.sha256(content).hexdigest()
        rows = pq.read_table(pa.BufferReader(content)).to_pylist()
        indexed = {}
        for row in rows:
            key = row.get("prediction_id")
            if not isinstance(key, str) or not key or key in indexed:
                raise WorkspaceAdapterError("workspace_array_identity_ambiguous")
            indexed[key] = row
        return indexed

    def iter_exports(self) -> Iterator[ArenaRunExport]:
        from nirs4all_benchmarks.ingestion.resolve import resolve_identities

        for path in (self.db_path, self.root, Path(str(self.db_path)+"-wal"), Path(str(self.db_path)+"-shm")):
            if path.is_symlink():
                raise WorkspaceAdapterError("workspace_store_not_quiescent")
        wal = Path(str(self.db_path)+"-wal")
        if wal.exists() and wal.stat().st_size:
            raise WorkspaceAdapterError("workspace_store_not_quiescent")
        flags = os.O_RDONLY | getattr(os, "O_NOATIME", 0) | getattr(os, "O_NOFOLLOW", 0)
        with os.fdopen(os.open(self.db_path, flags), "rb") as stream:
            content = stream.read()
            self.source_sha256 = hashlib.sha256(content).hexdigest()
        image = bytearray(content)
        image[18:20] = b"\x01\x01"
        # Immutable private in-memory SQLite snapshot: no source opens by SQLite,
        # hence no source sidecars or atime updates. A live source WAL was refused.
        conn = sqlite3.connect("file:arena-workspace-snapshot?mode=memory&cache=private&immutable=1", uri=True)
        deserialize = getattr(conn, "deserialize", None)
        if deserialize is None:
            conn.close()
            raise WorkspaceAdapterError("workspace_snapshot_unsupported")
        deserialize(bytes(image))
        conn.row_factory = sqlite3.Row
        try:
            pipe_cols = _table_columns(conn, "pipelines")
            if not pipe_cols:
                return
            conn.execute("BEGIN")
            pipelines = conn.execute("SELECT * FROM pipelines").fetchall()
            exports = []
            seen: dict[str, tuple[set[str | None], bool]] = {}
            for pipe in pipelines:
                row = dict(pipe)
                unresolved = _unresolved_generator(_parse_pipeline_steps(
                    row.get("expanded_config") or row.get("original_template")))
                if unresolved:
                    raise WorkspaceAdapterError("workspace_unresolved_generator")
                for export in self._export_for_pipeline(conn, row):
                    identity = resolve_identities(export).run_condition_hash
                    prior = seen.get(identity)
                    names = (prior[0] if prior else set()) | {row.get("name")}
                    if len(names) > 1:
                        raise WorkspaceAdapterError("workspace_identity_collision")
                    seen[identity] = (names, unresolved or bool(prior and prior[1]))
                    exports.append(export)
            # Preflight the whole workspace before any caller can ingest a prefix.
            if wal.exists() and wal.stat().st_size:
                raise WorkspaceAdapterError("workspace_store_not_quiescent")
            yield from exports
        finally:
            conn.close()

    def _export_for_pipeline(self, conn: sqlite3.Connection, pipe: dict[str, Any]) -> Iterator[ArenaRunExport]:
        pipeline_id = pipe.get("pipeline_id")
        dataset_name = pipe.get("dataset_name")
        if not isinstance(dataset_name, str) or not dataset_name:
            raise WorkspaceAdapterError("workspace_dataset_label_required")
        dataset_hash = pipe.get("dataset_hash")
        steps = _parse_pipeline_steps(pipe.get("expanded_config") or pipe.get("original_template"))
        if not steps:
            raise WorkspaceAdapterError("workspace_pipeline_structure_required")
        rows = conn.execute("SELECT * FROM predictions WHERE pipeline_id = ?", (pipeline_id,)).fetchall()
        preds = [dict(p) for p in rows]
        if not preds and not (is_hash(pipe.get("task_hash")) and is_hash(pipe.get("dataset_variant_hash"))):
            raise ValueError("workspace_failed_run_identity_required")
        arrays = self._arrays_for_dataset(dataset_name)
        if not is_hash(dataset_hash):
            raise ValueError("workspace_dataset_identity_required")
        dataset_fingerprint = dataset_hash

        # A nirs4all pipeline can hold several models / branches (DESIGN.md §31);
        # the prediction natural key includes (model_name, branch_id). Emit one
        # ArenaRunExport per (model, branch) so scores and positional residuals from
        # different models never mix into one run.
        groups: dict[tuple[str, str], list[dict[str, Any]]] = {}
        for p in preds:
            branch = p.get("branch_id")
            branch_key = str(branch) if branch is not None else ""
            groups.setdefault((str(p.get("model_name") or ""), branch_key), []).append(p)
        if not groups:
            groups[("", "")] = []
        multi = len(groups) > 1

        for (model_name, branch_id), gpreds in sorted(groups.items()):
            if len({p.get("chain_id") for p in gpreds}) > 1:
                raise ValueError("workspace_multiple_chains_unsupported")
            first = gpreds[0] if gpreds else {}
            source_task = first.get("task_type") or pipe.get("task_type")
            if source_task is None:
                raise WorkspaceAdapterError("workspace_task_type_unreported")
            task_type = {"binary_classification": "binary", "multiclass_classification": "multiclass"}.get(
                source_task, source_task
            )
            if task_type == "classification":
                raise WorkspaceAdapterError("workspace_classification_task_unsupported")
            if task_type not in {"regression", "binary", "multiclass", "multilabel"}:
                raise WorkspaceAdapterError("workspace_task_type_unsupported")
            if any(p.get("task_type") not in {None, source_task} for p in gpreds):
                raise WorkspaceAdapterError("workspace_task_type_conflict")
            residual_rows, observations = self._collect_results(gpreds, arrays, task_type)

            target_rows = []
            identity_predictions = [p for p in gpreds if not is_pseudo_fold(p.get("fold_id"))
                                    or str(p.get("fold_id")).strip() == "final"]
            target_context_complete = bool(identity_predictions)
            for prediction in sorted(
                identity_predictions, key=lambda p: (str(p.get("fold_id")), str(p.get("partition")))
            ):
                array = arrays.get(str(prediction.get("prediction_id")))
                if not array or not array.get("sample_indices") or array.get("y_true") is None:
                    target_context_complete = False
                    continue
                if len(array["sample_indices"]) != len(array["y_true"]):
                    raise WorkspaceAdapterError("workspace_target_context_malformed")
                if any(target is None for target in array["y_true"]):
                    target_context_complete = False
                for index, target in zip(array["sample_indices"], array["y_true"], strict=True):
                    target_rows.append([_workspace_partition(prediction.get("partition")),
                                        {"type":type(index).__name__, "value":index},
                                        {"type":type(target).__name__, "value":target}])
            task_inputs = {
                "namespace": "arena.task/2",
                "dataset_name": dataset_name,
                "dataset_fingerprint": dataset_fingerprint,
                "task_type": task_type,
                "target_context_sha256": fingerprint(target_rows) if target_context_complete and target_rows else None,
            }
            if not target_context_complete or not target_rows:
                task_inputs["opaque_record"] = fingerprint({"workspace_sha256":self.source_sha256,
                                                            "array_sha256":self.array_sha256.get(dataset_name),
                                                            "pipeline_id":pipeline_id, "model":model_name,
                                                            "branch":branch_id})
            task_hash = pipe.get("task_hash") or fingerprint(task_inputs)
            variant_inputs = {"namespace":"arena.variant/2", "dataset_fingerprint":dataset_fingerprint,
                              "task_hash":task_hash}
            variant_hash = pipe.get("dataset_variant_hash") or fingerprint(variant_inputs)
            basis = {
                "dataset_fingerprint": {
                    "kind": "owner_reported",
                    "owner": "nirs4all",
                    "inputs": {"source": "pipelines.dataset_hash"},
                },
                "task_hash": (
                    {"kind": "owner_reported", "owner": "nirs4all"}
                    if pipe.get("task_hash")
                    else {"kind": "arena_derived", "inputs": task_inputs}
                ),
                "dataset_variant_hash": (
                    {"kind": "owner_reported", "owner": "nirs4all"}
                    if pipe.get("dataset_variant_hash")
                    else {"kind": "arena_derived", "inputs": variant_inputs}
                ),
            }
            # Append the run's model as a terminal node so each (model, branch) has a
            # distinct, topology-aware pipeline_dag_hash.
            nodes = _steps_to_nodes(steps)
            if model_name or branch_id:
                nodes.append({
                    "node_id": "model_terminal", "role": "model", "operator": model_name or None,
                    "branch_path": [branch_id] if branch_id else [],
                })
            label = pipe.get("name") or str(pipeline_id)
            if multi:
                label = f"{label} [{model_name}{'/' + branch_id if branch_id else ''}]"
            is_classification = task_type != "regression"

            manifest: dict[str, Any] = {
                "arena_export_schema_version": ARENA_EXPORT_SCHEMA_VERSION,
                "identity_basis": basis,
                "producer": {"capsule": "nirs4all-workspace", "nirs4all_version": None},
                "dataset": {
                    "dataset_fingerprint": dataset_fingerprint,
                    "dataset_card": {"name": dataset_name, "task_type": task_type},
                    "visibility": pipe.get("visibility") or "unreported",
                    "visibility_source": pipe.get("visibility_source") or "unreported",
                    # Prediction n_samples is a partition size, not dataset cardinality.
                    "n_samples": pipe.get("n_samples"),
                    "n_features": first.get("n_features"),
                },
                "task": {
                    "task_hash": task_hash,
                    "task_type": task_type,
                    "target_name": pipe.get("target_name"),
                    "target_hash": pipe.get("target_hash"),
                },
                "dataset_variant": {
                    "dataset_variant_hash": variant_hash,
                    "variant_spec": {"namespace": "arena.owner-record/2"},
                },
                "pipeline": {
                    "graph": {"nodes": nodes},
                    "nodes": nodes,
                    "nirs4all_identity_hash": (
                        pipe.get("nirs4all_identity_hash")
                    ),
                    "human_label": label,
                },
                "split": {"method": "unreported", "params": {}},
                "cv": {"method": "unreported", "n_folds": pipe.get("expected_fold_count")},
                "rng": {"root_seed": None, "derivation": "unknown"},
                "refit": {"strategy": pipe.get("refit_strategy") or "unreported"},
                "execution": {**_workspace_execution(pipe, f"{pipeline_id}:{model_name}:{branch_id}"),
                              "owner_pipeline_name":pipe.get("name")},
                "provenance": {"workspace_source":{"sha256":self.source_sha256, "pipeline_id":pipeline_id,
                                                    "array_sha256":self.array_sha256.get(dataset_name)}},
                # Honest: the workspace store carries no OOF attestation.
                "leakage_attestation": {},
                "scores": {"score_version": pipe.get("score_version") or "unreported",
                           "observations": observations, "origin": "producer_reported"},
                "residuals": {
                    "key": "positional",
                    "pseudonymized": False,
                    "publishable": {"y_true": True, "y_pred": True, "residual": True,
                                    "y_proba": is_classification},
                    "inline": residual_rows,
                },
            }
            from nirs4all_benchmarks.ingestion.offline import validate_metadata
            try:
                model = ArenaRunExport.model_validate(manifest)
            except ValueError:
                raise WorkspaceAdapterError("workspace_metadata_unsupported") from None
            validation = validate_metadata(model)
            if validation.status != "metadata_valid":
                raise WorkspaceAdapterError("workspace_metadata_refused:" + validation.issues[0]["code"])
            model.arena_export_hash = validation.arena_export_hash
            yield model

    def _collect_results(
        self,
        preds: list[dict[str, Any]],
        arrays: dict[str, dict[str, Any]],
        task_type: str,
    ) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
        residual_rows: list[dict[str, Any]] = []
        observations: list[dict[str, Any]] = []
        residual_keys: set[tuple[str, str, str | None, str]] = set()
        is_regression = task_type == "regression"
        for p in preds:
            raw_fold_id = p.get("fold_id")
            fold_id = str(raw_fold_id).strip() if raw_fold_id is not None else None
            pseudo = is_pseudo_fold(fold_id)
            refit = fold_id in {"final", "final_agg"}
            partition = _workspace_partition(p.get("partition"))
            # Aggregate scores stay inspectable without entering the CV mean.
            scope = "refit" if refit else "view" if pseudo else "test" if partition == "test" else "cv"
            # Scores: from multi-metric JSON or val/test/train columns.
            for name, value in _iter_pred_scores(p).items():
                observations.append(
                    {
                        "metric_name": name,
                        "metric_value": value,
                        "direction": METRIC_DIRECTION.get(name.lower()),
                        "direction_source": "registry" if name.lower() in METRIC_DIRECTION else "unreported",
                        "scope": scope,
                        "fold_id": None if pseudo else fold_id,
                        "source_fold_id": str(raw_fold_id) if pseudo else None,
                        "partition": partition,
                        "aggregation_level": "group" if fold_id is not None and fold_id.endswith("_agg") else "sample",
                        "n_samples": p.get("n_samples"),
                    }
                )
            # Residuals: from the prediction parquet (positional sample_indices).
            prediction_id = p.get("prediction_id")
            arr = arrays.get(str(prediction_id)) if prediction_id is not None else None
            if not arr or (pseudo and fold_id != "final"):
                continue
            if ((arr.get("partition") is not None and _workspace_partition(arr["partition"]) != partition)
                    or (arr.get("fold_id") is not None and str(arr["fold_id"]).strip() != fold_id)):
                raise ValueError("workspace_array_context_mismatch")
            y_true = arr.get("y_true") or []
            y_pred = arr.get("y_pred") or []
            indices = arr.get("sample_indices")
            if (not isinstance(indices, list) or len(indices) != len(y_pred)
                    or (y_true and len(y_true) != len(indices))
                    or any(isinstance(idx, bool) or not isinstance(idx, int) or idx < 0 for idx in indices)):
                raise ValueError("workspace_residual_positions_unsupported")
            proba_vectors = _reshape_proba(arr.get("y_proba"), arr.get("y_proba_shape"), len(indices)) \
                if not is_regression else None
            for j, idx in enumerate(indices):
                yt = y_true[j] if j < len(y_true) else None
                yp = y_pred[j] if j < len(y_pred) else None
                residual = (yt - yp) if (is_regression and yt is not None and yp is not None) else None
                row: dict[str, Any] = {
                    "sample_id": f"pos_{idx}",
                    "scope": scope,
                    "fold_id": None if pseudo else fold_id,
                    "partition": partition,
                    "y_true": yt,
                    "y_pred": yp,
                    "residual": residual,
                }
                key = (row["sample_id"], scope, row["fold_id"], partition)
                if key in residual_keys:
                    raise ValueError("workspace_residual_key_ambiguous")
                residual_keys.add(key)
                if proba_vectors is not None and j < len(proba_vectors):
                    row["y_proba"] = proba_vectors[j]
                residual_rows.append(row)
        return residual_rows, observations


def _reshape_proba(flat: Any, shape: Any, n_rows: int) -> list[list[float]] | None:
    """Reshape a flat ``y_proba`` list into per-sample vectors using ``y_proba_shape``.

    The owner stores a flat list and explicit [n, k] shape. Absent probabilities
    stay absent; malformed or unsupported nonempty values refuse without loss.
    """
    if flat is None or flat == []:
        return None
    if (not isinstance(flat, (list, tuple))
            or any(isinstance(x, bool) or not isinstance(x, (int, float)) for x in flat)
            or not isinstance(shape, (list, tuple)) or len(shape) != 2
            or any(type(x) is not int for x in shape) or shape[0] != n_rows or shape[1] < 1
            or len(flat) != shape[0]*shape[1]):
        raise WorkspaceAdapterError("workspace_probability_shape_unsupported")
    values = [float(x) for x in flat]
    k = shape[1]
    return [values[i*k:(i+1)*k] for i in range(n_rows)]


def _iter_pred_scores(pred: dict[str, Any]) -> dict[str, float | None]:
    """Read owner partition-keyed scores or the legacy flat metric map.

    Owner: workspace_store.py:save_prediction documents scores[partition][metric];
    base_model.py persists the full map on each separate partition row.
    """
    out: dict[str, float | None] = {}
    partition = _workspace_partition(pred.get("partition"))
    raw = pred.get("scores")
    if raw:
        try:
            parsed = strict_loads(raw) if isinstance(raw, str) else raw
        except (ValueError, TypeError) as exc:
            raise ValueError("workspace_scores_malformed") from exc
        if not isinstance(parsed, dict):
            raise ValueError("workspace_scores_unsupported")
        if any(isinstance(v, dict) for v in parsed.values()):
            matching = [v for k, v in parsed.items() if _workspace_partition(k) == partition]
            if len(matching) != 1 or not isinstance(matching[0], dict):
                raise ValueError("workspace_score_partition_missing_or_ambiguous")
            parsed = matching[0]
        for k, v in parsed.items():
            if not isinstance(k, str) or isinstance(v, bool) or (v is not None and not isinstance(v, (int, float))):
                raise ValueError("workspace_score_value_unsupported")
            name = k.lower()
            if name in out:
                raise ValueError("workspace_score_metric_ambiguous")
            out[name] = float(v) if v is not None else None
    if not out:
        metric = pred.get("metric")
        if not isinstance(metric, str) or not metric:
            raise WorkspaceAdapterError("workspace_metric_unreported")
        metric = metric.lower()
        column = {"validation": "val_score", "test": "test_score", "train": "train_score"}.get(partition)
        value = pred.get(column) if column else None
        if value is not None:
            if isinstance(value, bool) or not isinstance(value, (int, float)):
                raise ValueError("workspace_score_value_unsupported")
            out[metric] = float(value)
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
