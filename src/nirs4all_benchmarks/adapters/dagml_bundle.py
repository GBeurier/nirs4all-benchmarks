"""Adapter B — a dag-ml ``ExecutionBundle`` (+ side files) → ``ArenaRunExport``.

dag-ml is the future engine. Its persisted output is *already* the Arena's shape
(PERSISTENCE_FORMATS.md §6.2): a topology-aware ``graph_fingerprint``,
sample-keyed OOF ``PredictionBlock``s, ``fold_set_fingerprint``, leakage
``unsafe_flags``, and standards-conformant provenance. This adapter maps that JSON
onto the export contract **without importing dag-ml** — it parses the serde JSON
the engine writes (``bundle.rs`` ``ExecutionBundle`` :913) and the data envelope.

Because dag-ml metrics are regression-only and predictions are sample-keyed, the
emitted export uses ``residuals.key = "sample_id"`` — the native, non-degraded path.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from nirs4all_benchmarks.contract import ArenaRunExport
from nirs4all_benchmarks.identity import fingerprint
from nirs4all_benchmarks.identity.hashing import export_hash as compute_export_hash
from nirs4all_benchmarks.scoring import direction_of


def _load_json(value: dict[str, Any] | str | Path) -> dict[str, Any]:
    if isinstance(value, dict):
        return value
    return json.loads(Path(value).read_text(encoding="utf-8"))


def _prediction_residuals(prediction_blocks: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Map dag-ml ``PredictionBlock``s (sample-keyed) → residual rows."""
    rows: list[dict[str, Any]] = []
    for block in prediction_blocks:
        partition = str(block.get("partition", "validation")).lower()
        scope = {"validation": "cv", "test": "test", "final": "refit", "train": "cv"}.get(partition, "cv")
        fold_id = block.get("fold_id")
        sample_ids = block.get("sample_ids") or []
        values = block.get("values") or []
        y_true = block.get("y_true") or block.get("targets") or []
        group_ids = block.get("group_ids") or []
        for i, sid in enumerate(sample_ids):
            yp = values[i] if i < len(values) else None
            if isinstance(yp, list):
                yp = yp[0] if yp else None
            yt = y_true[i] if i < len(y_true) else None
            if isinstance(yt, list):
                yt = yt[0] if yt else None
            residual = (yt - yp) if (yt is not None and yp is not None) else None
            rows.append(
                {
                    "sample_id": str(sid),
                    "group_id": group_ids[i] if i < len(group_ids) else None,
                    "scope": scope,
                    "fold_id": fold_id,
                    "partition": "final" if partition == "final" else partition,
                    "y_true": yt,
                    "y_pred": yp,
                    "residual": residual,
                }
            )
    return rows


def _metric_observations(metric_reports: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Map dag-ml ``RegressionMetricReport``s → score observations."""
    out: list[dict[str, Any]] = []
    for report in metric_reports:
        partition = str(report.get("partition", "validation")).lower()
        level = str(report.get("level", "cv")).lower()
        scope = "test" if partition == "test" else ("fold" if report.get("fold_id") else level)
        scope = scope if scope in {"fold", "cv", "refit", "test", "view"} else "cv"
        metrics = report.get("metrics", {})
        for name, value in metrics.items():
            if not isinstance(value, (int, float)):
                continue
            out.append(
                {
                    "metric_name": str(name).lower(),
                    "metric_value": float(value),
                    "direction": direction_of(str(name)),
                    "scope": scope,
                    "fold_id": report.get("fold_id"),
                    "partition": "final" if partition == "final" else partition,
                    "aggregation_level": "target" if report.get("level") == "macro" else "sample",
                }
            )
    return out


def bundle_to_export(
    bundle: dict[str, Any] | str | Path,
    *,
    graph: dict[str, Any] | str | Path | None = None,
    dataset_envelope: dict[str, Any] | str | Path | None = None,
    dataset_card: dict[str, Any] | None = None,
    metric_reports: list[dict[str, Any]] | None = None,
    prediction_blocks: list[dict[str, Any]] | None = None,
    provenance: dict[str, str] | None = None,
    task_type: str = "regression",
    n_samples: int | None = None,
    n_features: int | None = None,
) -> ArenaRunExport:
    """Build an ``ArenaRunExport`` from a dag-ml bundle and its companion files.

    All companions are optional; missing pieces degrade gracefully (e.g. without a
    graph, identity falls back to the engine ``graph_fingerprint`` recorded as a
    secondary id and a single opaque pipeline node).
    """
    b = _load_json(bundle)
    graph_dict = _load_json(graph) if graph is not None else None
    envelope = _load_json(dataset_envelope) if dataset_envelope is not None else {}

    graph_fingerprint = b.get("graph_fingerprint")
    unsafe_flags = sorted(b.get("unsafe_flags", []) or [])

    # Dataset identity from io's CoordinatorDataPlanEnvelope (three fingerprints).
    dataset_fingerprint = (
        envelope.get("schema_fingerprint")
        or envelope.get("dataset_fingerprint")
        or fingerprint({"dag_ml_bundle": b.get("bundle_id"), "graph": graph_fingerprint})
    )

    pred_blocks = prediction_blocks or b.get("prediction_caches") or []
    residual_rows = _prediction_residuals(pred_blocks)
    observations = _metric_observations(metric_reports or [])

    pipeline_block: dict[str, Any] = {
        "engine_graph_fingerprint": graph_fingerprint,
        "controller_fingerprint": b.get("controller_fingerprint"),
        "human_label": (b.get("metadata") or {}).get("label"),
    }
    if graph_dict:
        pipeline_block["graph"] = graph_dict
        pipeline_block["nodes"] = graph_dict.get("nodes", [])
    else:
        pipeline_block["nodes"] = [
            {"node_id": "g0", "role": "pipeline", "operator": f"dagml:{graph_fingerprint or 'unknown'}"}
        ]

    manifest: dict[str, Any] = {
        "arena_export_schema_version": 1,
        "producer": {"capsule": "dag-ml", "dag_ml_version": str(b.get("schema_version", "1"))},
        "dataset": {
            "dataset_fingerprint": dataset_fingerprint,
            "schema_fingerprint": envelope.get("schema_fingerprint"),
            "relation_fingerprint": envelope.get("relation_fingerprint"),
            "plan_fingerprint": envelope.get("plan_fingerprint"),
            "dataset_card": dataset_card or {"task_type": task_type},
            "visibility": "public",
            "n_samples": n_samples,
            "n_features": n_features,
        },
        "task": {"task_hash": fingerprint({"d": dataset_fingerprint, "t": task_type}), "task_type": task_type},
        "dataset_variant": {
            "dataset_variant_hash": fingerprint({"d": dataset_fingerprint, "v": b.get("selected_variant_id", "all")}),
            "variant_spec": {"size": "all", "selected_variant_id": b.get("selected_variant_id")},
        },
        "pipeline": pipeline_block,
        "split": {"method": "engine", "params": {}},
        "cv": {
            "method": "engine",
            "cv_instance_hash": b.get("campaign_fingerprint"),
            "within_train_only": True,
        },
        "rng": {"root_seed": (b.get("metadata") or {}).get("root_seed"), "derivation": "sha256-hash-chain"},
        "refit": {"strategy": "global_best_params_full_train" if b.get("refit_artifacts") else "none",
                  "selected_variant_id": b.get("selected_variant_id")},
        "execution": {"execution_id": b.get("bundle_id"), "status": "ok"},
        "leakage_attestation": {
            "oof_enforced": True,  # dag-ml enforces OOF by construction (§3.4)
            "group_leakage_checked": True,
            "nested_cv_safe": True,
            "unsafe_flags": unsafe_flags,
        },
        "scores": {"score_version": "1.0", "metric_implementation": "dag-ml/regression", "observations": observations},
        "residuals": {
            "key": "sample_id",
            "pseudonymized": False,
            "publishable": {"y_true": True, "y_pred": True, "residual": True},
            "inline": residual_rows,
        },
    }
    if provenance:
        manifest["provenance"] = provenance
    manifest["arena_export_hash"] = compute_export_hash(manifest)
    return ArenaRunExport.model_validate(manifest)
