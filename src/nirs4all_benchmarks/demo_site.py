"""Explicit historical demonstration, separate from qualified result publication.

Only the exact frozen reference archive is admitted. Producer-public records are
displayed as unqualified historical observations, never as valid executions.
"""

from __future__ import annotations

import hashlib
import json
import shutil
from pathlib import Path
from typing import Any

from nirs4all_benchmarks import __version__
from nirs4all_benchmarks.contract.strict_json import strict_loads
from nirs4all_benchmarks.frozen_history import (
    SOURCE_ARCHIVE_SHA256,
    SOURCE_MANIFEST_SHA256,
    _convert_record,
    verified_source,
)
from nirs4all_benchmarks.identity import fingerprint
from nirs4all_benchmarks.indexing import build_run_facets
from nirs4all_benchmarks.ingestion.resolve import resolve_identities
from nirs4all_benchmarks.site_build import _COPY, _WEB_DIR, _columnar, _write_seo

PROFILE = "arena.frozen-history-public-demo/1"


def build_demo_site(
    source: str | Path, out_dir: str | Path, *, domain: str | None = "benchmarks.nirs4all.org",
) -> dict[str, Any]:
    """Render only producer-public records from the hash-pinned historical archive.

    Display dataset keys distinguish source, task and variant. Original identity
    fields remain available beside those display keys and in each run document.
    No store, current project data, fitted artifacts or new execution is read.
    """
    out = Path(out_dir)
    if out.is_symlink() or (out.exists() and (not out.is_dir() or any(out.iterdir()))):
        raise ValueError("demo output must be an empty directory")
    metadata, lines = verified_source(source)
    selected = [(ordinal, line) for ordinal, line in enumerate(lines)
                if strict_loads(line)["dataset"]["visibility"] == "public"]
    if len(selected) != 4998:
        raise ValueError("frozen_history_public_count_mismatch")

    metrics: list[dict[str, Any]] = []
    facets: dict[str, list[dict[str, Any]]] = {}
    datasets: dict[str, dict[str, Any]] = {}
    pipelines: dict[str, dict[str, Any]] = {}
    nodes: dict[str, list[dict[str, Any]]] = {}
    operators: dict[str, dict[str, Any]] = {}
    parameters: dict[str, set[str]] = {}
    documents: dict[str, dict[str, Any]] = {}
    condition_contexts: dict[str, str] = {}
    dataset_conditions: dict[str, set[str]] = {}
    pipeline_conditions: dict[str, set[str]] = {}
    operator_pipelines: dict[str, set[str]] = {}

    for ordinal, line in selected:
        model = _convert_record(line, ordinal)
        resolved = resolve_identities(model)
        eh, rc, ph = resolved.execution_hash, resolved.run_condition_hash, resolved.pipeline_dag_hash
        card = model.dataset.dataset_card
        view_id = fingerprint({"profile": PROFILE, "dataset_fingerprint": resolved.dataset_fingerprint,
                               "task_hash": resolved.task_hash, "dataset_variant_hash": resolved.dataset_variant_hash})
        if condition_contexts.setdefault(rc, view_id) != view_id:
            raise ValueError("frozen_history_run_condition_context_conflict")
        axis = card.get("axis") or {}
        axis_range = axis.get("range") or [None, None]
        source_id = model.dataset_variant.variant_spec.get("source_id") or card.get("source_id")
        datasets.setdefault(view_id, {
            "dataset_fingerprint": view_id, "source_dataset_fingerprint": resolved.dataset_fingerprint,
            "task_hash": resolved.task_hash, "dataset_variant_hash": resolved.dataset_variant_hash,
            "source_id": source_id, "target_name": model.task.target_name,
            "name": f"{card.get('name') or card.get('dataset_id')} · {source_id} · {model.task.target_name}",
            "domain": card.get("domain"), "modality": card.get("modality"),
            "signal_type": card.get("signal_type"), "task_type": model.task.task_type,
            "n_samples": model.dataset.n_samples, "n_features": model.dataset.n_features,
            "axis_unit": axis.get("unit"), "axis_min": axis_range[0], "axis_max": axis_range[1],
            "privacy_level": "producer-reported public", "scientific_validity": "unreported",
        })
        dataset_conditions.setdefault(view_id, set()).add(rc)
        pipeline_conditions.setdefault(ph, set()).add(rc)
        pipelines.setdefault(ph, {
            "pipeline_dag_hash": ph, "human_label": model.pipeline.human_label,
            "main_model": resolved.main_model, "n_nodes": len(resolved.node_rows),
            "is_linear": resolved.pipeline_id.is_linear,
            "nirs4all_identity_hash": model.pipeline.nirs4all_identity_hash,
            "engine_graph_fingerprint": model.pipeline.engine_graph_fingerprint,
        })
        nodes.setdefault(ph, [{**node, "pipeline_dag_hash": ph} for node in resolved.node_rows])
        for operator in resolved.operator_rows:
            key = operator["operator_spec_hash"]
            operators.setdefault(key, dict(operator))
            operator_pipelines.setdefault(key, set()).add(ph)
        for parameter in resolved.param_value_rows:
            if parameter["is_numeric"]:
                parameters.setdefault(parameter["name"], set()).add(parameter["param_value_hash"])
        if rc not in facets:
            facets[rc] = build_run_facets(resolved, model)
            for facet in facets[rc]:
                if facet["facet_key"] == "dataset":
                    facet["facet_value"] = view_id
        scores = [observation.model_dump(mode="json") for observation in model.scores.observations]
        for score in scores:
            metrics.append({
                "execution_hash": eh, "run_condition_hash": rc,
                "execution_validity": "unreported", "execution_status": "unreported",
                "producer_capsule": model.producer.capsule, "time_ms": None,
                "pipeline_dag_hash": ph, "pipeline_label": model.pipeline.human_label,
                "dataset_fingerprint": view_id, "source_dataset_fingerprint": resolved.dataset_fingerprint,
                "task_hash": resolved.task_hash, "dataset_variant_hash": resolved.dataset_variant_hash,
                "main_model": resolved.main_model, "score_scope": score["scope"],
                "score_validity": "historical_reported", "metric_name": score["metric_name"],
                "metric_value": score["metric_value"], "partition": score["partition"],
            })
        documents[eh] = {"detail": {
            "execution": {"execution_hash": eh, "run_condition_hash": rc, "status": "unreported",
                          "validity_status": "unreported", "producer_capsule": model.producer.capsule,
                          "oof_enforced": None, "group_leakage_checked": None, "nested_cv_safe": None},
            "run_condition": {"run_condition_hash": rc, "dataset_fingerprint": resolved.dataset_fingerprint,
                              "task_hash": resolved.task_hash, "dataset_variant_hash": resolved.dataset_variant_hash},
            "pipeline": pipelines[ph], "nodes": resolved.node_rows, "edges": resolved.edge_rows,
            "node_params": resolved.node_param_rows, "scores": scores, "dataset": datasets[view_id],
            "cv": {**model.cv.model_dump(mode="json"), "cv_instance_hash": resolved.cv_instance_hash},
            "rng": model.rng.model_dump(mode="json"), "refit": model.refit.model_dump(mode="json"),
            "metadata": {"profile": PROFILE, "source_archive_sha256": SOURCE_ARCHIVE_SHA256,
                         "source_record_sha256": hashlib.sha256(line).hexdigest(), "source_ordinal": ordinal,
                         "scientific_validity": "unreported", "visibility": "producer-reported public"},
        }, "residuals": []}

    for key, dataset in datasets.items():
        dataset["n_run_conditions"] = len(dataset_conditions[key])
    for key, pipeline in pipelines.items():
        pipeline["n_run_conditions"] = len(pipeline_conditions[key])
    for key, operator in operators.items():
        operator["n_pipelines"] = len(operator_pipelines[key])
    strings: dict[str, int] = {}
    tables = {"metrics": _columnar(metrics, strings), "pipelines": _columnar(list(pipelines.values()), strings),
              "facets": _columnar([row for group in facets.values() for row in group], strings),
              "pipeline_nodes": _columnar([row for group in nodes.values() for row in group], strings)}
    overview = {
        "datasets": len(datasets), "dataset_cards": len(datasets),
        "tasks": len({row["task_hash"] for row in metrics}), "pipelines": len(pipelines),
        "operators": len(operators), "parameters": len(parameters), "run_conditions": len(facets),
        "executions": len(documents), "valid_executions": 0, "qualified_executions": 0,
        "historical_executions": len(documents), "unreported_executions": len(documents),
        "quarantined_executions": 0, "include_unreported": True, "score_sets": len(documents),
        "metric_observations": len(metrics), "total_metric_observations": len(metrics),
        "collections": 1, "residual_sets": 0, "metrics": sorted({row["metric_name"] for row in metrics}),
        "score_scopes": sorted({row["score_scope"] for row in metrics}), "schema_version": None,
    }
    bundle = {
        "meta": {"version": __version__, "static": True, "profile": PROFILE,
                 "source_archive_sha256": SOURCE_ARCHIVE_SHA256,
                 "source_manifest_sha256": SOURCE_MANIFEST_SHA256, "scientific_validity": "unreported"},
        "overview": overview, "datasets": list(datasets.values()), "operators": list(operators.values()),
        "parameters": [{"name": name, "n_values": len(values), "numeric": 1}
                       for name, values in parameters.items()],
        "collections": [{"collection_id": metadata["collection_id"], "name": "Frozen historical demonstration"}],
        "planned": [], "strings": list(strings), "tables": tables,
    }
    out.mkdir(parents=True, exist_ok=True)
    for name in _COPY:
        if name == "generated":
            continue
        src, dst = _WEB_DIR / name, out / name
        if src.is_dir():
            shutil.copytree(src, dst)
        elif src.is_file():
            shutil.copy2(src, dst)
    (out / "config.js").write_text(
        "window.ARENA_STATIC = true;\nwindow.ARENA_HISTORICAL_DEMO = true;\n", encoding="utf-8",
    )
    runs = out / "data" / "runs"
    runs.mkdir(parents=True)
    (out / "data" / "bundle.json").write_text(json.dumps(bundle, separators=(",", ":")), encoding="utf-8")
    for eh, document in documents.items():
        (runs / f"{eh}.json").write_text(json.dumps(document, separators=(",", ":")), encoding="utf-8")
    if domain:
        (out / "CNAME").write_text(domain + "\n", encoding="utf-8")
        _write_seo(out, domain)
    (out / ".nojekyll").write_text("", encoding="utf-8")
    return {"out": str(out), "profile": PROFILE, **overview}
