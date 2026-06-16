"""The unified upload entry point — the run/store/display state machine.

DESIGN.md §6.3 / DATA_MANAGEMENT.md §5 ``UPLOAD``: a user hands the Arena *anything*
that identifies a pipeline and/or a run, and it figures out what to do:

* an ``ArenaRunExport`` manifest (results) → ingest it;
* a dag-ml ``ExecutionBundle`` (results) → adapt + ingest;
* a ``.n4a`` bundle (with **or** without fitted artifacts) → strip weights, register
  the pipeline recipe;
* a raw nirs4all pipeline as a Python list, JSON, or YAML → register the recipe.

For a registered pipeline plus target datasets it runs the **run / store / display**
decision per dataset: if a valid execution already exists for that pipeline×dataset
→ *already run* (display); else → *planned* (a runner fulfils it later and ingests
the result — the Arena itself never runs compute, DESIGN.md non-objectives).

Auto-detection keeps the surface to a single call (``upload``); the typed helpers
(``register_pipeline``) are available too.
"""

from __future__ import annotations

import json
import zipfile
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from nirs4all_benchmarks.adapters.dagml_bundle import bundle_to_export
from nirs4all_benchmarks.adapters.n4a_bundle import extract_n4a_recipe
from nirs4all_benchmarks.contract import ArenaRunExport
from nirs4all_benchmarks.identity import canonical_json, compute_pipeline_dag_hash, fingerprint
from nirs4all_benchmarks.ingestion.ingest import IngestionPolicy, ingest_export
from nirs4all_benchmarks.ingestion.resolve import _extract_pipeline_rows
from nirs4all_benchmarks.store import ArenaStore
from nirs4all_benchmarks.store.arena_store import utc_now


@dataclass
class UploadResult:
    """Outcome of an upload: what was recognized + what happened per dataset."""

    kind: str  # arena_export | dagml_bundle | pipeline | unknown
    status: str  # ingested | registered | rejected
    pipeline_dag_hash: str | None = None
    pipeline_label: str | None = None
    stripped_artifacts: int = 0
    datasets: list[dict[str, Any]] = field(default_factory=list)  # [{dataset, status, n_executions, plan_id}]
    ingestion: dict[str, Any] | None = None
    message: str = ""

    def to_json(self) -> dict[str, Any]:
        return {
            "kind": self.kind, "status": self.status, "pipeline_dag_hash": self.pipeline_dag_hash,
            "pipeline_label": self.pipeline_label, "stripped_artifacts": self.stripped_artifacts,
            "datasets": self.datasets, "ingestion": self.ingestion, "message": self.message,
        }


# ── recipe parsing ──────────────────────────────────────────────────────

def _load_text_recipe(text: str) -> Any:
    text = text.strip()
    try:
        return json.loads(text)
    except json.JSONDecodeError:
        pass
    try:
        import yaml

        return yaml.safe_load(text)
    except Exception as exc:
        raise ValueError(f"could not parse upload as JSON or YAML: {exc}") from exc


def _recipe_to_pipeline_source(recipe: Any) -> tuple[Any, list[Any] | None, str | None, int]:
    """Normalize any recipe into a ``(graph_source, steps, label, n_stripped)`` tuple."""
    # bare step list
    if isinstance(recipe, list):
        return recipe, recipe, None, 0
    if isinstance(recipe, dict):
        for key in ("pipeline", "steps"):
            if isinstance(recipe.get(key), list):
                return recipe[key], recipe[key], recipe.get("name") or recipe.get("label"), 0
        if "nodes" in recipe:
            return recipe, None, recipe.get("name") or recipe.get("label"), 0
    raise ValueError("unrecognized pipeline recipe shape (expected a step list or {pipeline|steps|nodes})")


def _resolve_dataset_fingerprint(store: ArenaStore, token: str) -> str:
    """Map a dataset token (already a fingerprint, a card name, or a dataset_id) → fingerprint."""
    if len(token) == 64 and all(c in "0123456789abcdef" for c in token):
        return token
    row = store.query_one(
        "SELECT dataset_fingerprint FROM dataset_cards WHERE name = ? OR dataset_id = ? LIMIT 1",
        (token, token),
    )
    if row and row["dataset_fingerprint"]:
        return row["dataset_fingerprint"]
    # Deterministic fingerprint for an as-yet-unknown named dataset.
    return fingerprint({"dataset_token": token})


# ── pipeline registration + planning ────────────────────────────────────

def register_pipeline(
    store: ArenaStore,
    recipe: Any,
    *,
    collection_id: str = "uploads",
    target_datasets: list[str] | None = None,
    source: str = "upload",
    human_label: str | None = None,
    stripped_artifacts: int = 0,
) -> UploadResult:
    """Register a pipeline recipe as a dimension and plan/inspect it on target datasets."""
    graph_source, steps, label, _ = _recipe_to_pipeline_source(recipe)
    pid = compute_pipeline_dag_hash(graph_source, steps_for_identity_hash=steps)
    operator_rows, param_value_rows, node_rows, node_param_rows, edge_rows, main_model = _extract_pipeline_rows(pid)
    now = utc_now()
    norm = pid.normalized_graph
    label = human_label or label

    with store.transaction():
        store.ensure_collection(collection_id, kind="user_run_collection")
        store.upsert("pipeline_dags", {
            "pipeline_dag_hash": pid.pipeline_dag_hash,
            "dag_schema_version": norm.get("schema", "arena.graph/1"),
            "graph_json": canonical_json(norm),
            "entry_nodes_json": canonical_json([n["id"] for n in norm["nodes"]
                                                if n["id"] not in {e["dst"] for e in norm["edges"]}]),
            "terminal_nodes_json": canonical_json([n["id"] for n in norm["nodes"]
                                                   if n["id"] not in {e["src"] for e in norm["edges"]}]),
            "n_nodes": len(norm["nodes"]),
            "is_linear": int(pid.is_linear),
            "main_model": main_model,
            "human_label": label,
            "engine_graph_fingerprint": pid.engine_graph_fingerprint,
            "nirs4all_identity_hash": pid.nirs4all_identity_hash,
            "created_at": now,
        })
        for op in operator_rows:
            store.upsert("operator_specs", {**op, "created_at": now})
        for pv in param_value_rows:
            store.upsert("parameter_values", pv)
        for node in node_rows:
            store.upsert("pipeline_nodes", {"pipeline_dag_hash": pid.pipeline_dag_hash, **node})
        for np_row in node_param_rows:
            store.upsert("pipeline_node_params", {"pipeline_dag_hash": pid.pipeline_dag_hash, **np_row})
        for edge in edge_rows:
            store.upsert("pipeline_edges", {"pipeline_dag_hash": pid.pipeline_dag_hash, **edge})

        datasets: list[dict[str, Any]] = []
        for token in target_datasets or []:
            df = _resolve_dataset_fingerprint(store, token)
            existing = store.query_one(
                """SELECT COUNT(*) n FROM run_conditions rc
                   JOIN executions e ON e.run_condition_hash = rc.run_condition_hash
                   WHERE rc.pipeline_dag_hash = ? AND rc.dataset_fingerprint = ? AND e.validity_status = 'valid'""",
                (pid.pipeline_dag_hash, df),
            )
            n_exec = (existing or {}).get("n", 0)
            if n_exec:
                datasets.append({"dataset": df, "token": token, "status": "already_run", "n_executions": n_exec})
            else:
                plan_id = f"plan_{fingerprint({'p': pid.pipeline_dag_hash, 'd': df, 'c': collection_id})[:16]}"
                store.upsert("planned_runs", {
                    "plan_id": plan_id,
                    "pipeline_dag_hash": pid.pipeline_dag_hash,
                    "dataset_fingerprint": df,
                    "task_hash": None,
                    "collection_id": collection_id,
                    "status": "planned",
                    "source": source,
                    "created_at": now,
                })
                datasets.append({"dataset": df, "token": token, "status": "planned",
                                 "n_executions": 0, "plan_id": plan_id})

    return UploadResult(
        kind="pipeline", status="registered", pipeline_dag_hash=pid.pipeline_dag_hash,
        pipeline_label=label, stripped_artifacts=stripped_artifacts, datasets=datasets,
        message=f"registered pipeline {pid.pipeline_dag_hash[:12]}"
                + (f"; stripped {stripped_artifacts} artifact(s)" if stripped_artifacts else ""),
    )


# ── the unified entry point ─────────────────────────────────────────────

def upload(
    store: ArenaStore,
    payload: Any,
    *,
    collection_id: str = "uploads",
    target_datasets: list[str] | None = None,
    as_release: bool = False,
    filename: str | None = None,
) -> UploadResult:
    """Auto-detect ``payload`` and route it through the run/store/display machine.

    ``payload`` may be: an ``ArenaRunExport``/dict manifest, a dag-ml bundle dict, a
    pipeline list/dict, a path to a ``.n4a``/``.json``/``.yaml`` file, or raw JSON/YAML
    text. ``filename`` disambiguates raw text (e.g. ``.n4a`` bytes are not text).
    """
    # A path on disk → a zip is a .n4a; anything else is read as text and sniffed
    # (JSON or YAML), so an extension-less upload of a manifest/recipe still works.
    if isinstance(payload, (str, Path)) and _looks_like_path(payload):
        path = Path(payload)
        if zipfile.is_zipfile(path):
            recipe = extract_n4a_recipe(path)
            return register_pipeline(
                store, recipe["steps"] or {"nodes": recipe["nodes"]},
                collection_id=collection_id, target_datasets=target_datasets,
                source=f".n4a:{path.name}", human_label=path.stem,
                stripped_artifacts=len(recipe["stripped_artifacts"]),
            )
        try:
            text = path.read_text(encoding="utf-8")
        except (UnicodeDecodeError, OSError) as exc:
            raise ValueError(f"unsupported upload file '{path.name}': not a .n4a zip or a text recipe") from exc
        return upload(store, _load_text_recipe(text), collection_id=collection_id,
                      target_datasets=target_datasets, as_release=as_release, filename=path.name)

    # Raw text → parse, then recurse.
    if isinstance(payload, str):
        return upload(store, _load_text_recipe(payload), collection_id=collection_id,
                      target_datasets=target_datasets, as_release=as_release, filename=filename)

    # A results-bearing ArenaRunExport.
    if isinstance(payload, ArenaRunExport) or (isinstance(payload, dict) and "arena_export_schema_version" in payload):
        policy = IngestionPolicy(
            collection_id=collection_id,
            collection_kind="benchmark_release" if as_release else "user_run_collection",
        )
        res = ingest_export(store, payload, policy=policy)
        return UploadResult(
            kind="arena_export",
            status="ingested" if res.ok else "rejected",
            datasets=[],
            ingestion={
                "status": res.status, "validity_status": res.validity_status,
                "run_condition_hash": res.run_condition_hash, "execution_hash": res.execution_hash,
                "issues": res.issues, "clean_report": res.clean_report,
            },
            message=f"ingested ArenaRunExport: {res.status} ({res.validity_status})",
        )

    # A dag-ml ExecutionBundle (results).
    if isinstance(payload, dict) and ("graph_fingerprint" in payload or "bundle_id" in payload):
        export = bundle_to_export(payload)
        return upload(store, export, collection_id=collection_id, target_datasets=target_datasets,
                      as_release=as_release)

    # Otherwise: a pipeline recipe (list or {pipeline|steps|nodes}).
    if isinstance(payload, (list, dict)):
        return register_pipeline(store, payload, collection_id=collection_id,
                                 target_datasets=target_datasets, source="upload")

    raise ValueError(f"unrecognized upload payload of type {type(payload).__name__}")


def _looks_like_path(value: str | Path) -> bool:
    if isinstance(value, Path):
        return value.exists()
    s = value.strip()
    # JSON/YAML recipe text is data, never a path — even if it happens to name a file.
    if not s or s[0] in "{[\"" or "\n" in s or len(s) > 1024:
        return False
    return Path(s).exists()
