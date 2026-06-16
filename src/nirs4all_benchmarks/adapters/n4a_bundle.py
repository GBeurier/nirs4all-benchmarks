"""``.n4a`` recipe extraction — pull the pipeline, drop the weights.

A ``.n4a`` is a ZIP: ``manifest.json`` + ``pipeline.json``/``chain.json`` +
optional ``trace.json`` + ``artifacts/*`` (PERSISTENCE_FORMATS.md §2.3). The Arena
accepts it as a pipeline *upload* but **always strips the weights** (DESIGN.md §2):
only the recipe is read, and its canonical pipeline identity computed.

Per §5.3 the identity is taken from ``pipeline.json`` (a DSL step list) — **never**
``chain.json`` (chain step descriptors + artifact refs, not a DSL).
"""

from __future__ import annotations

import json
import zipfile
from pathlib import Path
from typing import Any

from nirs4all_benchmarks.adapters.nirs4all_workspace import _steps_to_nodes
from nirs4all_benchmarks.identity import compute_pipeline_dag_hash
from nirs4all_benchmarks.identity.pipeline_dag import PipelineDagIdentity


def _read_zip_json(zf: zipfile.ZipFile, name: str) -> dict[str, Any] | list[Any] | None:
    try:
        with zf.open(name) as fh:
            return json.loads(fh.read().decode("utf-8"))
    except KeyError:
        return None


def extract_n4a_recipe(path: str | Path) -> dict[str, Any]:
    """Extract the weights-free recipe from a ``.n4a`` bundle.

    Returns ``{manifest, pipeline, steps, nodes, stripped_artifacts}``. Artifact
    bytes are never read — only their names are listed for the audit trail.
    """
    p = Path(path)
    with zipfile.ZipFile(p) as zf:
        names = zf.namelist()
        manifest = _read_zip_json(zf, "manifest.json") or {}
        pipeline = _read_zip_json(zf, "pipeline.json")
        # Identity must come from pipeline.json, not chain.json (§5.3).
        steps: list[Any] = []
        if isinstance(pipeline, list):
            steps = pipeline
        elif isinstance(pipeline, dict):
            for key in ("pipeline", "steps"):
                if isinstance(pipeline.get(key), list):
                    steps = pipeline[key]
                    break
        if not steps and isinstance(manifest, dict):
            chain = manifest.get("preprocessing_chain")
            if isinstance(chain, list):
                steps = chain
        stripped = [n for n in names if n.startswith("artifacts/")]
    return {
        "manifest": manifest,
        "pipeline": pipeline,
        "steps": steps,
        "nodes": _steps_to_nodes(steps),
        "stripped_artifacts": stripped,
    }


def n4a_pipeline_identity(path: str | Path) -> PipelineDagIdentity:
    """Compute the canonical ``pipeline_dag_hash`` of a ``.n4a`` bundle's recipe."""
    recipe = extract_n4a_recipe(path)
    steps = recipe["steps"]
    if steps:
        return compute_pipeline_dag_hash(steps, steps_for_identity_hash=steps)
    return compute_pipeline_dag_hash({"nodes": recipe["nodes"] or [{"node_id": "g0", "operator": "unknown"}]})
