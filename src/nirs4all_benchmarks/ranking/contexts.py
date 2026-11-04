"""Structural contexts for matched descriptive component comparisons.

Only a single unbranched chain is qualified. A context removes the selected
factor and preserves the remaining ordered scientific configuration. It is a
cohort identity, never a causal attribution or a numerical quality estimate.
"""

from __future__ import annotations

import json
from collections.abc import Mapping, Sequence
from copy import deepcopy
from typing import Any

from nirs4all_benchmarks.identity import fingerprint
from nirs4all_benchmarks.indexing import classify_role

_ROLES = {
    "model": {"model"},
    "preprocessing_sequence": {"preprocessing", "scaler"},
    "preprocessing_operator": {"preprocessing", "scaler"},
    "feature_selection": {"feature_selection"},
    "augmentation": {"augmentation"},
}
_KNOWN_ROLES = {"input", "model", "preprocessing", "scaler", "feature_selection", "augmentation", "split", "sampler"}
# These fields describe storage/derived identities, not independent scientific
# settings. In particular a Merkle signature would retain the removed factor.
_NODE_DERIVED = {
    "id",
    "node_id",
    "signature",
    "node_signature",
    "operator_spec_hash",
    "params_hash",
    "pipeline_dag_hash",
    "params_json",
    "branch_path_json",
    "model_family",
}
_GRAPH_DERIVED = {"id", "graph_id", "signature", "fingerprint", "graph_fingerprint", "pipeline_dag_hash"}


class _Excluded(ValueError):
    pass


def _decode(value: Any, default: Any) -> Any:
    if value is None:
        return deepcopy(default)
    return json.loads(value) if isinstance(value, str) else deepcopy(value)


def _source(pipe: Mapping[str, Any]) -> dict[str, Any]:
    """Prefer full source metadata over the materialized indexing projection."""
    source = pipe.get("normalized_graph")
    if source is None and pipe.get("graph_json") is not None:
        source = _decode(pipe["graph_json"], {})
    if source is None:
        source = {"schema": "arena.graph/1", "nodes": pipe.get("nodes"), "edges": pipe.get("edges")}
    if not isinstance(source, dict):
        raise _Excluded("invalid_graph_source")
    return deepcopy(source)


def _chain(source: dict[str, Any]) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    raw_nodes, raw_edges = source.get("nodes"), source.get("edges")
    if not isinstance(raw_nodes, list) or not raw_nodes:
        raise _Excluded("missing_graph_nodes")
    if not isinstance(raw_edges, list):
        raise _Excluded("missing_explicit_edges")
    nodes: dict[str, dict[str, Any]] = {}
    for raw in raw_nodes:
        if not isinstance(raw, dict):
            raise _Excluded("invalid_graph_node")
        nid = raw.get("id", raw.get("node_id"))
        if not isinstance(nid, str) or not nid or nid in nodes:
            raise _Excluded("ambiguous_node_identity")
        if "id" in raw and "node_id" in raw and raw["id"] != raw["node_id"]:
            raise _Excluded("ambiguous_node_identity")
        content = {k: deepcopy(v) for k, v in raw.items() if k not in _NODE_DERIVED}
        kind = raw.get("kind", raw.get("role"))
        if "kind" in raw and "role" in raw and raw["kind"] != raw["role"]:
            raise _Excluded("ambiguous_node_role")
        operator = raw.get("operator")
        if operator is not None and not isinstance(operator, str):
            raise _Excluded("invalid_operator_identity")
        if kind is not None and not isinstance(kind, str):
            raise _Excluded("ambiguous_node_role")
        role = classify_role(operator, kind)
        if role == "merge":
            raise _Excluded("unsupported_merge_topology")
        if role not in _KNOWN_ROLES or (kind in _KNOWN_ROLES and role != kind):
            raise _Excluded("ambiguous_node_role")
        if role != "input" and not operator:
            raise _Excluded("missing_operator_identity")
        params = _decode(raw.get("params", raw.get("params_json")), {})
        branch = _decode(raw.get("branch_path", raw.get("branch_path_json")), [])
        if not isinstance(params, dict) or not isinstance(branch, list):
            raise _Excluded("invalid_node_metadata")
        if branch:
            raise _Excluded("unsupported_branch_metadata")
        content.pop("role", None)
        content.update(
            kind=kind,
            operator=operator,
            operator_version=raw.get("operator_version"),
            params=params,
            branch_path=branch,
            fit_scope=raw.get("fit_scope"),
        )
        nodes[nid] = {"content": content, "role": role, "id": nid}
    incoming: dict[str, list[dict[str, Any]]] = {nid: [] for nid in nodes}
    outgoing: dict[str, list[dict[str, Any]]] = {nid: [] for nid in nodes}
    for raw in raw_edges:
        if not isinstance(raw, dict) or raw.get("src") not in nodes or raw.get("dst") not in nodes:
            raise _Excluded("invalid_graph_edge")
        edge = deepcopy(raw)
        edge.setdefault("src_port", "out")
        edge.setdefault("dst_port", "in")
        incoming[edge["dst"]].append(edge)
        outgoing[edge["src"]].append(edge)
    if any(len(v) > 1 for v in [*incoming.values(), *outgoing.values()]):
        raise _Excluded("unsupported_branch_topology")
    starts = [nid for nid, edges in incoming.items() if not edges]
    if len(starts) != 1:
        raise _Excluded("cyclic_or_disconnected_topology")
    ordered, edges, seen = [], [], set()
    nid = starts[0]
    while nid not in seen:
        seen.add(nid)
        ordered.append(nodes[nid])
        if not outgoing[nid]:
            break
        edge = outgoing[nid][0]
        edges.append(edge)
        nid = edge["dst"]
    if len(seen) != len(nodes) or len(edges) != len(nodes) - 1:
        raise _Excluded("cyclic_or_disconnected_topology")
    if sum(node["role"] == "model" for node in ordered) != 1:
        raise _Excluded("ambiguous_model_count")
    return ordered, edges


def _context(
    pipe: Mapping[str, Any],
    dimension: str,
    operator: str | None,
    parameter: str | None,
) -> str:
    source = _source(pipe)
    nodes, edges = _chain(source)
    selected: set[int]
    if dimension == "parameter":
        if not operator or not parameter:
            raise _Excluded("missing_parameter_selector")
        found = [i for i, node in enumerate(nodes) if node["content"]["operator"] == operator]
        if len(found) != 1:
            raise _Excluded("ambiguous_parameter_operator" if found else "missing_parameter_operator")
        node = nodes[found[0]]["content"]
        if parameter not in node["params"]:
            raise _Excluded("missing_target_parameter")
        del node["params"][parameter]
        selected = set()
    elif dimension in _ROLES:
        selected = {i for i, node in enumerate(nodes) if node["role"] in _ROLES[dimension]}
        if dimension == "preprocessing_operator":
            if not operator:
                raise _Excluded("missing_target_operator")
            selected = {i for i in selected if nodes[i]["content"]["operator"] == operator}
            if len(selected) > 1:
                raise _Excluded("ambiguous_target_operator")
        elif dimension == "preprocessing_sequence":
            if selected and max(selected) - min(selected) + 1 != len(selected):
                raise _Excluded("noncontiguous_preprocessing_sequence")
        elif len(selected) > 1:
            raise _Excluded("ambiguous_selected_nodes")
    else:
        raise _Excluded("unsupported_context_dimension")

    # Nonstandard ports or edge metadata touching a removed node cannot be
    # contracted without interpreting its semantics. Keep these cases excluded.
    kept_edges = []
    retained_positions = {old: new for new, old in enumerate(i for i in range(len(nodes)) if i not in selected)}
    for i, edge in enumerate(edges):
        metadata = {k: v for k, v in edge.items() if k not in {"src", "dst"}}
        if i in selected or i + 1 in selected:
            if metadata != {"src_port": "out", "dst_port": "in"}:
                raise _Excluded("unsupported_masked_edge_metadata")
        else:
            if metadata != {"src_port": "out", "dst_port": "in"}:
                kept_edges.append(
                    {"src": retained_positions[i], "dst": retained_positions[i + 1], "metadata": metadata}
                )
    payload = {
        "schema": "n4a.matched-component-context/1",
        "dimension": dimension,
        "operator": operator if dimension in {"parameter", "preprocessing_operator"} else None,
        "parameter": parameter if dimension == "parameter" else None,
        "graph_metadata": {k: v for k, v in source.items() if k not in _GRAPH_DERIVED | {"nodes", "edges"}},
        # Operator/version/parameters define the selected model configuration.
        # Training scope and unrecognized metadata remain matching constraints:
        # changing who was fitted must not masquerade as changing only a model.
        "selected_model_constraints": [
            {
                key: value
                for key, value in nodes[i]["content"].items()
                if key not in {"kind", "operator", "operator_version", "params"}
            }
            for i in sorted(selected)
        ]
        if dimension == "model"
        else [],
        "nodes": [node["content"] for i, node in enumerate(nodes) if i not in selected],
        # Preserve residual edge metadata in chain order. Plain chain links are
        # implicit so factor presence/absence or sequence length do not differ.
        "edge_metadata": kept_edges,
    }
    return fingerprint(payload)


def build_component_contexts(
    pipelines: Sequence[Mapping[str, Any]],
    dimension: str,
    *,
    parameter_operator: str | None = None,
    parameter_name: str | None = None,
) -> dict[str, dict[str, str | None]]:
    """Return a qualified fingerprint or an explicit exclusion per pipeline.

    Each pipeline supplies ``pipeline_dag_hash`` and preferably ``graph_json``
    or ``normalized_graph``. A ``nodes``/``edges`` materialized projection is
    accepted when source metadata is unavailable. IDs and derived Merkle hashes
    are removed; source graph/node metadata, versions, parameters, fit scopes,
    and remaining chain order are preserved. For a masked model, fit scope and
    unknown metadata also remain constraints; only its operator/version/params
    are treated as the selected model configuration. Input objects are never modified.

    ``preprocessing_operator`` requires an exact ``parameter_operator`` selector
    and permits its absence, enabling presence-versus-absence comparisons.
    ``parameter`` requires the selected qualified operator to occur exactly once
    and removes only ``parameter_name`` from its parameter mapping. The other
    dimensions mask selected role nodes, with contiguous preprocessing sequences
    permitted to vary in length. Branched/merged/cyclic/disconnected pipelines,
    multiple models and ambiguous roles receive no fingerprint.
    """
    result = {}
    for pipe in pipelines:
        key = str(pipe["pipeline_dag_hash"])
        try:
            context = _context(pipe, dimension, parameter_operator, parameter_name)
            result[key] = {"fingerprint": context, "excluded_reason": None}
        except _Excluded as exc:
            result[key] = {"fingerprint": None, "excluded_reason": str(exc)}
        except (TypeError, ValueError, KeyError, OverflowError):
            result[key] = {"fingerprint": None, "excluded_reason": "invalid_graph_metadata"}
    return result
