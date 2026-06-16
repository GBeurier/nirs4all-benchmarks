"""Topology-aware pipeline identity (``pipeline_dag_hash``).

PERSISTENCE_FORMATS.md §5.3 sets the goal: one ``pipeline_dag_hash`` that is
(a) topology-aware, (b) dedups "same pipeline, different syntax", and (c) is
identical whether a run came from nirs4all or native dag-ml.

The doc's *target* is to equal dag-ml's ``graph_fingerprint`` by lowering through
``dag-ml-py``. That requires the wheel at ingest **and** exact normalization parity
— a contract to *earn*, not assume. The Arena therefore owns a **self-contained
canonical identity** that needs no producer library:

* **Arena Merkle hash (primary key).** The canonical ``GraphSpec`` is normalized
  (graph id stripped; node ids made positional; merge inputs sorted only for
  order-insensitive reducers) and folded into a Merkle-DAG hash where each node's
  signature depends on its content *and* the multiset/sequence of its inputs'
  signatures. Two graphs that differ only by node naming or by the ordering of an
  order-insensitive merge hash **equal**; a real topology change does not.
* **Engine fingerprint (secondary id).** Any dag-ml ``graph_fingerprint`` carried
  in the export — or computed via ``dag-ml-py`` when installed — is recorded for
  verification/drift, never used as the join key (so dedup stays correct across
  producers that may or may not ship it).
* **nirs4all identity hash (secondary id).** ``get_hash`` (``sha256`` of the
  sorted-JSON linear step list, 16 hex) is recorded as the v0 lineage id.

A linear nirs4all step list is lifted into a minimal linear ``GraphSpec`` and run
through the *same* normalizer, so a pipeline and its DAG-equivalent converge.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from nirs4all_benchmarks.identity.hashing import canonical_json, fingerprint, sha256_hex

# Reducer node kinds whose input ordering is *not* semantically meaningful.
# For these the Merkle signature sorts inputs; for everything else (concat,
# stacking meta-features, plain transforms) input order is preserved.
_ORDER_INSENSITIVE_KINDS = frozenset(
    {
        "mean",
        "average",
        "weighted_mean",
        "weightedmean",
        "vote",
        "voting",
        "bagging",
        "bag",
        "sum",
        "aggregator",
        "ensemble",
    }
)


@dataclass(frozen=True)
class PipelineDagIdentity:
    """The resolved identity of a pipeline, plus recorded secondary ids."""

    pipeline_dag_hash: str
    """Arena canonical, topology-aware key (the join key for dedup)."""

    normalized_graph: dict[str, Any]
    """The id/order-normalized ``GraphSpec`` the Arena stores as source of truth."""

    is_linear: bool = False
    """True when the identity was derived from a linear step list (degraded DAG)."""

    engine_graph_fingerprint: str | None = None
    """dag-ml ``graph_fingerprint`` when supplied/computed — recorded, not keyed on."""

    nirs4all_identity_hash: str | None = None
    """nirs4all ``get_hash`` (16-hex) when a step list was available."""

    node_signatures: dict[str, str] = field(default_factory=dict)
    """``node_id → merkle signature`` — lets the store materialize per-node identity."""


# ---------------------------------------------------------------------------
# nirs4all linear identity (the recorded secondary id / v0 fallback)
# ---------------------------------------------------------------------------

def nirs4all_identity_hash(steps: list[Any]) -> str:
    """Reproduce nirs4all ``get_hash``: ``sha256(sorted-JSON step list)[:16]``.

    Matches ``pipeline_config.py:get_hash`` (``IDENTITY_HASH_LENGTH = 16``). Used
    only as a recorded lineage id / v0 fallback, never as the canonical key.
    """
    serializable = canonical_json(steps)
    return sha256_hex(serializable)[:16]


# ---------------------------------------------------------------------------
# Graph extraction (accept GraphSpec, Arena projection, or linear step list)
# ---------------------------------------------------------------------------

def _coerce_node(raw: Any, index: int) -> dict[str, Any]:
    """Project any node-ish object onto the fields that define its identity."""
    if not isinstance(raw, dict):
        # A bare operator (dotted path or string) — common in nirs4all step lists.
        return {
            "node_id": f"n{index}",
            "kind": "transform",
            "operator": str(raw),
            "operator_version": None,
            "params": {},
            "fit_scope": None,
            "branch_path": [],
        }
    node_id = str(raw.get("node_id") or raw.get("id") or f"n{index}")
    # Infer the stage kind: an explicit kind/role/type wins; otherwise the natural
    # nirs4all DSL shape {"model": ...} means a model node (so a raw uploaded step
    # list hashes IDENTICALLY to the engine/role-tagged graph for the same pipeline).
    explicit_kind = raw.get("kind") or raw.get("role") or raw.get("type")
    kind = str(explicit_kind or ("model" if "model" in raw else "transform"))
    operator = raw.get("operator") or raw.get("class") or raw.get("op") or raw.get("model")
    # An operator may itself be a dict ``{"class": ..., "params": ...}``.
    params = raw.get("params")
    if isinstance(operator, dict):
        params = params if params is not None else operator.get("params")
        operator = operator.get("class") or operator.get("operator") or operator.get("name")
    return {
        "node_id": node_id,
        "kind": kind,
        "operator": str(operator) if operator is not None else None,
        "operator_version": raw.get("operator_version") or raw.get("version"),
        "params": params if isinstance(params, dict) else {},
        "fit_scope": raw.get("fit_scope"),
        "branch_path": raw.get("branch_path") or [],
    }


def _extract_graph(source: Any) -> tuple[list[dict[str, Any]], list[dict[str, Any]], bool]:
    """Return ``(nodes, edges, is_linear)`` from any accepted pipeline shape."""
    # Linear step list → synthesize a linear chain.
    if isinstance(source, list):
        nodes = [_coerce_node(step, i) for i, step in enumerate(source)]
        edges = [
            {"src": nodes[i]["node_id"], "src_port": "out", "dst": nodes[i + 1]["node_id"], "dst_port": "in"}
            for i in range(len(nodes) - 1)
        ]
        return nodes, edges, True

    if not isinstance(source, dict):
        raise TypeError(f"unsupported pipeline source type: {type(source).__name__}")

    # ``{"pipeline": [...]}`` / ``{"steps": [...]}`` wrappers (nirs4all expanded_config).
    for key in ("pipeline", "steps"):
        if key in source and isinstance(source[key], list) and "nodes" not in source:
            return _extract_graph(source[key])

    raw_nodes = source.get("nodes")
    if not isinstance(raw_nodes, list):
        raise TypeError("graph source has no 'nodes' list")
    nodes = [_coerce_node(n, i) for i, n in enumerate(raw_nodes)]

    raw_edges = source.get("edges")
    edges = []
    if isinstance(raw_edges, list):
        for e in raw_edges:
            if not isinstance(e, dict):
                continue
            src = e.get("src") or e.get("from") or e.get("source") or e.get("from_node")
            dst = e.get("dst") or e.get("to") or e.get("target") or e.get("to_node")
            if src is None or dst is None:
                continue
            edges.append(
                {
                    "src": str(src),
                    "src_port": str(e.get("src_port") or e.get("from_port") or "out"),
                    "dst": str(dst),
                    "dst_port": str(e.get("dst_port") or e.get("to_port") or "in"),
                }
            )
        is_linear = False
    else:
        # No edges declared (Arena projection) → assume a linear chain in node order.
        edges = [
            {"src": nodes[i]["node_id"], "src_port": "out", "dst": nodes[i + 1]["node_id"], "dst_port": "in"}
            for i in range(len(nodes) - 1)
        ]
        is_linear = True
    return nodes, edges, is_linear


# ---------------------------------------------------------------------------
# Merkle-DAG canonicalization
# ---------------------------------------------------------------------------

def _topo_order(node_ids: list[str], edges: list[dict[str, Any]]) -> list[str]:
    """Kahn topological sort with deterministic tie-breaking by node id.

    Falls back to declaration order for nodes in a cycle (cycles are invalid DAGs
    but we must not crash on a malformed upload — validation flags them separately).
    """
    incoming: dict[str, int] = dict.fromkeys(node_ids, 0)
    adj: dict[str, list[str]] = {nid: [] for nid in node_ids}
    for e in edges:
        if e["src"] in adj and e["dst"] in incoming:
            adj[e["src"]].append(e["dst"])
            incoming[e["dst"]] += 1
    ready = sorted([nid for nid in node_ids if incoming[nid] == 0])
    order: list[str] = []
    while ready:
        nid = ready.pop(0)
        order.append(nid)
        for nxt in sorted(adj[nid]):
            incoming[nxt] -= 1
            if incoming[nxt] == 0:
                ready.append(nxt)
        ready.sort()
    # Append any remaining (cyclic) nodes deterministically so we always terminate.
    if len(order) < len(node_ids):
        order.extend(sorted(set(node_ids) - set(order)))
    return order


def _node_content(node: dict[str, Any]) -> dict[str, Any]:
    """The intrinsic, position-independent content of a node."""
    return {
        "kind": node["kind"],
        "operator": node["operator"],
        "operator_version": node["operator_version"],
        "params": node["params"],
        "fit_scope": node["fit_scope"],
    }


def _merkle_signatures(nodes: list[dict[str, Any]], edges: list[dict[str, Any]]) -> dict[str, str]:
    """Compute a Merkle signature per node id (content + upstream structure)."""
    by_id = {n["node_id"]: n for n in nodes}
    inbound: dict[str, list[tuple[str, str]]] = {nid: [] for nid in by_id}
    for e in edges:
        if e["dst"] in inbound and e["src"] in by_id:
            inbound[e["dst"]].append((e["dst_port"], e["src"]))
    order = _topo_order(list(by_id), edges)
    sigs: dict[str, str] = {}
    for nid in order:
        node = by_id[nid]
        order_insensitive = node["kind"].lower() in _ORDER_INSENSITIVE_KINDS
        inputs = inbound[nid]
        rendered: list[list[Any]]
        if order_insensitive:
            # Reducer whose input ordering is not meaningful → sort to a multiset.
            rendered = sorted([port, sigs.get(src, "<unresolved>")] for port, src in inputs)
        else:
            # Order-sensitive (transform chain, concat, stacking meta-features): the
            # *position* of each input is part of the identity. Carry an explicit
            # positional index so two inputs on the SAME port no longer collapse when
            # their upstream operators are swapped (e.g. stack(A,B) != stack(B,A)).
            rendered = [[i, port, sigs.get(src, "<unresolved>")] for i, (port, src) in enumerate(inputs)]
        sigs[nid] = fingerprint(
            {
                "content": _node_content(node),
                "inputs": rendered,
                "order_sensitive": not order_insensitive,
            }
        )
    return sigs


def normalize_graph_spec(source: Any) -> tuple[dict[str, Any], dict[str, str], bool]:
    """Normalize any pipeline shape into a canonical ``GraphSpec`` dict.

    Returns ``(normalized_graph, node_signatures, is_linear)``. Node ids are
    rewritten to ``g{topo_index}`` so the output is byte-stable regardless of the
    producer's id naming, and edges are rewritten/sorted accordingly.
    """
    nodes, edges, is_linear = _extract_graph(source)
    sigs = _merkle_signatures(nodes, edges)
    order = _topo_order([n["node_id"] for n in nodes], edges)
    rename = {nid: f"g{i}" for i, nid in enumerate(order)}
    by_id = {n["node_id"]: n for n in nodes}

    norm_nodes = []
    for nid in order:
        node = by_id[nid]
        norm_nodes.append(
            {
                "id": rename[nid],
                "signature": sigs[nid],
                **_node_content(node),
                "branch_path": node["branch_path"],
            }
        )
    norm_edges = sorted(
        (
            {
                "src": rename[e["src"]],
                "src_port": e["src_port"],
                "dst": rename[e["dst"]],
                "dst_port": e["dst_port"],
            }
            for e in edges
            if e["src"] in rename and e["dst"] in rename
        ),
        key=lambda e: (e["dst"], e["dst_port"], e["src"], e["src_port"]),
    )
    normalized = {"schema": "arena.graph/1", "nodes": norm_nodes, "edges": norm_edges}
    # Re-key signatures by the *normalized* node id so downstream tables align.
    norm_sigs = {rename[nid]: sigs[nid] for nid in order}
    return normalized, norm_sigs, is_linear


def compute_pipeline_dag_hash(
    source: Any,
    *,
    engine_graph_fingerprint: str | None = None,
    steps_for_identity_hash: list[Any] | None = None,
) -> PipelineDagIdentity:
    """Resolve the canonical pipeline identity from any accepted pipeline shape.

    Parameters
    ----------
    source:
        A canonical ``GraphSpec`` dict, an Arena ``pipeline.nodes`` projection, a
        ``{"pipeline"|"steps": [...]}`` wrapper, or a bare linear step list.
    engine_graph_fingerprint:
        A dag-ml ``graph_fingerprint`` carried in the export — recorded as a
        secondary id (never the key).
    steps_for_identity_hash:
        The original nirs4all linear step list, if available, to record the
        ``get_hash`` secondary id.
    """
    normalized, sigs, is_linear = normalize_graph_spec(source)
    graph_hash = fingerprint(
        {
            "schema": normalized["schema"],
            "nodes": sorted(n["signature"] for n in normalized["nodes"]),
            "edges": normalized["edges"],
        }
    )
    n4a_hash = None
    if steps_for_identity_hash is not None:
        n4a_hash = nirs4all_identity_hash(steps_for_identity_hash)
    elif is_linear and isinstance(source, list):
        n4a_hash = nirs4all_identity_hash(source)

    return PipelineDagIdentity(
        pipeline_dag_hash=graph_hash,
        normalized_graph=normalized,
        is_linear=is_linear,
        engine_graph_fingerprint=engine_graph_fingerprint,
        nirs4all_identity_hash=n4a_hash,
        node_signatures=sigs,
    )
