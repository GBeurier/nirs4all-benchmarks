"""Store-free ArenaRunExport metadata checks. No scientific verification or execution."""

from __future__ import annotations

from dataclasses import dataclass, field
from graphlib import CycleError, TopologicalSorter
from pathlib import Path
from typing import Any

from pydantic import ValidationError

from nirs4all_benchmarks.contract import ArenaRunExport, validate_manifest
from nirs4all_benchmarks.contract.arena_run_export import COUNT_MAX
from nirs4all_benchmarks.contract.evidence import METADATA_PROFILE, attestation_values, observation_direction
from nirs4all_benchmarks.contract.folds import is_canonical_fold_id, is_pseudo_fold
from nirs4all_benchmarks.contract.strict_json import strict_loads
from nirs4all_benchmarks.identity import canonical_json, fingerprint
from nirs4all_benchmarks.identity.hashing import export_hash, is_hash
from nirs4all_benchmarks.identity.pipeline_dag import _extract_graph
from nirs4all_benchmarks.ingestion.resolve import ResolvedExport, resolve_identities


class MetadataError(ValueError):
    """An input refusal with an input-independent, safe error code."""

    def __init__(self, code: str):
        self.code = code
        super().__init__(code)


def terminal_status(model: ArenaRunExport) -> str:
    """Resolve terminal semantics without interpreting the legacy ``ok`` default."""
    execution = model.execution
    terminal = execution.terminal_status
    if terminal is None:
        return "unreported" if execution.status in {"ok", "unreported"} else execution.status
    expected = {"completed": "ok", "incomplete": "failed", "failed": "failed", "cancelled": "cancelled"}
    if execution.status != expected[terminal]:
        raise MetadataError("status_contradiction")
    return terminal


@dataclass
class MetadataValidation:
    """A metadata verdict, deliberately separate from store validity and science."""

    profile: str = METADATA_PROFILE
    status: str = "rejected"
    scientific_verification: str = "not_performed"
    arena_export_hash: str | None = None
    terminal_status: str = "unreported"
    hash_profile: str | None = None
    fold_coverage: str = "not_reported"
    visibility: str = "unreported"
    visibility_source: str = "unreported"
    reported_attestation_fields: list[str] = field(default_factory=list)
    adopted_unverified: list[str] = field(default_factory=list)
    issues: list[dict[str, str]] = field(default_factory=list)
    model: ArenaRunExport | None = field(default=None, repr=False)
    resolved: ResolvedExport | None = field(default=None, repr=False)
    source_metadata_json: str | None = field(default=None, repr=False)
    source_representation_kind: str | None = field(default=None, repr=False)

    def to_json(self) -> dict[str, Any]:
        return {k: v for k, v in vars(self).items() if k not in {
            "model", "resolved", "source_metadata_json", "source_representation_kind"
        }}


def _check_graph(model: ArenaRunExport) -> None:
    if model.pipeline.graph_ref is not None:
        raise MetadataError("graph_ref_unsupported")
    graph = model.pipeline.graph
    if graph is not None:
        if not isinstance(graph.get("nodes"), list) or not graph["nodes"]:
            raise MetadataError("graph_invalid")
        if any(not isinstance(n, dict) for n in graph["nodes"]):
            raise MetadataError("graph_invalid")
        for node in graph["nodes"]:
            for key in ("node_id", "id"):
                if key in node and (not isinstance(node[key], str) or not node[key]):
                    raise MetadataError("graph_invalid")
            if "params" in node and not isinstance(node["params"], dict):
                raise MetadataError("graph_invalid")
            if "branch_path" in node and not isinstance(node["branch_path"], list):
                raise MetadataError("graph_invalid")
            for key in ("operator_version", "version", "fit_scope", "role", "kind", "type", "source_id", "operator"):
                if node.get(key) is not None and not isinstance(node[key], str):
                    raise MetadataError("graph_invalid")
        if "edges" in graph:
            edges = graph["edges"]
            if not isinstance(edges, list) or any(not isinstance(e, dict) for e in edges):
                raise MetadataError("graph_invalid")
            for edge in edges:
                if any(not isinstance(edge[k], str) or not edge[k] for k in (
                    "src", "from", "source", "from_node", "dst", "to", "target", "to_node"
                ) if k in edge):
                    raise MetadataError("graph_invalid")
                if not any(edge.get(k) is not None for k in ("src", "from", "source", "from_node")) or not any(
                    edge.get(k) is not None for k in ("dst", "to", "target", "to_node")
                ):
                    raise MetadataError("graph_invalid")
    elif model.pipeline.nodes:
        graph = {"nodes": [n.model_dump(exclude_none=True) for n in model.pipeline.nodes]}
    else:
        raise MetadataError("graph_missing")
    nodes, edges, _ = _extract_graph(graph)
    ids = [n["node_id"] for n in nodes]
    if len(ids) != len(set(ids)):
        raise MetadataError("graph_duplicate_node")
    parents: dict[str, set[str]] = {n: set() for n in ids}
    for edge in edges:
        if edge["src"] not in parents or edge["dst"] not in parents:
            raise MetadataError("graph_dangling_edge")
        parents[edge["dst"]].add(edge["src"])
    try:
        tuple(TopologicalSorter(parents).static_order())
    except CycleError as exc:
        raise MetadataError("graph_cycle") from exc


def _verify_claims(model: ArenaRunExport, resolved: ResolvedExport) -> None:
    claims = (
        (model.run_condition_hash, resolved.run_condition_hash),
        (model.pipeline.pipeline_dag_hash, resolved.pipeline_dag_hash),
        (model.scores.score_computation_hash, resolved.score_spec.score_computation_hash),
        (model.split.split_spec_hash, resolved.split_spec_hash),
        (model.cv.cv_spec_hash, resolved.cv_spec_hash),
    )
    if any(claim is not None and claim != computed for claim, computed in claims):
        raise MetadataError("identity_hash_mismatch")


def validate_metadata(export: ArenaRunExport | dict[str, Any] | str | Path) -> MetadataValidation:
    """Check schema, hashes and metadata consistency without opening/writing a store.

    Only strict version 2 normalized manifests define idempotency. Producer identities remain
    unverified; this function never loads datasets, residual refs or fitted models.
    """
    result = MetadataValidation()
    try:
        raw = export.model_dump(mode="python") if isinstance(export, ArenaRunExport) else (
            strict_loads(Path(export).read_text(encoding="utf-8")) if isinstance(export, (str, Path)) else export
        )
        if not isinstance(raw, dict):
            raise MetadataError("manifest_invalid")
        if type(raw.get("arena_export_schema_version")) is not int or raw["arena_export_schema_version"] != 2:
            raise MetadataError("unsupported_export_version")
        # Check BEFORE pydantic's JSON mode can turn a non-finite value into null.
        canonical_json(raw)
        fragments: dict[str, Any] = {}
        if "provenance" in raw:
            fragments["provenance"] = raw["provenance"]
        dataset = raw.get("dataset")
        if isinstance(dataset, dict):
            selected = {key: dataset[key] for key in ("dataset_card", "dataset_spec_ref") if key in dataset}
            if selected:
                fragments["dataset"] = selected
        result.source_metadata_json = canonical_json(fragments)
        result.source_representation_kind = (
            "model_normalized" if isinstance(export, ArenaRunExport) else "submitted_json"
        )
        cv = raw.get("cv")
        repeats = cv.get("n_repeats") if isinstance(cv, dict) else None
        if type(repeats) is int and repeats < 1:
            raise MetadataError("fold_count_contradiction")
        if validate_manifest(raw):
            raise MetadataError("schema_invalid")
        if "sanitized" in raw or "source_export_hash" in raw:
            raise MetadataError("sanitized_manifest_not_ingestible")
        try:
            model = ArenaRunExport.model_validate(raw, strict=True)
        except ValidationError as exc:
            raise MetadataError("schema_invalid") from exc
        if model.residuals.inline and set(model.residuals.publishable) != {"y_true", "y_pred", "residual", "y_proba"}:
            raise MetadataError("residual_publication_policy_unreported")
        if model.dataset.visibility_source == "producer_reported":
            result.visibility = model.dataset.visibility
            result.visibility_source = "producer_reported"
        elif model.dataset.visibility != "public":
            result.visibility = model.dataset.visibility
            result.visibility_source = model.dataset.visibility_source or "unreported"
        try:
            values = attestation_values(model.leakage_attestation)
        except ValueError as exc:
            raise MetadataError("attestation_contradiction") from exc
        result.reported_attestation_fields = [name for name, value in values.items() if value is not None]
        if model.arena_export_schema_version != 2:
            raise MetadataError("unsupported_export_version")
        # Refs are refused even on a dedup hit: no supported byte-binding contract.
        if model.residuals.ref is not None:
            raise MetadataError("residual_ref_unsupported")
        manifest = model.to_manifest()
        if validate_manifest(manifest):
            raise MetadataError("schema_invalid")
        # Rich cards allow extensions, but fields consumed by the existing row
        # builders must have representable shapes. Do not let an allowed extra
        # become an AttributeError or an unsupported SQLite bind during commit.
        card = model.dataset.dataset_card
        axis = card.get("axis") or {}
        if not isinstance(axis, dict) or (
            axis.get("range") is not None and not isinstance(axis["range"], (list, tuple))
        ):
            raise MetadataError("dataset_card_invalid")
        for key in ("dataset_id", "name", "version", "dataset_version", "source_registry", "domain", "modality",
                    "signal_type", "license", "content_hash", "descriptor_hash", "citation_id", "task_type"):
            if (isinstance(card.get(key), (dict, list)) or
                    (type(card.get(key)) is int and not -(2**63) <= card[key] <= COUNT_MAX)):
                raise MetadataError("dataset_card_invalid")
        if (isinstance(axis.get("unit"), (dict, list)) or
                (type(axis.get("unit")) is int and not -(2**63) <= axis["unit"] <= COUNT_MAX)):
            raise MetadataError("dataset_card_invalid")
        n = axis.get("n")
        if n is not None and (type(n) is not int or not 0 <= n <= COUNT_MAX):
            raise MetadataError("dataset_card_invalid")
        for value in [*(axis.get("range") or []), axis.get("resolution")]:
            try:
                if value is not None:
                    float(value)
            except OverflowError as exc:
                raise MetadataError("dataset_card_invalid") from exc
            except (ValueError, TypeError):
                pass
        result.arena_export_hash = export_hash(manifest)
        context: dict[str, Any] = {"schema_version": 1, "arena_export_hash": result.arena_export_hash}
        for block, key in (("pipeline", "human_label"), ("task", "target_unit")):
            submitted = raw.get(block)
            context[block] = {key: submitted[key]} if isinstance(submitted, dict) and key in submitted else {}
        fragments["arena_context"] = context
        result.source_metadata_json = canonical_json(fragments)
        claim = model.arena_export_hash
        if claim is not None and claim != result.arena_export_hash:
            raise MetadataError("export_hash_mismatch")
        result.hash_profile = "v2_normalized"
        result.terminal_status = terminal_status(model)
        _check_graph(model)
        fold_set_fingerprint = model.cv.fold_set_fingerprint
        if fold_set_fingerprint is not None:
            # A declared actual fold set is its own CV instance identity, bound to the relation authority.
            if not is_hash(fold_set_fingerprint):
                raise MetadataError("identity_invalid")
            if model.cv.cv_instance_hash is not None:
                raise MetadataError("cv_identity_contradiction")
            if not is_hash(model.dataset.relation_fingerprint):
                raise MetadataError("fold_set_relation_identity_required")
        resolved = resolve_identities(model)
        _verify_claims(model, resolved)
        contexts = set()
        metric_units: dict[str, set[str | None]] = {}
        for observation in model.scores.observations:
            units = metric_units.setdefault(observation.metric_name.casefold(), set())
            units.add(observation.metric_unit)
            if len(units) > 1:
                raise MetadataError("metric_unit_contradiction")
            if observation.fold_id is not None and not is_canonical_fold_id(observation.fold_id):
                raise MetadataError("noncanonical_fold_id")
            if observation.fold_id is not None and observation.source_fold_id is not None:
                raise MetadataError("score_context_contradiction")
            if observation.fold_id is not None and is_pseudo_fold(observation.fold_id):
                raise MetadataError("score_context_contradiction")
            score_context = (observation.metric_name.casefold(), observation.scope or "unreported",
                   observation.partition or "unreported", observation.aggregation_level or "unreported",
                   observation.fold_id, observation.source_fold_id)
            if score_context in contexts:
                raise MetadataError("duplicate_score_context")
            contexts.add(score_context)
        for field_name in ("dataset_fingerprint", "task_hash", "dataset_variant_hash"):
            value = getattr(getattr(model, {"dataset_fingerprint":"dataset", "task_hash":"task",
                                          "dataset_variant_hash":"dataset_variant"}[field_name]), field_name)
            if not is_hash(value):
                raise MetadataError("identity_invalid")
            basis = getattr(model.identity_basis, field_name)
            if basis.kind == "owner_reported" and not basis.owner:
                raise MetadataError("identity_owner_unreported")
            if basis.kind == "arena_derived" and value != fingerprint(basis.inputs):
                raise MetadataError("identity_hash_mismatch")
            if basis.kind == "arena_derived" and not str(basis.inputs.get("namespace", "")).startswith("arena."):
                raise MetadataError("identity_namespace_unreported")
        metric_folds: dict[tuple[str, str, str | None, str | None], set[str]] = {}
        for observation in model.scores.observations:
            try:
                observation_direction(observation)
            except ValueError as exc:
                raise MetadataError("metric_direction_mismatch") from exc
            if (observation.scope in {"fold", "cv"} and observation.fold_id is not None
                    and not is_pseudo_fold(observation.fold_id) and observation.partition in {"train", "validation"}):
                ids = metric_folds.setdefault((observation.metric_name, observation.partition,
                                               observation.aggregation_level, observation.source_fold_id), set())
                ids.add(observation.fold_id)
        # Each reported metric/partition fold series must independently cover CV.
        # Refit/test rows and reserved aggregate ids never complete missing folds.
        fold_ids = set().union(*metric_folds.values()) if metric_folds else set()
        residual_keys = set()
        for row in model.residuals.inline or []:
            if not isinstance(row.get("sample_id"), str) or not row["sample_id"]:
                raise MetadataError("residual_sample_id_missing")
            if row.get("fold_id") is not None and not isinstance(row["fold_id"], str):
                raise MetadataError("residual_context_invalid")
            if row.get("fold_id") is not None and not is_canonical_fold_id(row["fold_id"]):
                raise MetadataError("noncanonical_fold_id")
            residual_key = (row["sample_id"], row.get("scope"), row.get("fold_id"), row.get("partition"))
            if residual_key in residual_keys:
                raise MetadataError("duplicate_residual_context")
            residual_keys.add(residual_key)
            for key in ("group_id", "origin_sample_id"):
                if row.get(key) is not None and not isinstance(row[key], str):
                    raise MetadataError("residual_context_invalid")
            for key in ("y_true", "y_pred", "residual", "weight"):
                value = row.get(key)
                if value is not None and (isinstance(value, bool) or not isinstance(value, (int, float))):
                    raise MetadataError("residual_value_invalid")
            proba = row.get("y_proba")
            if proba is not None and (
                not isinstance(proba, (list, tuple)) or any(
                    isinstance(value, bool) or not isinstance(value, (int, float)) for value in proba
                )
            ):
                raise MetadataError("residual_value_invalid")
            if row.get("scope") not in {"cv", "fold", "refit", "test", "view", "unreported"} or row.get(
                "partition"
            ) not in {"train", "validation", "test", "final", "unreported"}:
                raise MetadataError("residual_context_invalid")
            if (row.get("fold_id") is not None and not is_pseudo_fold(row["fold_id"])
                    and row.get("scope") in {"cv", "fold"}
                    and row.get("partition") in {"train", "validation"}):
                fold_ids.add(str(row["fold_id"]))
        cv_expected = None
        if ((model.cv.n_folds is not None and model.cv.n_folds < 1)
                or (model.cv.n_repeats is not None and model.cv.n_repeats < 1)):
            raise MetadataError("fold_count_contradiction")
        if model.cv.n_folds is not None:
            cv_expected = model.cv.n_folds * (model.cv.n_repeats or 1)
        expected = model.scores.expected_fold_count
        if expected is not None and cv_expected is not None and expected != cv_expected:
            raise MetadataError("fold_expectation_contradiction")
        if expected is None:
            expected = cv_expected
        completed = model.scores.completed_fold_count
        if expected is not None and len(fold_ids) > expected:
            raise MetadataError("fold_count_contradiction")
        if fold_ids:
            result.fold_coverage = "reported"
            if completed is not None and completed != len(fold_ids):
                raise MetadataError("fold_count_contradiction")
        if result.terminal_status == "completed" and expected is not None:
            if any(len(ids) < expected for ids in metric_folds.values()):
                raise MetadataError("incomplete_metric_fold_coverage")
            if (fold_ids and len(fold_ids) < expected) or (completed is not None and completed < expected):
                raise MetadataError("incomplete_fold_coverage")
        if expected is not None and completed is not None and completed > expected:
            raise MetadataError("fold_count_contradiction")
        for observation in model.scores.observations:
            if model.producer.capsule == "nirs4all-workspace" and (
                (observation.scope in {"refit", "view"} and observation.source_fold_id is None)
                or (observation.scope in {"cv", "test"} and observation.fold_id is None)
            ):
                raise MetadataError("workspace_score_context_unreported")
            if observation.fold_id is not None and is_pseudo_fold(observation.fold_id):
                raise MetadataError("score_context_unsupported")
            label = observation.source_fold_id
            if label is not None and is_pseudo_fold(label):
                key = label.strip()
                scope = "refit" if key in {"final", "final_agg"} else "view"
                level = "group" if key.endswith("_agg") else "sample"
                if (observation.scope != scope or observation.aggregation_level != level
                        or observation.fold_id is not None):
                    raise MetadataError("score_context_contradiction")
        if (model.dataset.n_samples is not None and model.cv.n_folds is not None
                and model.cv.n_folds > model.dataset.n_samples):
            raise MetadataError("cv_inconsistent")
        adopted = {
            "dataset": ("dataset_fingerprint", "schema_fingerprint", "relation_fingerprint", "plan_fingerprint"),
            "task": ("task_hash", "target_hash"),
            "dataset_variant": ("dataset_variant_hash", "sample_manifest_hash"),
            "pipeline": ("engine_graph_fingerprint", "controller_fingerprint", "nirs4all_identity_hash"),
            "cv": ("cv_instance_hash", "fold_set_fingerprint"), "split": ("split_instance_hash",),
            # These remain engine-scoped for v1 compatibility; no Arena verification claim.
            "rng": ("rng_context_hash",), "refit": ("refit_strategy_hash",),
        }
        result.adopted_unverified = [
            f"{block}.{name}" for block, names in adopted.items() for name in names
            if getattr(getattr(model, block), name) is not None
        ]
        result.model, result.resolved = model, resolved
        result.status = "metadata_valid"
    except MetadataError as exc:
        result.issues = [{"level": "error", "code": exc.code, "message": "Arena metadata rejected: " + exc.code}]
    except (ValueError, TypeError, KeyError, AttributeError, OSError, OverflowError, RecursionError):
        # Never return pydantic/jsonschema errors containing the submitted value.
        result.issues = [{"level": "error", "code": "manifest_invalid", "message": "Arena metadata is malformed"}]
    return result
