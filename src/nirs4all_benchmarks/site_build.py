"""Static-site builder — snapshot a store into JSON the SPA can query client-side.

GitHub Pages is static-only, but the Arena SPA is API-driven. ``build_site`` exports
the store as a compact JSON bundle (the v_run_metrics backbone + facets + pipelines
+ residuals) and copies the no-build SPA next to it; a small client-side engine
(``web/lib/static-engine.js``) answers every query from the bundle, so the whole
dataviz runs on Pages with **no backend**.

Layout produced::

    out/
      index.html  app.js  styles.css  lib/  views/  vendor/  brand/
      config.js                 # window.ARENA_STATIC = true
      CNAME                     # the custom domain
      robots.txt / sitemap.xml  # crawl discovery for the public domain
      data/
        bundle.json             # aggregation backbone (loaded on boot; columnar, see _columnar)
        runs/<execution_hash>.json   # per-run detail + residuals (loaded on demand)
"""

from __future__ import annotations

import json
import shutil
from pathlib import Path
from typing import Any

from nirs4all_benchmarks import __version__
from nirs4all_benchmarks.ranking import RankingRequest, RelativeGainRequest
from nirs4all_benchmarks.ranking.component_analysis import ComponentAnalysisRequest
from nirs4all_benchmarks.scoring import METRIC_DIRECTION
from nirs4all_benchmarks.store import ArenaStore, Queries

_WEB_DIR = Path(__file__).resolve().parent / "web"
# Files copied verbatim into the static site (everything the SPA needs at runtime).
_COPY = ["index.html", "app.js", "styles.css", "lib", "views", "vendor", "brand", "generated"]


def build_site(
    store_root: str | Path, out_dir: str | Path, *, domain: str | None = "benchmarks.nirs4all.org",
    ranking_requests: list[RankingRequest | dict[str, Any]] | None = None,
    relative_gain_requests: list[RelativeGainRequest | dict[str, Any]] | None = None,
    component_analysis_requests: list[ComponentAnalysisRequest | dict[str, Any]] | None = None,
    unavailable_results: bool = False,
) -> dict[str, Any]:
    """Build an explicitly data-free status page; result publication remains closed.

    Args:
        store_root: Store location for result builds; unused by the status page.
        out_dir: Absent or empty output directory.
        domain: Optional custom domain written to CNAME.
        ranking_requests: Explicit ranking snapshots; forbidden for the status page.
        relative_gain_requests: Explicit gain snapshots; forbidden for the status page.
        component_analysis_requests: Explicit component snapshots; forbidden for the status page.
        unavailable_results: Publish only the static unavailable-results page.

    Returns:
        Summary of the generated page and its zero published scientific records.

    Raises:
        ValueError: Result publication is closed, requests are supplied for the status
            page, or output could retain stale content.
    """
    if unavailable_results:
        if any(requests is not None for requests in (
            ranking_requests, relative_gain_requests, component_analysis_requests,
        )):
            raise ValueError("unavailable_results_forbids_analysis_requests")
        out = Path(out_dir)
        if out.is_symlink() or (out.exists() and (not out.is_dir() or any(out.iterdir()))):
            raise ValueError("site output must be an empty directory; stale files cannot be published")
        out.mkdir(parents=True, exist_ok=True)
        # Deliberate file allowlist: no SPA, store, history, data or generated runtime.
        for source, target in (
            ("unavailable.html", "index.html"), ("styles.css", "styles.css"), ("brand/icon.svg", "brand/icon.svg"),
        ):
            destination = out / target
            destination.parent.mkdir(parents=True, exist_ok=True)
            shutil.copyfile(_WEB_DIR / source, destination)
        if domain:
            (out / "CNAME").write_text(domain + "\n", encoding="utf-8")
        (out / ".nojekyll").write_text("", encoding="utf-8")
        return {"out": str(out), "state": "unavailable", "reason": "no_qualified_public_snapshot",
                "executions": 0, "pipelines": 0, "datasets": 0, "metric_rows": 0, "facet_rows": 0,
                "domain": domain}
    raise ValueError("publication_gate_closed")
    if not (Path(store_root) / "arena.sqlite").is_file():
        raise ValueError("site build requires an existing store with rankable completed runs")
    if len(ranking_requests or []) + len(relative_gain_requests or []) + len(component_analysis_requests or []) > 64:
        raise ValueError("At most 64 explicit analysis snapshots may be requested per build")
    ranking_configs = [r if isinstance(r, RankingRequest) else RankingRequest(**r) for r in ranking_requests or []]
    gain_configs = [r if isinstance(r, RelativeGainRequest) else RelativeGainRequest(**r)
                    for r in relative_gain_requests or []]
    analysis_configs = [r if isinstance(r, ComponentAnalysisRequest) else ComponentAnalysisRequest(**r)
                        for r in component_analysis_requests or []]
    if ranking_configs or gain_configs or analysis_configs:
        # Ranking and analysis artifacts read the whole store, including private executions;
        # the public bundle is projected from public receipts only. Precomputed snapshots
        # need an owner-defined public projection before they can be embedded.
        raise ValueError("ranking_snapshots_require_public_projection")
    out = Path(out_dir)
    if out.is_symlink() or (out.exists() and any(out.iterdir())):
        raise ValueError("site output must be an empty directory; stale files cannot be published")
    with ArenaStore(store_root) as store:
        # One read snapshot governs every table and run file. A second open or
        # query after copying could include a run not covered by the first gate.
        store.conn.execute("BEGIN")
        try:
            q = Queries(store)
            bundle = _build_bundle(store, q)
            execs = _execution_hashes(bundle)
            out.mkdir(parents=True, exist_ok=True)
            for name in _COPY:
                src, dst = _WEB_DIR / name, out / name
                if not src.exists():
                    continue
                if src.is_dir():
                    shutil.copytree(src, dst)
                else:
                    shutil.copy2(src, dst)
            (out / "config.js").write_text("window.ARENA_STATIC = true;\n", encoding="utf-8")
            if domain:
                (out / "CNAME").write_text(domain + "\n", encoding="utf-8")
                _write_seo(out, domain)
            (out / ".nojekyll").write_text("", encoding="utf-8")
            data_dir = out / "data"
            runs_dir = data_dir / "runs"
            runs_dir.mkdir(parents=True)
            (data_dir / "bundle.json").write_text(json.dumps(bundle, separators=(",", ":")), encoding="utf-8")
            for eh in execs:
                document = _public_run_document(store, q, eh, bundle)
                (runs_dir / f"{eh}.json").write_text(json.dumps(document, separators=(",", ":")), encoding="utf-8")
        finally:
            store.conn.rollback()

    return {"out": str(out), "executions": len(execs), "pipelines": bundle["overview"]["pipelines"],
            "datasets": bundle["overview"]["datasets"], "metric_rows": bundle["tables"]["metrics"]["n"],
            "facet_rows": bundle["tables"]["facets"]["n"], "domain": domain}


def _write_seo(out: Path, domain: str) -> None:
    base = f"https://{domain.strip('/')}/"
    (out / "robots.txt").write_text(
        f"User-agent: *\nAllow: /\n\nSitemap: {base}sitemap.xml\n",
        encoding="utf-8",
    )
    (out / "sitemap.xml").write_text(
        '<?xml version="1.0" encoding="UTF-8"?>\n'
        '<urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">\n'
        "  <url>\n"
        f"    <loc>{base}</loc>\n"
        "    <lastmod>2026-07-01</lastmod>\n"
        "    <changefreq>weekly</changefreq>\n"
        "    <priority>0.7</priority>\n"
        "  </url>\n"
        "</urlset>\n",
        encoding="utf-8",
    )


def _columnar(rows: list[dict[str, Any]], strings: dict[str, int]) -> dict[str, Any]:
    """Encode rows column by column; string columns hold indices into the shared string table.

    The large tables repeat the same 64-character hashes thousands of times; storing each
    string once keeps the bundle small enough to download and parse quickly.
    """
    columns: dict[str, Any] = {}
    for name in rows[0] if rows else []:
        values = [r[name] for r in rows]
        if all(v is None or isinstance(v, str) for v in values):
            columns[name] = {"s": [None if v is None else strings.setdefault(v, len(strings)) for v in values]}
        else:
            columns[name] = {"v": values}
    return {"n": len(rows), "columns": columns}


def _execution_hashes(bundle: dict[str, Any]) -> list[str]:
    values = bundle["tables"]["metrics"]["columns"]["execution_hash"]["s"]
    return sorted({bundle["strings"][index] for index in values})


def _publication_evidence(store: ArenaStore, q: Queries) -> dict[str, dict[str, Any]]:
    from nirs4all_benchmarks.contract.evidence import METADATA_PROFILE

    public = {}
    for row in store.query("""SELECT DISTINCT e.execution_hash, e.arena_export_hash, ib.clean_report_json
            FROM executions e JOIN ingestion_batches ib ON ib.ingestion_batch_id = e.ingestion_batch_id
            JOIN run_conditions rc ON rc.run_condition_hash = e.run_condition_hash
            JOIN dataset_fingerprints df ON df.dataset_fingerprint = rc.dataset_fingerprint
            JOIN v_ranked_metrics mo ON mo.execution_hash = e.execution_hash
            WHERE df.privacy_level = 'public'"""):
        try:
            metadata = json.loads(row["clean_report_json"])["metadata_validation"]
            if (metadata.get("profile") != METADATA_PROFILE or metadata.get("visibility") != "public"
                    or metadata.get("visibility_source") != "producer_reported"):
                continue
            receipt = q._evidence(row)
            evidence = receipt.get("metadata_validation", {})
            if (evidence.get("profile") == METADATA_PROFILE and evidence.get("visibility") == "public"
                    and evidence.get("visibility_source") == "producer_reported"):
                public[row["execution_hash"]] = receipt
        except (KeyError, TypeError, ValueError):
            continue
    return public


def _build_bundle(store: ArenaStore, q: Queries) -> dict[str, Any]:
    public = _publication_evidence(store, q)
    units: dict[str, set[str | None]] = {}
    for receipt in public.values():
        for observation in receipt.get("scores", {}).get("observations", []):
            units.setdefault(observation["metric_name"].casefold(), set()).add(observation.get("metric_unit"))
    if any(len(values) > 1 for values in units.values()):
        raise ValueError("static_metric_unit_context_unsupported")
    rows = [row for row in store.query("SELECT * FROM v_ranked_metrics")
            if row["execution_hash"] in public and row["direction"] is not None]
    # The unchanged static consumer has no required refit partition selection.
    # Refuse incompatible public packs instead of silently dropping or mixing facts.
    if any(row["score_scope"] in {"refit", "view"} or row["source_fold_id"] is not None
           or row["aggregation_level"] != "sample" or row["partition"] != (
        "test" if row["score_scope"] == "test" else "validation"
    ) for row in rows):
        raise ValueError("static_score_partition_selection_unsupported")
    if any(row["metric_name"] not in METRIC_DIRECTION for row in rows):
        raise ValueError("static publication requires registered metric directions; custom metrics are Python-only")
    if not rows:
        raise ValueError("site build refused: zero rankable explicitly public completed runs")
    executions = {r["execution_hash"] for r in rows}
    conditions = {r["run_condition_hash"] for r in rows}
    pdhs = {r["pipeline_dag_hash"] for r in rows}
    dfs = {r["dataset_fingerprint"] for r in rows}
    cids = {r["collection_id"] for r in rows}
    receipts_by_dataset = {r["dataset_fingerprint"]: public[r["execution_hash"]] for r in rows}
    receipts_by_pipeline = {r["pipeline_dag_hash"]: public[r["execution_hash"]] for r in rows}
    datasets = []
    for row in q.datasets():
        if row["dataset_fingerprint"] not in dfs:
            continue
        source = receipts_by_dataset[row["dataset_fingerprint"]]["dataset"]
        row["task_type"] = receipts_by_dataset[row["dataset_fingerprint"]]["task"].get("task_type")
        card = source.get("dataset_card", {})
        for key in ("name", "domain", "modality", "signal_type", "axis_unit", "axis_min", "axis_max"):
            row[key] = card.get(key)
        row["n_samples"], row["n_features"] = source.get("n_samples"), source.get("n_features")
        row["n_run_conditions"] = len({r["run_condition_hash"] for r in rows
                                       if r["dataset_fingerprint"] == row["dataset_fingerprint"]})
        datasets.append(row)
    pipelines = [row for row in q.pipelines() if row["pipeline_dag_hash"] in pdhs]
    for row in pipelines:
        source = receipts_by_pipeline[row["pipeline_dag_hash"]]["pipeline"]
        for key in ("human_label", "nirs4all_identity_hash", "engine_graph_fingerprint"):
            row[key] = source.get(key)
        row["n_run_conditions"] = len({r["run_condition_hash"] for r in rows
                                       if r["pipeline_dag_hash"] == row["pipeline_dag_hash"]})
    labels = {row["pipeline_dag_hash"]: row["human_label"] for row in pipelines}
    for row in rows:
        row["pipeline_label"] = labels[row["pipeline_dag_hash"]]
    nodes = [r for r in store.query("SELECT pipeline_dag_hash, role, operator, operator_spec_hash FROM pipeline_nodes")
             if r["pipeline_dag_hash"] in pdhs]
    operators = [r for r in q.operators() if r["operator_spec_hash"] in {n["operator_spec_hash"] for n in nodes}]
    for row in operators:
        row["n_pipelines"] = len({n["pipeline_dag_hash"] for n in nodes
                                  if n["operator_spec_hash"] == row["operator_spec_hash"]})
    parameter_values = [r for r in store.query("""SELECT pn.pipeline_dag_hash, pv.* FROM pipeline_node_params pn
                JOIN parameter_values pv ON pv.param_value_hash = pn.param_value_hash WHERE pv.is_sweepable = 1""")
                        if r["pipeline_dag_hash"] in pdhs and r["kind"] in {"numeric", "bool"}]
    parameters = [{"name": name, "n_values": len({r["param_value_hash"] for r in parameter_values
                                               if r["name"] == name}),
                   "numeric": int(any(r["is_numeric"] for r in parameter_values if r["name"] == name))}
                  for name in sorted({r["name"] for r in parameter_values})]
    # Facets stringify parameter values. Consult the typed parameter record so a
    # free-form string such as "True" or "12.5" is not mistaken for a safe value.
    safe_parameter_facets = {
        (r["pipeline_dag_hash"], "param:" + r["name"], str(json.loads(r["value_json"])))
        for r in parameter_values
    }
    condition_pipelines = {r["run_condition_hash"]: r["pipeline_dag_hash"] for r in rows}
    facets = [r for r in store.query(
        "SELECT run_condition_hash, facet_key, facet_value, facet_num, role FROM run_facets")
              if r["run_condition_hash"] in conditions and (
                  not r["facet_key"].startswith("param:") or
                  (condition_pipelines[r["run_condition_hash"]], r["facet_key"], r["facet_value"])
                  in safe_parameter_facets
              )]
    # Collection descriptions/metadata can describe private runs in the same
    # collection. Publish the selected condition's id only, not that free text.
    collections = [{"collection_id": cid, "name": cid} for cid in sorted(cids)]
    overview = {
        "datasets": len(datasets), "dataset_cards": len(datasets), "tasks": len({r["task_hash"] for r in rows}),
        "pipelines": len(pipelines), "operators": len(operators), "parameters": len(parameters),
        "run_conditions": len(conditions), "executions": len(executions), "valid_executions": len(executions),
        "unreported_executions": 0, "quarantined_executions": 0, "include_unreported": False,
        "score_sets": len({r["score_set_id"] for r in rows}), "metric_observations": len(rows),
        "total_metric_observations": len(rows), "collections": len(collections),
        "residual_sets": sum(store.count("residual_sets", "execution_hash = ?", (eh,)) for eh in executions),
        "metrics": sorted({r["metric_name"] for r in rows}), "score_scopes": sorted({r["score_scope"] for r in rows}),
        "schema_version": int(store.get_meta("arena_schema_version") or 0),
    }
    metric_columns = ("execution_hash", "run_condition_hash", "execution_validity", "execution_status",
                      "producer_capsule",
                      "time_ms", "pipeline_dag_hash", "dataset_fingerprint", "main_model", "pipeline_label",
                      "score_scope", "score_validity", "metric_name", "metric_value", "partition")
    metrics = [{key: row[key] for key in metric_columns} for row in rows]
    strings: dict[str, int] = {}
    tables = {"pipelines": _columnar(pipelines, strings), "metrics": _columnar(metrics, strings),
              "facets": _columnar(facets, strings),
              "pipeline_nodes": _columnar([
                  {k: r[k] for k in ("pipeline_dag_hash", "role", "operator")} for r in nodes], strings)}
    return {"meta": {"version": __version__, "static": True}, "overview": overview,
            "datasets": datasets, "operators": operators, "parameters": parameters, "collections": collections,
            "planned": [], "strings": list(strings), "tables": tables}


def _public_run_document(store: ArenaStore, q: Queries, eh: str, bundle: dict[str, Any]) -> dict[str, Any]:
    if eh not in _execution_hashes(bundle):
        raise ValueError("execution is outside the public snapshot")
    detail = q.run_detail(eh)
    assert detail is not None
    execution_keys = ("execution_hash", "run_condition_hash", "status", "validity_status", "time_ms",
                      "oof_enforced", "group_leakage_checked", "nested_cv_safe")
    detail["execution"] = {k: detail["execution"].get(k) for k in execution_keys}
    for block in ("residual_set", "run_condition", "pipeline", "cv", "rng", "refit", "dataset"):
        if detail.get(block):
            detail[block] = {k: v for k, v in detail[block].items()
                             if not k.endswith(("_json", "_path")) and k not in {"failure_code", "failure_message"}}
    # The same pipeline/dataset keys may have first been inserted by a private
    # execution; use the selected public receipt for descriptive metadata.
    receipt = detail["metadata"]
    detail["pipeline"]["human_label"] = receipt["pipeline"].get("human_label")
    for key in ("nirs4all_identity_hash", "engine_graph_fingerprint"):
        detail["pipeline"][key] = receipt["pipeline"].get(key)
    for block in ("cv", "rng", "refit"):
        detail[block] = receipt.get(block, {})
    detail["pipeline"].pop("graph", None)
    detail["dataset"] = next(row for row in bundle["datasets"]
                             if row["dataset_fingerprint"] == detail["run_condition"]["dataset_fingerprint"])
    detail["nodes"] = [{k: n[k] for k in ("node_id", "role", "operator", "operator_version")} for n in detail["nodes"]]
    detail["node_params"] = []
    detail["scores"] = store.query("""SELECT metric_name, metric_value, fold_id, source_fold_id, aggregation_level,
                                         partition,
                                         direction, score_scope AS scope
                                  FROM v_ranked_metrics WHERE execution_hash = ?""", (eh,))
    def score_key(row):
        return tuple(row.get(k) for k in (
            "metric_name", "metric_value", "fold_id", "source_fold_id", "aggregation_level", "partition", "scope"))

    eligible_scores = {score_key(row) for row in detail["scores"]}
    receipt["scores"]["observations"] = [obs for obs in receipt["scores"]["observations"]
                                        if score_key(obs) in eligible_scores]
    return {"detail": detail, "residuals": q.residuals(eh)}
