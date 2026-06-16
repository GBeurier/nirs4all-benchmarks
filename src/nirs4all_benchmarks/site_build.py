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
      data/
        bundle.json             # aggregation backbone (loaded on boot)
        runs/<execution_hash>.json   # per-run detail + residuals (loaded on demand)
"""

from __future__ import annotations

import json
import shutil
from pathlib import Path
from typing import Any

from nirs4all_benchmarks import __version__
from nirs4all_benchmarks.store import ArenaStore, Queries

_WEB_DIR = Path(__file__).resolve().parent / "web"
# Files copied verbatim into the static site (everything the SPA needs at runtime).
_COPY = ["index.html", "app.js", "styles.css", "lib", "views", "vendor", "brand"]


def build_site(
    store_root: str | Path, out_dir: str | Path, *, domain: str | None = "benchmarks.nirs4all.org"
) -> dict[str, Any]:
    """Build a fully static, client-side copy of the Arena from a store. Returns a summary."""
    out = Path(out_dir)
    out.mkdir(parents=True, exist_ok=True)

    # 1. copy the SPA
    for name in _COPY:
        src = _WEB_DIR / name
        if not src.exists():
            continue
        dst = out / name
        if src.is_dir():
            shutil.copytree(src, dst, dirs_exist_ok=True)
        else:
            shutil.copy2(src, dst)

    # 2. static-mode flag + custom domain
    (out / "config.js").write_text("window.ARENA_STATIC = true;\n", encoding="utf-8")
    if domain:
        (out / "CNAME").write_text(domain + "\n", encoding="utf-8")
    (out / ".nojekyll").write_text("", encoding="utf-8")

    # 3. the data bundle
    data_dir = out / "data"
    runs_dir = data_dir / "runs"
    runs_dir.mkdir(parents=True, exist_ok=True)

    with ArenaStore(store_root) as store:
        q = Queries(store)
        bundle = _build_bundle(store, q)
        (data_dir / "bundle.json").write_text(json.dumps(bundle, separators=(",", ":")), encoding="utf-8")

        execs = [r["execution_hash"] for r in store.query("SELECT execution_hash FROM executions")]
        for eh in execs:
            detail = q.run_detail(eh)
            residuals = q.residuals(eh)
            (runs_dir / f"{eh}.json").write_text(
                json.dumps({"detail": detail, "residuals": residuals}, separators=(",", ":")), encoding="utf-8"
            )

    return {"out": str(out), "executions": len(execs), "pipelines": bundle["overview"]["pipelines"],
            "datasets": bundle["overview"]["datasets"], "metric_rows": len(bundle["metrics"]),
            "facet_rows": len(bundle["facets"]), "domain": domain}


def _build_bundle(store: ArenaStore, q: Queries) -> dict[str, Any]:
    overview = q.overview()
    # The v_run_metrics backbone — every aggregation query runs off these rows.
    metrics = store.query(
        """SELECT execution_hash, run_condition_hash, execution_validity, execution_status, producer_capsule,
                  time_ms, pipeline_dag_hash, dataset_fingerprint, task_hash, main_model,
                  pipeline_label, score_set_id, score_scope, score_validity, score_computation_hash,
                  metric_name, metric_value, direction, fold_id, partition, aggregation_level, n_samples
           FROM v_run_metrics"""
    )
    facets = store.query("SELECT run_condition_hash, facet_key, facet_value, facet_num, role FROM run_facets")
    pipeline_nodes = store.query(
        "SELECT pipeline_dag_hash, node_id, role, operator, operator_version FROM pipeline_nodes"
    )
    return {
        "meta": {"version": __version__, "static": True},
        "overview": overview,
        "datasets": q.datasets(),
        "pipelines": q.pipelines(),
        "operators": q.operators(),
        "parameters": q.sweepable_parameters(),
        "collections": q.collections(),
        "planned": q.planned(),
        "metrics": metrics,
        "facets": facets,
        "pipeline_nodes": pipeline_nodes,
    }
