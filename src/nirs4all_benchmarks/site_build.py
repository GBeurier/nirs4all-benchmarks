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
        _write_seo(out, domain)
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


def _build_bundle(store: ArenaStore, q: Queries) -> dict[str, Any]:
    overview = q.overview()
    # The v_run_metrics backbone — every aggregation query runs off these rows. Only the
    # columns the static engine (web/lib/static-engine.js) reads are exported.
    metrics = store.query(
        """SELECT execution_hash, run_condition_hash, execution_validity, execution_status, producer_capsule,
                  time_ms, pipeline_dag_hash, dataset_fingerprint, main_model, pipeline_label,
                  score_scope, score_validity, metric_name, metric_value, partition
           FROM v_run_metrics"""
    )
    facets = store.query("SELECT run_condition_hash, facet_key, facet_value, facet_num, role FROM run_facets")
    pipeline_nodes = store.query("SELECT pipeline_dag_hash, role, operator FROM pipeline_nodes")
    strings: dict[str, int] = {}
    tables = {
        "pipelines": _columnar(q.pipelines(), strings),
        "metrics": _columnar(metrics, strings),
        "facets": _columnar(facets, strings),
        "pipeline_nodes": _columnar(pipeline_nodes, strings),
    }
    return {
        "meta": {"version": __version__, "static": True},
        "overview": overview,
        "datasets": q.datasets(),
        "operators": q.operators(),
        "parameters": q.sweepable_parameters(),
        "collections": q.collections(),
        "planned": q.planned(),
        "strings": list(strings),
        "tables": tables,
    }
