"""Static-site build produces a self-contained, client-queryable snapshot."""

from __future__ import annotations

import json
from pathlib import Path

from nirs4all_benchmarks.fixtures import seed_store
from nirs4all_benchmarks.site_build import build_site


def test_build_site_produces_static_snapshot(tmp_path: Path):
    seed_store(tmp_path / "store", collection_id="demo", demo=True)
    out = tmp_path / "site"
    summary = build_site(tmp_path / "store", out, domain="benchmarks.nirs4all.org")

    # SPA copied + static flag + custom domain
    assert (out / "index.html").exists()
    assert (out / "app.js").exists()
    assert (out / "lib" / "static-engine.js").exists()
    assert (out / "brand" / "icon.svg").exists()
    assert (out / "config.js").read_text().strip() == "window.ARENA_STATIC = true;"
    assert (out / "CNAME").read_text().strip() == "benchmarks.nirs4all.org"
    assert "Sitemap: https://benchmarks.nirs4all.org/sitemap.xml" in (out / "robots.txt").read_text()
    assert "<loc>https://benchmarks.nirs4all.org/</loc>" in (out / "sitemap.xml").read_text()
    assert (out / ".nojekyll").exists()

    # bundle is well-formed and carries the aggregation backbone
    bundle = json.loads((out / "data" / "bundle.json").read_text())
    assert bundle["meta"]["static"] is True
    assert bundle["overview"]["pipelines"] == summary["pipelines"]
    assert bundle["metrics"] and bundle["facets"] and bundle["pipeline_nodes"]
    assert {"datasets", "pipelines", "operators", "parameters", "planned"} <= set(bundle)

    # one per-run file per execution, with detail + residuals
    run_files = list((out / "data" / "runs").glob("*.json"))
    assert len(run_files) == summary["executions"] > 0
    one = json.loads(run_files[0].read_text())
    assert "detail" in one and "residuals" in one
