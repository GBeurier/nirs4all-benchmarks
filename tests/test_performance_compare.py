"""Performance comparison harness smoke test."""

from __future__ import annotations

import math
from pathlib import Path

from nirs4all_benchmarks.performance_compare import render_markdown, run_comparison


def test_run_comparison_reports_both_surfaces(tmp_path: Path):
    report = run_comparison(repeats=1, warmups=0)

    assert report["case"]["name"] == "seeded_small_pls_cv"
    assert set(report["suites"]) == {"python_run", "studio_run"}

    for suite_name, suite in report["suites"].items():
        assert suite["label"]
        legacy = suite["engines"]["legacy"]
        dagml = suite["engines"]["dag-ml"]
        assert "error" not in legacy, f"{suite_name} legacy failed: {legacy.get('error')}"
        assert "error" not in dagml, f"{suite_name} dag-ml failed: {dagml.get('error')}"
        assert legacy["engine_recorded"] == "legacy"
        assert dagml["engine_recorded"] == "dag-ml"
        ratio = suite["ratios"]["run_s_dag_ml_over_legacy"]
        assert isinstance(ratio, float)
        assert math.isfinite(ratio)
        assert ratio > 0

    markdown = render_markdown(report)
    assert "nirs4all.run() direct" in markdown
    assert "Studio training worker" in markdown
    assert "dag-ml/legacy run ratio" in markdown
