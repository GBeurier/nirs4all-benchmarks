"""Serving queries over a seeded store."""

from __future__ import annotations

from nirs4all_benchmarks.store import Queries


def test_overview_counts(queries: Queries):
    ov = queries.overview()
    assert ov["datasets"] == 2
    assert ov["pipelines"] == 7
    assert ov["run_conditions"] == 14
    assert "rmse" in ov["metrics"]


def test_leaderboard_sorted_by_direction(queries: Queries):
    lb = queries.leaderboard(metric="rmse", scope="cv")
    means = [r["mean"] for r in lb["rows"]]
    assert lb["direction"] == "min"
    assert means == sorted(means)  # ascending for a min-metric
    assert lb["rows"][0]["rank"] == 1


def test_leaderboard_r2_is_descending(queries: Queries):
    lb = queries.leaderboard(metric="r2", scope="cv")
    means = [r["mean"] for r in lb["rows"]]
    assert lb["direction"] == "max"
    assert means == sorted(means, reverse=True)


def test_matrix_shape(queries: Queries):
    m = queries.matrix(metric="rmse", scope="cv")
    assert len(m["pipelines"]) == 7
    assert len(m["datasets"]) == 2
    assert len(m["cells"]) == 14


def test_parameter_effect_n_components(queries: Queries):
    pe = queries.parameter_effect("n_components", metric="rmse", scope="cv")
    assert pe["points"]
    numeric = sorted({p["numeric"] for p in pe["points"] if p["numeric"] is not None})
    assert numeric == [5.0, 8.0, 12.0, 16.0, 20.0]


def test_operator_effect(queries: Queries):
    oe = queries.operator_effect(metric="rmse", scope="cv")
    assert oe["series"]
    assert all("values" in s and s["n"] > 0 for s in oe["series"])


def test_robustness(queries: Queries):
    rob = queries.robustness(metric="rmse", scope="cv")
    assert rob
    assert all("stdev" in r for r in rob)


def test_residual_compare_is_sample_keyed(queries: Queries):
    # two pipelines on the same dataset must share pseudonymized sample ids
    execs = queries.store.query(
        """SELECT e.execution_hash, rc.dataset_fingerprint FROM executions e
           JOIN run_conditions rc ON rc.run_condition_hash = e.run_condition_hash"""
    )
    df0 = execs[0]["dataset_fingerprint"]
    same = [e for e in execs if e["dataset_fingerprint"] == df0][:2]
    cmp = queries.residual_compare(same[0]["execution_hash"], same[1]["execution_hash"])
    assert cmp["n_common"] > 50
    assert cmp["residual_correlation"] is not None


def test_run_detail(queries: Queries):
    ex = queries.store.query("SELECT execution_hash FROM executions LIMIT 1")[0]["execution_hash"]
    d = queries.run_detail(ex)
    assert d is not None
    assert d["nodes"] and d["scores"]
    assert d["pipeline"]["graph"] is not None


def test_invalid_scope_raises(queries: Queries):
    import pytest

    with pytest.raises(ValueError):
        queries.leaderboard(metric="rmse", scope="nonsense")
