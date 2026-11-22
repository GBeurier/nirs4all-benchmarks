"""Serving queries over a seeded store."""

from __future__ import annotations

import hashlib
import sqlite3
from pathlib import Path

import pytest

from nirs4all_benchmarks.store import Queries


def _store_snapshot(root: Path) -> dict[str, object]:
    conn = sqlite3.connect(f"file:{root / 'arena.sqlite'}?mode=ro", uri=True)
    try:
        user_version = conn.execute("PRAGMA user_version").fetchone()[0]
        database = tuple(conn.iterdump())
    finally:
        conn.close()

    files = {}
    for path in sorted(p for p in root.rglob("*") if p.is_file()):
        if path.name.endswith("-shm"):
            continue
        files[str(path.relative_to(root))] = hashlib.sha256(path.read_bytes()).hexdigest()
    return {"user_version": user_version, "database": database, "files": files}


def _forbid_store_mutators(queries: Queries, monkeypatch: pytest.MonkeyPatch) -> None:
    def fail(*_args: object, **_kwargs: object) -> None:
        raise AssertionError("Queries.pipelines() must stay read-only")

    for name in ("set_meta", "upsert", "insert", "update", "ensure_collection", "transaction"):
        monkeypatch.setattr(queries.store, name, fail)


def test_overview_counts(queries: Queries):
    ov = queries.overview()
    assert ov["datasets"] == 2
    assert ov["pipelines"] == 7
    assert ov["run_conditions"] == 14
    assert "rmse" in ov["metrics"]


def test_pipelines_catalogue_is_read_only(queries: Queries, monkeypatch: pytest.MonkeyPatch):
    _forbid_store_mutators(queries, monkeypatch)
    before_snapshot = _store_snapshot(queries.store.root)
    before_changes = queries.store.conn.total_changes
    before = queries.overview()

    rows = queries.pipelines()

    after = queries.overview()
    after_snapshot = _store_snapshot(queries.store.root)

    assert before == after
    assert queries.store.conn.total_changes == before_changes
    assert after_snapshot == before_snapshot
    assert len(rows) == 7
    assert {row["n_run_conditions"] for row in rows} == {2}
    assert all(row["pipeline_dag_hash"] and row["human_label"] for row in rows)
    assert rows == sorted(rows, key=lambda row: (row["main_model"], row["human_label"]))


def test_leaderboard_sorted_by_direction(queries: Queries):
    lb = queries.leaderboard(metric="rmse", scope="cv", dataset_fingerprint=queries.datasets()[0]["dataset_fingerprint"])
    means = [r["mean"] for r in lb["rows"]]
    assert lb["direction"] == "min"
    assert means == sorted(means)  # ascending for a min-metric
    assert lb["rows"][0]["rank"] == 1


def test_leaderboard_r2_is_descending(queries: Queries):
    lb = queries.leaderboard(metric="r2", scope="cv", dataset_fingerprint=queries.datasets()[0]["dataset_fingerprint"])
    means = [r["mean"] for r in lb["rows"]]
    assert lb["direction"] == "max"
    assert means == sorted(means, reverse=True)


def test_dataset_leaderboard_reports_the_best_run_condition_per_target(queries: Queries):
    board = queries.dataset_leaderboard()
    rows = board["families"]["regression"]
    assert (board["scope"], board["partition"]) == ("cv", "validation")
    assert board["families"]["classification"] == []
    assert [r["dataset_name"] for r in rows] == ["Mock Corn", "Mock Wheat"]
    for row in rows:
        # The fixtures report no nRMSE, so each target falls back to RMSE.
        assert (row["metric"], row["direction"]) == ("rmse", "min")
        lb = queries.leaderboard(metric="rmse", scope="cv", dataset_fingerprint=row["dataset_fingerprint"],
                                 task_hash=row["task_hash"], limit=None)
        means = sorted(r["mean"] for r in lb["rows"] if r["metric_unit"] == row["metric_unit"])
        assert row["values"] == means and row["n_scores"] == len(means) > 1
        assert row["best_value"] == means[0]
        assert row["best"]["run_condition_hash"] == min(
            r["run_condition_hash"] for r in lb["rows"] if r["mean"] == means[0])


def test_dataset_leaderboard_files_classification_targets_and_names_missing_metrics(queries: Queries):
    task = queries.dataset_leaderboard()["families"]["regression"][0]["task_hash"]
    queries.store.conn.execute("UPDATE task_specs SET task_type = 'multiclass' WHERE task_hash = ?", (task,))
    board = queries.dataset_leaderboard()
    [row] = board["families"]["classification"]
    assert len(board["families"]["regression"]) == 1
    # Neither balanced accuracy nor accuracy is reported: no score is borrowed from another metric.
    assert row["task_hash"] == task and row["metric"] is None and row["best"] is None and row["values"] == []
    assert "rmse" in row["available_metrics"]


def test_matrix_shape(queries: Queries):
    m = queries.matrix(metric="rmse", scope="cv")
    assert len(m["pipelines"]) == 7
    assert len(m["datasets"]) == 2
    assert len(m["cells"]) == 14


def test_model_matrix_averages_pipeline_cells(queries: Queries):
    by_pipeline = queries.matrix(metric="rmse", scope="cv")
    by_model = queries.matrix(metric="rmse", scope="cv", by="model")
    assert by_model["datasets"] == by_pipeline["datasets"]
    assert by_model["models"] and "pipelines" not in by_model
    assert sum(c["n_pipelines"] for c in by_model["cells"]) == len(by_pipeline["cells"])
    for cell in by_model["cells"]:
        if cell["value"] is None:
            continue
        members = [c["value"] for c in by_pipeline["cells"] if c["dataset_fingerprint"] == cell["dataset_fingerprint"]
                   and _model_of(queries, c["pipeline_dag_hash"]) == cell["model"]]
        assert len(members) == cell["n_pipelines"]
        assert cell["value"] == pytest.approx(sum(members) / len(members))


def _model_of(queries: Queries, pipeline_dag_hash: str) -> str:
    return next(p["main_model"] or "" for p in queries.pipelines() if p["pipeline_dag_hash"] == pipeline_dag_hash)


def test_matrix_rejects_unknown_grouping(queries: Queries):
    with pytest.raises(ValueError):
        queries.matrix(metric="rmse", scope="cv", by="operator")


def test_parameter_effect_n_components(queries: Queries):
    pe = queries.parameter_effect("n_components", metric="rmse", scope="cv")
    assert pe["points"]
    numeric = sorted({p["numeric"] for p in pe["points"] if p["numeric"] is not None})
    assert numeric == [5.0, 8.0, 12.0, 16.0, 20.0]


@pytest.mark.parametrize(("metric", "descending"), [("rmse", False), ("r2", True)])
def test_runs_limit_keeps_the_best_runs(queries: Queries, metric: str, descending: bool):
    dataset = queries.datasets()[0]["dataset_fingerprint"]
    everything = queries.run_explorer(metric=metric, scope="cv", dataset_fingerprint=dataset, limit=100000)
    values = [r["metric_value"] for r in everything]
    assert len(values) > 3
    assert values == sorted(values, reverse=descending)
    # The limit is applied after the best-first ordering, not to an arbitrary prefix of the runs.
    assert [r["execution_hash"] for r in queries.run_explorer(metric=metric, scope="cv", dataset_fingerprint=dataset, limit=3)] == [
        r["execution_hash"] for r in everything[:3]
    ]


def test_operator_effect(queries: Queries):
    oe = queries.operator_effect(metric="rmse", scope="cv", dataset_fingerprint=queries.datasets()[0]["dataset_fingerprint"])
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


def test_analysis_queries_honor_the_dataset_lens(queries: Queries):
    ds = queries.datasets()[0]["dataset_fingerprint"]
    kw = {"metric": "rmse", "scope": "cv"}
    assert {r["dataset_fingerprint"] for r in queries.robustness(**kw, dataset_fingerprint=ds)} == {ds}
    assert {d["dataset_fingerprint"] for d in queries.matrix(**kw, dataset_fingerprint=ds)["datasets"]} == {ds}
    assert {d["dataset_fingerprint"] for d in queries.stats(**kw, dataset_fingerprint=ds)["by_dataset"]} == {ds}
    points = queries.parameter_effect("n_components", **kw, dataset_fingerprint=ds)["points"]
    assert points and {p["dataset_fingerprint"] for p in points} == {ds}
    assert 0 < len(queries.parallel(dimensions=["seed"], **kw, dataset_fingerprint=ds)["rows"]) < len(
        queries.parallel(dimensions=["seed"], **kw)["rows"]
    )
    totals = {
        "composition": lambda **k: sum(r["n_runs"] for r in queries.composition(**k)["rows"]),
        "pipeline_graph": lambda **k: sum(n["size"] for n in queries.pipeline_graph(**k)["nodes"]),
        "operator_graph": lambda **k: sum(n["n_runs"] for n in queries.operator_graph(**k)["nodes"]),
    }
    for name, total in totals.items():
        assert 0 < total(**kw, dataset_fingerprint=ds) < total(**kw), name
    assert sum(s["n"] for s in queries.operator_effect(**kw, dataset_fingerprint=ds)["series"]) > 0
    assert queries.operator_effect(**kw)["score_context"]["withheld"]["count"] > 0
