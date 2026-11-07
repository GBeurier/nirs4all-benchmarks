"""Self-contained synthetic production probes mirroring Opus T1/T2 and D1/D2.

No producer import, scientific execution, user store or static site is needed.
"""

from __future__ import annotations

import json
import sqlite3

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from conftest import declare_synthetic_safety
from nirs4all_benchmarks.adapters import workspace_to_exports
from nirs4all_benchmarks.contract.folds import is_pseudo_fold
from nirs4all_benchmarks.ingestion import ingest_export, validate_metadata
from nirs4all_benchmarks.store import ArenaStore, Queries

REAL = [str(i) for i in range(5)]
# Keep this owner-evidence fixture independent of the implementation's set.
PSEUDO = ["final", "final_agg", "avg", "w_avg", "ensemble", "all", "macro",
          "fold_avg", "fold_w_avg", "_agg", "custom_agg", "", " ", "avg ", " w_avg", " x_agg\t"]


def workspace(root, ids, *, expected=5, term="completed", status=None, extra=(), fold_type="TEXT"):
    """Use the reviewer's T1 SQLite shape, including score-only predictions."""
    root.mkdir()
    (root / "arrays").mkdir()
    with sqlite3.connect(root / "store.sqlite") as c:
        assert fold_type in {"TEXT", ""}
        c.executescript(f"""
            CREATE TABLE pipelines(pipeline_id TEXT, name TEXT, expanded_config TEXT, dataset_name TEXT,
              dataset_hash TEXT, metric TEXT, task_hash TEXT, dataset_variant_hash TEXT, status TEXT,
              terminal_status TEXT, expected_fold_count INT);
            CREATE TABLE predictions(prediction_id TEXT, pipeline_id TEXT, dataset_name TEXT, model_name TEXT,
              fold_id {fold_type}, partition TEXT, val_score REAL, metric TEXT, task_type TEXT, n_samples INT,
              n_features INT, scores TEXT);
        """)
        steps = json.dumps([{"model": {"class": "sklearn.cross_decomposition.PLSRegression"},
                             "params": {"n_components": 3}}])
        c.execute("INSERT INTO pipelines VALUES (?,?,?,?,?,?,?,?,?,?,?)",
                  ("p1", "PLS", steps, "corn", "a" * 64, "rmse", "2" * 64, "3" * 64,
                   ("completed" if term == "completed" and status is None else status), term, expected))
        for i, (fold, partition) in enumerate([(f, "validation") for f in ids] + list(extra)):
            c.execute("INSERT INTO predictions VALUES (?,?,?,?,?,?,?,?,?,?,?,?)",
                      (f"r{i}", "p1", "corn", "PLS", fold, partition, 0.7, "rmse", "regression",
                       30, 700, json.dumps({"rmse": 0.7})))
    return root


@pytest.mark.parametrize("ids,expected,term,status,extra,verdict,ranked", [
    pytest.param(REAL[:2], 5, "completed", None, (), "rejected", False, id="T1-2of5-completed"),
    pytest.param(REAL, 5, "completed", None, (), "committed", True, id="T1-5of5-completed"),
    pytest.param(REAL[:2], 5, "incomplete", "failed", (), "committed", False, id="T1-incomplete"),
    pytest.param(REAL[:2], 5, None, None, (), "committed", False, id="T1-unreported"),
    pytest.param([*REAL, "final", "_agg"], 5, "completed", None, (), "committed", True, id="T1-5-plus-final-agg"),
    pytest.param([*REAL[:1], "final", "_agg"], 5, "completed", None, (), "rejected", False, id="T1-1-plus-final-agg"),
    pytest.param([*REAL, "avg", "w_avg"], 5, "completed", None, (), "committed", True, id="T1-5-plus-avg-wavg"),
    pytest.param([*REAL[:3], "avg", "w_avg"], 5, "completed", None, (), "rejected", False, id="T1-3-plus-avg-wavg"),
    pytest.param(REAL, 5, "completed", None, [(f, "test") for f in REAL], "committed", True, id="T1-test-rows"),
    pytest.param(REAL[:2], None, "completed", None, (), "committed", True, id="T1-no-expected"),
    pytest.param(["fold0", "split/A", "east", "trial-4", "FINAL"], 5, "completed", None,
                 (), "committed", True, id="D1-arbitrary-genuine-ids"),
    pytest.param(REAL + PSEUDO, 5, "completed", None, (), "committed", True, id="D1-all-owner-sentinels"),
])
def test_t1_workspace_production_path(tmp_path, ids, expected, term, status, extra, verdict, ranked):
    root = workspace(tmp_path / "workspace", ids, expected=expected, term=term, status=status, extra=extra)
    if verdict == "rejected":
        with pytest.raises(ValueError,match="incomplete_metric_fold_coverage"):
            workspace_to_exports(root)
        return
    export, = workspace_to_exports(root)
    observed = {o.fold_id for o in export.scores.observations if o.fold_id is not None}
    assert observed == {f for f in ids if f not in PSEUDO} | {f for f, _ in extra if f not in PSEUDO}
    assert not export.residuals.inline
    with ArenaStore(tmp_path / "store") as store:
        result = ingest_export(store, declare_synthetic_safety(export))
        assert result.status == verdict
        assert bool(Queries(store).leaderboard(metric="rmse", scope="cv")["rows"]) is ranked
        if verdict == "rejected":
            assert result.issues[0]["code"] == "incomplete_metric_fold_coverage"
            assert store.count("executions") == 0 and not list(store.exports_dir.iterdir())
        else:
            assert result.clean_report["metadata_validation"]["fold_coverage"] == "reported"
            assert result.terminal_status == (term or "unreported")
            assert ingest_export(store, declare_synthetic_safety(export)).status == "already_ingested"


def folds(ids, scope="fold", partition="validation", metric="rmse"):
    from test_arena_v2 import observation
    result=[]
    for fold in ids:
        if is_pseudo_fold(fold):
            key=str(fold).strip()
            result.append(observation(1.0,fold=None,label=str(fold),
                scope="refit" if key in {"final","final_agg"} else "view",partition=partition,
                level="group" if key.endswith("_agg") else "sample"))
        else:
            result.append(observation(1.0,fold=str(fold),scope=scope,partition=partition))
        result[-1]["metric_name"]=metric
    return result


def aggregate(metric="rmse"):
    from test_arena_v2 import observation
    return [{**observation(1.0,fold=None),"metric_name":metric}]


def manifest(observations, **cv):
    from test_arena_v2 import record
    source=record()
    source["scores"]["observations"]=observations
    source["cv"]={"method":"kfold","n_folds":5,**cv}
    return source


@pytest.mark.parametrize("observations,cv,error,coverage", [
    pytest.param(folds(REAL[:2], "cv"), {}, "incomplete_metric_fold_coverage", "reported", id="T2-cv-2of5"),
    pytest.param(folds(REAL, "cv"), {}, None, "reported", id="T2-cv-5of5"),
    pytest.param(folds(REAL, "cv") + aggregate(), {}, None, "reported", id="T2-cv-plus-aggregate"),
    pytest.param(aggregate(), {}, None, "not_reported", id="T2-aggregate-only"),
    pytest.param(folds(REAL[:3]) + folds(REAL[3:], "cv"), {}, None, "reported", id="T2-mixed-scopes"),
    pytest.param(folds(REAL) + folds(REAL, "fold", "test"), {}, None, "reported", id="T2-fold-test"),
    pytest.param(folds(REAL, "cv") + folds(REAL, "cv", "test"), {}, None, "reported", id="T2-cv-test"),
    pytest.param(folds(REAL) + folds(["final"], "refit", "final"), {}, None, "reported", id="T2-refit"),
    pytest.param(folds(REAL) + folds(["_agg"], "cv", metric="mae"), {}, None, "reported", id="T2-other-metric-agg-id"),
    pytest.param(folds(REAL) + aggregate("mae"), {}, None, "reported", id="T2-other-metric-aggregate"),
    pytest.param(folds(REAL) + folds(REAL[:1], partition="train"), {}, "incomplete_metric_fold_coverage", "reported", id="T2-train-subset"),
    pytest.param(folds([*REAL, "5"]), {"nested": True}, "fold_count_contradiction", "not_reported", id="T2-nested-excess"),
    pytest.param(folds(list("abcd"), "cv"), {"n_folds": 2, "n_repeats": 2}, None, "reported", id="T2-repeats"),
])
def test_t2_native_metadata_path(observations, cv, error, coverage):
    result = validate_metadata(manifest(observations, **cv))
    assert result.status == ("rejected" if error else "metadata_valid")
    assert result.fold_coverage == coverage
    assert [issue["code"] for issue in result.issues] == ([error] if error else [])


def test_t2_incomplete_test_only_is_ingestible_and_unranked(empty_store):
    source = manifest(folds(REAL, partition="test"))
    source["execution"].update(status="failed", terminal_status="incomplete")
    result = ingest_export(empty_store, source)
    assert result.status == "committed" and result.terminal_status == "incomplete"
    assert result.clean_report["metadata_validation"]["fold_coverage"] == "not_reported"
    assert Queries(empty_store).leaderboard()["rows"] == []


@pytest.mark.parametrize("pseudo", PSEUDO)
@pytest.mark.parametrize("evidence", ["scores", "residuals"])
def test_every_owner_sentinel_is_excluded_from_offline_coverage(pseudo, evidence):
    def source(ids):
        value = manifest(folds(ids) if evidence == "scores" else aggregate())
        if evidence == "residuals":
            value["residuals"] = {"key":"sample_id", "publishable":dict.fromkeys(("y_true","y_pred","residual","y_proba"),True), "inline": [
                {"sample_id": f"s{i}", "fold_id": f, "scope": "cv", "partition":"validation", "y_true": 1.0, "y_pred": 1.0}
                for i, f in enumerate(ids)]}
        return value

    assert is_pseudo_fold(pseudo)
    if evidence == "residuals" and (not pseudo or pseudo != pseudo.strip()):
        assert validate_metadata(source([*REAL,pseudo])).issues[0]["code"] == "noncanonical_fold_id"
        return
    assert validate_metadata(source([*REAL, pseudo])).status == "metadata_valid"
    partial = validate_metadata(source([*REAL[:4], pseudo]))
    assert partial.status == "rejected"
    assert partial.issues[0]["code"] in {"incomplete_metric_fold_coverage", "incomplete_fold_coverage"}
    assert validate_metadata(source(["fold0", "region/A", "repeat:2", "avg2", "aggregate"])).status == "metadata_valid"


@pytest.mark.parametrize("scope", ["fold", "cv"])
def test_non_cv_rows_and_sentinels_do_not_create_metric_series(scope):
    source = manifest(folds(REAL, scope) + folds(REAL, scope, "test")
                      + folds(PSEUDO, scope, metric="mae"))
    assert validate_metadata(source).status == "metadata_valid"
    # A genuine but partial second series still has a meaningful gap.
    source["scores"]["observations"] += folds(["0"], scope, metric="mae")
    assert validate_metadata(source).issues[0]["code"] == "incomplete_metric_fold_coverage"


def test_workspace_aggregate_arrays_are_not_emitted_as_cv_residuals(tmp_path):
    ids = ["fold0", "split/A", "east", "trial-4", "FINAL", *PSEUDO]
    root = workspace(tmp_path / "workspace", ids)
    pq.write_table(pa.Table.from_pylist([
        {"prediction_id": f"r{i}", "sample_indices": [i], "y_true": [1.0], "y_pred": [1.5]}
        for i in range(len(ids))
    ]), root / "arrays" / "corn.parquet")
    export, = workspace_to_exports(root)
    residuals = export.residuals.inline
    assert residuals is not None
    assert {row["fold_id"] for row in residuals if row["scope"] == "cv"} == set(ids[:5])
    assert [(row["fold_id"], row["partition"]) for row in residuals if row["scope"] == "refit"] == [(None, "validation")]
    assert len(residuals) == 6  # Five genuine CV rows and the separately reported refit.
    assert all(o.fold_id is None for o in export.scores.observations[5:])
    assert validate_metadata(export).status == "metadata_valid"


@pytest.mark.parametrize("pseudo", ["", " ", "avg ", " w_avg", " final ", " final_agg ", "\tx_agg "])
@pytest.mark.parametrize("count", [4, 5])
def test_whitespace_pseudo_ids_cannot_pad_or_invalidate_workspace_coverage(tmp_path, pseudo, count):
    root = workspace(tmp_path / "workspace", [*REAL[:count], pseudo])
    if count == 4:
        with pytest.raises(ValueError,match="incomplete_metric_fold_coverage"):
            workspace_to_exports(root)
        return
    export, = workspace_to_exports(root)
    assert export.scores.observations[-1].fold_id is None
    with ArenaStore(tmp_path / "store") as store:
        result = ingest_export(store, declare_synthetic_safety(export))
        assert result.status == ("rejected" if count == 4 else "committed")
        assert bool(Queries(store).leaderboard()["rows"]) is (count == 5)


@pytest.mark.parametrize("pseudo_ids,scope", [
    (["avg", "w_avg"], "view"), (["final_agg"], "refit"), (["final"], "refit"),
    ([" final_agg "], "refit"), ([" avg ", " w_avg"], "view"),
    (["ensemble", "all", "macro", "fold_avg", "fold_w_avg", "x_agg", "", " "], "view"),
])
@pytest.mark.parametrize("partition", ["validation", "test"])
def test_pseudo_scores_remain_queryable_without_contaminating_cv_mean(tmp_path, pseudo_ids, scope, partition):
    root = workspace(tmp_path / "workspace", REAL, extra=[(p, partition) for p in pseudo_ids])
    with sqlite3.connect(root / "store.sqlite") as c:
        c.execute("UPDATE predictions SET scores=?, val_score=1.0 WHERE prediction_id IN ('r0','r1','r2','r3','r4')",
                  (json.dumps({"rmse": 1.0}),))
        c.execute("UPDATE predictions SET scores=?, val_score=0.1 WHERE prediction_id NOT IN ('r0','r1','r2','r3','r4')",
                  (json.dumps({"rmse": 0.1}),))
    export, = workspace_to_exports(root)
    assert all(o.scope == scope and o.fold_id is None for o in export.scores.observations[5:])
    assert all(o.partition == partition for o in export.scores.observations[5:])
    with ArenaStore(tmp_path / "store") as store:
        result = ingest_export(store, declare_synthetic_safety(export))
        assert result.status == "committed"
        query = Queries(store)
        ranked, = query.leaderboard(metric="rmse", scope="cv")["rows"]
        assert ranked["mean"] == 1.0 and ranked["n_obs"] == 5
        detail = query.run_detail(result.execution_hash)
        retained = [o for o in detail["scores"] if o["scope"] == scope]
        assert len(retained) == len(pseudo_ids)
        assert all(o["metric_value"] == 0.1 for o in retained)


@pytest.mark.parametrize("fold_type", ["", "TEXT"])
def test_integer_workspace_fold_ids_with_or_without_type_affinity(tmp_path, fold_type):
    root = workspace(tmp_path / "workspace", list(range(5)), fold_type=fold_type)
    with sqlite3.connect(root / "store.sqlite") as c:
        actual_type, = c.execute("SELECT typeof(fold_id) FROM predictions LIMIT 1").fetchone()
        assert actual_type == ("text" if fold_type else "integer")
    export, = workspace_to_exports(root)
    assert {o.fold_id for o in export.scores.observations} == set(REAL)
    assert not is_pseudo_fold(0)
    assert validate_metadata(export).status == "metadata_valid"
