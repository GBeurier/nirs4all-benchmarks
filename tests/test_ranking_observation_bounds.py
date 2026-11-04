"""Original score identity prevents join amplification and unbounded fit inputs."""

from __future__ import annotations

import importlib

import pytest

from nirs4all_benchmarks.ranking import component_analysis, quality_ranking, quality_relative_gain
from nirs4all_benchmarks.ranking.observations import original_observations
from nirs4all_benchmarks.store import ArenaStore
from test_component_analysis import MixedCapture, populate, request
from test_quality_ranking import Capture, add


def duplicate(store, count):
    row = store.query("SELECT * FROM metric_observations LIMIT 1")[0]
    store.conn.executemany(
        "INSERT INTO metric_observations(score_set_id,metric_name,metric_value,direction,partition,fold_id,aggregation_level) VALUES(?,?,?,?,?,?,?)",
        [
            (
                row["score_set_id"],
                row["metric_name"],
                row["metric_value"],
                row["direction"],
                row["partition"],
                row["fold_id"],
                row["aggregation_level"],
            )
        ]
        * count,
    )
    store.conn.commit()


def test_primary_key_reader_fetches_duplicates_linearly_and_preserves_high_values(tmp_path, monkeypatch):
    with ArenaStore(tmp_path) as store:
        populate(store)
        duplicate(store, 50)
        for i, value in enumerate((1e300, 5e299, 1e300)):
            add(store, "D1", "A", value, metric="nrmse", execution=f"high-{i}")
        query = store.query
        materialized = []

        def capture(sql, args=()):
            rows = query(sql, args)
            if "SELECT mo.metric_observation_id" in sql:
                materialized.append(len(rows))
            return rows

        monkeypatch.setattr(store, "query", capture)
        batch = original_observations(store, metric="nrmse", scope="cv", partition="validation")
        assert materialized == [65]  # Previously 2,615 rows from the value-based self-join.
        assert batch.source_count == 65 and not batch.limit_exceeded
        assert len({row["metric_observation_id"] for row in batch.rows}) == 65
        assert [row["metric_value"] for row in batch.rows[-3:]] == [1e300, 5e299, 1e300]
        assert batch.scan == {"limit": 100000, "limit_exceeded": False, "count_is_lower_bound": False}
        component = component_analysis(store, request(), estimator=MixedCapture())
        ranking = quality_ranking(store, {"metric": "nrmse"}, ranker=Capture())
        gain = quality_relative_gain(
            store,
            {"metric": "nrmse", "baseline_id": "A", "candidate_id": "B"},
            estimator=lambda *args, **kwargs: {"schema_version": "n4m.relative_gain/1"},
        )
        for result in (component, ranking, gain):
            assert result["state"] == "ready", result
            assert result["provenance"]["source_observations"] == 65
            assert result["provenance"]["excluded"] == {"duplicate_observation": 50}
        assert component["provenance"]["accepted_observations"] == 15


@pytest.mark.parametrize("analysis", ["ranking", "gain", "component"])
def test_source_cap_refuses_before_wide_fetch_without_partial_fit(tmp_path, monkeypatch, analysis):
    module = importlib.import_module("nirs4all_benchmarks.ranking.observations")
    monkeypatch.setattr(module, "SOURCE_OBSERVATION_LIMIT", 16)
    with ArenaStore(tmp_path) as store:
        populate(store)
        duplicate(store, 50)

        def no_wide_fetch(*args, **kwargs):
            raise AssertionError("oversized cohorts must be refused before fetching wide rows")

        monkeypatch.setattr(store, "query", no_wide_fetch)
        if analysis == "component":
            result = component_analysis(store, request(), estimator=no_wide_fetch)
        elif analysis == "ranking":
            result = quality_ranking(store, {"metric": "nrmse"}, ranker=no_wide_fetch)
        else:
            result = quality_relative_gain(
                store, {"metric": "nrmse", "baseline_id": "A", "candidate_id": "B"}, estimator=no_wide_fetch
            )
        assert result["state"] == "unavailable"
        assert result["reason"] == "source_observation_limit_requires_filter"
        provenance = result["provenance"]
        assert provenance["source_observations"] == 17  # An explicitly labeled lower bound, not a false exact count.
        assert provenance["source_scan"] == {"limit": 16, "limit_exceeded": True, "count_is_lower_bound": True}
        assert provenance["accepted_observations"] == 0 and provenance["excluded"] == {}
        assert any("at least 17" in warning and "No truncated cohort" in warning for warning in result["warnings"])


def test_source_limit_is_checked_again_after_preflight(tmp_path, monkeypatch):
    module = importlib.import_module("nirs4all_benchmarks.ranking.observations")
    monkeypatch.setattr(module, "SOURCE_OBSERVATION_LIMIT", 16)
    with ArenaStore(tmp_path) as store:
        populate(store)
        query = store.query

        def changed_between_queries(sql, args=()):
            if "SELECT mo.metric_observation_id" in sql:
                # Model an importer committing additional observations after the
                # narrow scan but before the bounded, identity-preserving fetch.
                duplicate(store, 10)
            return query(sql, args)

        # duplicate() itself queries through store.query, so keep that tiny read direct.
        def race(sql, args=()):
            if "SELECT * FROM metric_observations LIMIT 1" in sql:
                return query(sql, args)
            return changed_between_queries(sql, args)

        monkeypatch.setattr(store, "query", race)
        batch = original_observations(store, metric="nrmse", scope="cv", partition="validation")
        assert batch.limit_exceeded and batch.rows == [] and batch.source_count == 17


def test_filtered_ranking_scopes_metadata_and_component_reuses_graphs(tmp_path, monkeypatch):
    with ArenaStore(tmp_path) as store:
        populate(store)
        store.upsert(
            "pipeline_dags",
            {"pipeline_dag_hash": "unrelated", "graph_json": "{}", "created_at": "2026-11-04T00:00:00Z"},
        )
        query = store.query

        def scoped(sql, args=()):
            if any(f"FROM {table}" in sql for table in ("pipeline_dags", "pipeline_nodes", "pipeline_edges")):
                assert "WHERE pipeline_dag_hash IN" in sql and "unrelated" not in args
            return query(sql, args)

        monkeypatch.setattr(store, "query", scoped)
        ranking = quality_ranking(
            store, {"metric": "nrmse", "dimension": "model", "comparison_mode": "matched"}, ranker=Capture()
        )
        assert ranking["state"] == "ready", ranking
        get = store.get
        graphs = []

        def cached(table, key, value):
            if table == "pipeline_dags":
                graphs.append(value)
            return get(table, key, value)

        monkeypatch.setattr(store, "get", cached)
        assert component_analysis(store, request(), estimator=MixedCapture())["state"] == "ready"
        assert sorted(graphs) == ["A", "B"]
