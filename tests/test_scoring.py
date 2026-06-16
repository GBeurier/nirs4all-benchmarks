"""Metric correctness + score recomputation."""

from __future__ import annotations

import math

import pytest

from nirs4all_benchmarks.scoring import (
    compute_classification_metrics,
    compute_regression_metrics,
    direction_of,
    recompute_observations,
)
from nirs4all_benchmarks.scoring.score_spec import ScoreComputationSpec


def test_regression_perfect_prediction():
    m = compute_regression_metrics([1, 2, 3, 4], [1, 2, 3, 4])
    assert m["rmse"] == pytest.approx(0.0)
    assert m["mae"] == pytest.approx(0.0)
    assert m["r2"] == pytest.approx(1.0)


def test_regression_known_values():
    m = compute_regression_metrics([1.0, 2.0, 3.0], [1.5, 2.0, 2.5])
    assert m["mae"] == pytest.approx((0.5 + 0 + 0.5) / 3)
    assert m["rmse"] == pytest.approx(math.sqrt((0.25 + 0 + 0.25) / 3))
    assert m["bias"] == pytest.approx(0.0)


def test_directions():
    assert direction_of("rmse") == "min"
    assert direction_of("r2") == "max"
    assert direction_of("accuracy") == "max"
    assert direction_of("unknown_metric") == "min"


def test_classification_accuracy_and_f1():
    yt = ["a", "a", "b", "b"]
    yp = ["a", "b", "b", "b"]
    m = compute_classification_metrics(yt, yp)
    assert m["accuracy"] == pytest.approx(0.75)
    assert 0.0 <= m["f1_macro"] <= 1.0
    assert "mcc" in m


def test_classification_auc_and_logloss_with_proba():
    yt = [0, 0, 1, 1]
    yp = [0, 0, 1, 1]
    proba = [[0.9, 0.1], [0.8, 0.2], [0.3, 0.7], [0.2, 0.8]]
    m = compute_classification_metrics(yt, yp, y_proba=proba, labels=[0, 1])
    assert m["roc_auc"] == pytest.approx(1.0)
    assert m["log_loss"] > 0


def test_recompute_observations_groups_by_scope_fold_partition():
    rows = [
        {"scope": "cv", "fold_id": "fold0", "partition": "validation", "y_true": 1.0, "y_pred": 1.1},
        {"scope": "cv", "fold_id": "fold0", "partition": "validation", "y_true": 2.0, "y_pred": 1.9},
        {"scope": "test", "fold_id": None, "partition": "test", "y_true": 3.0, "y_pred": 3.2},
    ]
    obs = recompute_observations(rows, "regression")
    scopes = {(o["scope"], o["partition"]) for o in obs}
    assert ("cv", "validation") in scopes
    assert ("test", "test") in scopes
    assert all("metric_value" in o and "direction" in o for o in obs)


def test_score_computation_hash_is_stable_and_versioned():
    a = ScoreComputationSpec(score_version="1.0")
    b = ScoreComputationSpec(score_version="1.0")
    c = ScoreComputationSpec(score_version="2.0")
    assert a.score_computation_hash == b.score_computation_hash
    assert a.score_computation_hash != c.score_computation_hash
