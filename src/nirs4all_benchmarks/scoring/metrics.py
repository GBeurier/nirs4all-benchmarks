"""NumPy-only metric registry — regression + classification.

The Arena recomputes scores from residuals so it never depends on a producer's
metric implementation (PERSISTENCE_FORMATS.md §6.2: classification metrics are
absent from dag-ml; nirs4all's are task-rich but producer-specific). Implementing
them here keeps scores *re-derivable and auditable*.
"""

from __future__ import annotations

from collections import defaultdict
from collections.abc import Sequence
from typing import Any

import numpy as np

# Optimization direction per metric — "min" = lower is better.
METRIC_DIRECTION: dict[str, str] = {
    "mse": "min",
    "rmse": "min",
    "mae": "min",
    "medae": "min",
    "bias": "min",
    "r2": "max",
    "rpd": "max",
    "rpiq": "max",
    "ccc": "max",
    "accuracy": "max",
    "balanced_accuracy": "max",
    "f1_macro": "max",
    "f1_micro": "max",
    "precision_macro": "max",
    "recall_macro": "max",
    "log_loss": "min",
    "roc_auc": "max",
    "mcc": "max",
}


def direction_of(metric_name: str) -> str:
    """Optimization direction of ``metric_name`` (defaults to ``min``)."""
    return METRIC_DIRECTION.get(metric_name.lower(), "min")


# ── regression ─────────────────────────────────────────────────────────

def compute_regression_metrics(y_true: Sequence[float], y_pred: Sequence[float]) -> dict[str, float]:
    """Standard NIRS regression metrics from aligned ``y_true``/``y_pred``."""
    yt = np.asarray(y_true, dtype=float)
    yp = np.asarray(y_pred, dtype=float)
    mask = np.isfinite(yt) & np.isfinite(yp)
    yt, yp = yt[mask], yp[mask]
    out: dict[str, float] = {}
    if yt.size == 0:
        return out
    err = yt - yp
    mse = float(np.mean(err**2))
    out["mse"] = mse
    out["rmse"] = float(np.sqrt(mse))
    out["mae"] = float(np.mean(np.abs(err)))
    out["medae"] = float(np.median(np.abs(err)))
    out["bias"] = float(np.mean(err))
    var = float(np.var(yt))
    if var > 0:
        out["r2"] = float(1.0 - mse / var)
        std = float(np.std(yt))
        if out["rmse"] > 0:
            out["rpd"] = std / out["rmse"]
            iqr = float(np.subtract(*np.percentile(yt, [75, 25])))
            out["rpiq"] = iqr / out["rmse"]
    # Lin's concordance correlation coefficient
    if yt.size > 1:
        cov = float(np.mean((yt - yt.mean()) * (yp - yp.mean())))
        denom = float(np.var(yt) + np.var(yp) + (yt.mean() - yp.mean()) ** 2)
        if denom > 0:
            out["ccc"] = 2 * cov / denom
    return out


# ── classification ─────────────────────────────────────────────────────

def _confusion(y_true: np.ndarray, y_pred: np.ndarray, labels: list[Any]) -> np.ndarray:
    idx = {lab: i for i, lab in enumerate(labels)}
    m = np.zeros((len(labels), len(labels)), dtype=float)
    for t, p in zip(y_true, y_pred, strict=False):
        if t in idx and p in idx:
            m[idx[t], idx[p]] += 1
    return m


def compute_classification_metrics(
    y_true: Sequence[Any],
    y_pred: Sequence[Any],
    y_proba: Sequence[Sequence[float]] | None = None,
    labels: Sequence[Any] | None = None,
) -> dict[str, float]:
    """Accuracy / macro-F1 / precision / recall / MCC (+ AUC, log-loss with proba)."""
    yt = np.asarray(list(y_true))
    yp = np.asarray(list(y_pred))
    if yt.size == 0:
        return {}
    labs = list(labels) if labels is not None else sorted(set(yt.tolist()) | set(yp.tolist()))
    cm = _confusion(yt, yp, labs)
    total = cm.sum()
    out: dict[str, float] = {}
    if total == 0:
        return out
    out["accuracy"] = float(np.trace(cm) / total)
    per_class_recall = []
    precisions, recalls, f1s = [], [], []
    for i in range(len(labs)):
        tp = cm[i, i]
        fp = cm[:, i].sum() - tp
        fn = cm[i, :].sum() - tp
        prec = tp / (tp + fp) if (tp + fp) > 0 else 0.0
        rec = tp / (tp + fn) if (tp + fn) > 0 else 0.0
        f1 = 2 * prec * rec / (prec + rec) if (prec + rec) > 0 else 0.0
        precisions.append(prec)
        recalls.append(rec)
        f1s.append(f1)
        per_class_recall.append(rec)
    out["precision_macro"] = float(np.mean(precisions))
    out["recall_macro"] = float(np.mean(recalls))
    out["f1_macro"] = float(np.mean(f1s))
    out["balanced_accuracy"] = float(np.mean(per_class_recall))
    # Matthews correlation coefficient (multiclass generalization)
    c, s = np.trace(cm), cm.sum()
    pk, tk = cm.sum(axis=0), cm.sum(axis=1)
    cov_ytyp = c * s - tk @ pk
    cov_ypyp = s * s - pk @ pk
    cov_ytyt = s * s - tk @ tk
    denom = np.sqrt(cov_ypyp * cov_ytyt)
    if denom > 0:
        out["mcc"] = float(cov_ytyp / denom)
    if y_proba is not None:
        proba = np.asarray([list(p) for p in y_proba], dtype=float)
        out.update(_proba_metrics(yt, proba, labs))
    return out


def _proba_metrics(y_true: np.ndarray, proba: np.ndarray, labels: list[Any]) -> dict[str, float]:
    idx = {lab: i for i, lab in enumerate(labels)}
    n, k = proba.shape
    if proba.shape[0] != y_true.shape[0]:
        return {}
    eps = 1e-15
    p = np.clip(proba, eps, 1 - eps)
    rows = np.arange(n)
    true_idx = np.array([idx.get(t, -1) for t in y_true])
    valid = true_idx >= 0
    out: dict[str, float] = {}
    if valid.any():
        ll = -np.mean(np.log(p[rows[valid], true_idx[valid]]))
        out["log_loss"] = float(ll)
    # Binary ROC AUC via the Mann–Whitney U statistic.
    if k == 2 and valid.any():
        scores = p[:, 1][valid]
        y = (true_idx[valid] == 1).astype(int)
        out_auc = _binary_auc(y, scores)
        if out_auc is not None:
            out["roc_auc"] = out_auc
    return out


def _binary_auc(y: np.ndarray, scores: np.ndarray) -> float | None:
    n_pos = int(y.sum())
    n_neg = int((1 - y).sum())
    if n_pos == 0 or n_neg == 0:
        return None
    order = np.argsort(scores, kind="mergesort")
    ranks = np.empty_like(order, dtype=float)
    ranks[order] = np.arange(1, len(scores) + 1)
    # Average ranks for ties.
    s_sorted = scores[order]
    i = 0
    while i < len(s_sorted):
        j = i
        while j + 1 < len(s_sorted) and s_sorted[j + 1] == s_sorted[i]:
            j += 1
        if j > i:
            avg = (ranks[order[i]] + ranks[order[j]]) / 2.0
            for kk in range(i, j + 1):
                ranks[order[kk]] = avg
        i = j + 1
    sum_pos = ranks[y == 1].sum()
    auc = (sum_pos - n_pos * (n_pos + 1) / 2.0) / (n_pos * n_neg)
    return float(auc)


# ── recomputation from residuals ────────────────────────────────────────

def recompute_observations(
    rows: Sequence[dict[str, Any]],
    task_type: str,
) -> list[dict[str, Any]]:
    """Recompute metric observations from residual rows, grouped by scope/fold/partition.

    ``rows`` are residual-store rows (``y_true``/``y_pred``/``y_proba`` + ``scope``/
    ``fold_id``/``partition``). Returns a long-format list of metric observations
    ready for the ``metric_observations`` table — the engine of score *recompute*.
    """
    groups: dict[tuple[str, str | None, str], list[dict[str, Any]]] = defaultdict(list)
    for r in rows:
        key = (r.get("scope") or "cv", r.get("fold_id"), r.get("partition") or "validation")
        groups[key].append(r)

    is_classification = task_type in {"binary", "multiclass", "multilabel"}
    observations: list[dict[str, Any]] = []
    for (scope, fold_id, partition), grp in sorted(groups.items(), key=lambda kv: (kv[0][0], str(kv[0][1]), kv[0][2])):
        # Pair row-by-row: a row missing either field is dropped as a whole, so
        # y_true/y_pred (and y_proba) stay index-aligned (the per-row None contract).
        pairs = [g for g in grp if g.get("y_true") is not None and g.get("y_pred") is not None]
        if not pairs:
            continue
        y_true = [g["y_true"] for g in pairs]
        y_pred = [g["y_pred"] for g in pairs]
        if is_classification:
            proba = [g["y_proba"] for g in pairs]
            metrics = compute_classification_metrics(
                y_true, y_pred, y_proba=proba if all(p is not None for p in proba) else None
            )
        else:
            metrics = compute_regression_metrics(y_true, y_pred)
        coverage = len(pairs) / len(grp) if grp else 0.0
        for name, value in metrics.items():
            observations.append(
                {
                    "metric_name": name,
                    "metric_value": value,
                    "direction": direction_of(name),
                    "scope": scope,
                    "fold_id": fold_id,
                    "partition": partition,
                    "aggregation_level": "sample",
                    "n_samples": len(pairs),
                    "coverage": coverage,
                }
            )
    return observations
