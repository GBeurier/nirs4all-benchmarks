"""Role-aware indexing — turn a pipeline DAG + run condition into queryable facets.

The dataviz needs to slice the benchmark space by *every* dimension a NIRS pipeline
varies: split, CV, seed, refit, dataset, model family, and — crucially — the
preprocessing chain and augmentation stages (DESIGN.md §3.1/§9.2). A single
``pipeline_dag_hash`` cannot answer "effect of SNV" or "augmentation on vs off", so
at ingest the Arena classifies each node into a stage *role* and materializes a long
``run_facets`` table: one row per (run_condition, facet_key, facet_value). Multi-
valued stages (several preprocessings) emit several rows; numeric facets carry a
``facet_num`` for ordered/continuous plots.
"""

from __future__ import annotations

from typing import Any

# Node-stage roles the Arena indexes. ``model``/``merge`` come straight from the
# canonical graph kind; the rest are inferred from the operator entrypoint.
ROLE_PREPROCESSING = "preprocessing"
ROLE_AUGMENTATION = "augmentation"
ROLE_SCALER = "scaler"
ROLE_FEATURE_SELECTION = "feature_selection"
ROLE_MODEL = "model"
ROLE_MERGE = "merge"
ROLE_SPLIT = "split"
ROLE_SAMPLER = "sampler"
ROLE_INPUT = "input"
ROLE_OTHER = "other"

# Substring heuristics over the lowercased operator entrypoint. Order matters:
# the first matching bucket wins, so augmentation/scaler are checked before the
# broad preprocessing bucket.
_KEYWORDS: list[tuple[str, tuple[str, ...]]] = [
    (ROLE_AUGMENTATION, ("augment", "augmentation", "mixup", "jitter", "warp", "randomshift",
                          "rotation", "addnoise", "gaussiannoise", "synthetic", "oversampl", "smote")),
    (ROLE_SCALER, ("standardscaler", "minmaxscaler", "robustscaler", "maxabsscaler", "normalizer",
                   "standardize", "minmax", "scaler")),
    (ROLE_FEATURE_SELECTION, ("selectkbest", "selectfrommodel", "rfe", "boruta", "vip", "cars",
                              "_spa", "geneticselect", "featureselect", "variancethreshold", "pca",
                              "kernelpca", "feature_select")),
    (ROLE_MERGE, ("stacking", "stackingregressor", "stackingclassifier", "voting", "votingregressor",
                  "bagging", "baggingregressor", "averaging", "weightedmean", "meanensemble",
                  "concat", "blend")),
    (ROLE_MODEL, ("plsregression", "plsr", "_pls", "pls(", "opls", "ikpls", "mbpls", "simpls",
                  "ridge", "lasso", "elasticnet", "linearregression", "logistic", "svr", "svc", "svm",
                  "randomforest", "extratrees", "gradientboost", "histgradientboost", "xgb", "lgbm",
                  "lightgbm", "catboost", "mlpregressor", "mlpclassifier", "cnn", "lstm", "gru",
                  "transformerencoder", "tabpfn", "gaussianprocess", "gpr", "kneighbors", "knn",
                  "decisiontree", "adaboost", "regressor", "classifier")),
    (ROLE_SAMPLER, ("kennard", "spxy", "sampling", "duplex", "sampler")),
    (ROLE_SPLIT, ("kfold", "shufflesplit", "groupkfold", "stratified", "leaveoneout", "traintest",
                  "split")),
    (ROLE_PREPROCESSING, ("snv", "msc", "emsc", "savgol", "savitzky", "detrend", "derivative",
                          "deriv", "baseline", "gapsegment", "smooth", "normalize", "osc", "scatter",
                          "resample", "crop", "trim", "haar", "wavelet", "continuum", "firstderiv",
                          "secondderiv", "logtransform", "preprocess", "transform", "filter")),
]

_KIND_TO_ROLE = {
    "input": ROLE_INPUT,
    "model": ROLE_MODEL,
    "merge": ROLE_MERGE,
    "mean": ROLE_MERGE,
    "average": ROLE_MERGE,
    "weighted_mean": ROLE_MERGE,
    "vote": ROLE_MERGE,
    "voting": ROLE_MERGE,
    "stacking": ROLE_MERGE,
    "bagging": ROLE_MERGE,
    "aggregator": ROLE_MERGE,
    "augmentation": ROLE_AUGMENTATION,
    "split": ROLE_SPLIT,
    "feature_selection": ROLE_FEATURE_SELECTION,
}


def classify_role(operator: str | None, declared_kind: str | None) -> str:
    """Classify a node into a stage role from its declared kind + operator path.

    Keyword matching runs over the **leaf** of the dotted entrypoint (the class name),
    not the full module path — so a module segment like ``sklearn.ensemble`` cannot
    masquerade a ``RandomForestRegressor`` as a merge node, and a ``PowerTransformer``
    stays preprocessing rather than being caught by a neural "transformer" needle.
    """
    kind = (declared_kind or "").strip().lower()
    if kind in _KIND_TO_ROLE:
        return _KIND_TO_ROLE[kind]
    op_full = (operator or "").lower()
    leaf = op_full.rsplit(".", 1)[-1]
    if not leaf:
        return ROLE_PREPROCESSING if kind in ("", "transform") else (kind or ROLE_OTHER)
    # Augmentation is usually signaled by the MODULE (e.g. nirs4all.augmentation.RandomShift,
    # whose class name carries no "aug" token), so check the full path for it first.
    if "augment" in op_full:
        return ROLE_AUGMENTATION
    # Everything else matches on the LEAF (class name) so module segments like
    # "sklearn.ensemble" cannot masquerade a model as a merge node.
    for role, needles in _KEYWORDS:
        if any(n in leaf for n in needles):
            return role
    # A declared "transform" with no keyword hit is preprocessing by default.
    if kind in ("transform", ""):
        return ROLE_PREPROCESSING
    return kind or ROLE_OTHER


def _short(operator: str | None) -> str | None:
    if not operator:
        return None
    return operator.split(".")[-1]


def build_run_facets(resolved: Any, model: Any) -> list[dict[str, Any]]:
    """Build deduped ``run_facets`` rows for one resolved run condition.

    ``resolved`` is a ``ResolvedExport``; ``model`` is the ``ArenaRunExport``. Returns
    rows ``{facet_key, facet_value, facet_num, role}`` (role only for stage facets).
    """
    rch = resolved.run_condition_hash
    # Deduped inline by (facet_key, facet_value) — a stage operator may repeat.
    rows_by_kv: dict[tuple[str, str], dict[str, Any]] = {}

    def add(key: str, value: Any, num: float | None = None, role: str | None = None) -> None:
        if value is None:
            return
        kv = (key, str(value))
        if kv in rows_by_kv:
            return
        rows_by_kv[kv] = {"run_condition_hash": rch, "facet_key": key, "facet_value": str(value),
                          "facet_num": num, "role": role}

    # ── condition-level facets ─────────────────────────────────────────
    add("dataset", resolved.dataset_fingerprint)
    add("task_type", model.task.task_type)
    add("split_method", model.split.method)
    add("cv_method", model.cv.method)
    if model.cv.n_folds is not None:
        add("n_folds", model.cv.n_folds, float(model.cv.n_folds))
    if model.rng.root_seed is not None:
        add("seed", model.rng.root_seed, float(model.rng.root_seed))
    add("refit_strategy", model.refit.strategy)
    add("pipeline", resolved.pipeline_dag_hash)
    add("is_linear", "linear" if resolved.pipeline_id.is_linear else "branching")

    # ── stage facets from the normalized nodes ─────────────────────────
    roles_count: dict[str, int] = {}
    model_ops: list[str] = []
    for node in resolved.node_rows:
        role = classify_role(node.get("operator"), node.get("role"))
        short = _short(node.get("operator"))
        if role == ROLE_INPUT or not short:
            continue
        roles_count[role] = roles_count.get(role, 0) + 1
        # one presence row per stage operator (enables "effect of SNV", etc.)
        add(f"{role}_op", short, None, role)
        add("operator", short, None, role)
        if role == ROLE_MODEL:
            model_ops.append(short)
        if role == ROLE_MERGE:
            add("merge_strategy", short, None, role)
        # per-parameter facets (numeric params carry facet_num for sweeps)
        for pname, pval in _safe_params(node).items():
            num = float(pval) if isinstance(pval, (int, float)) and not isinstance(pval, bool) else None
            add(f"param:{pname}", pval, num)

    add("model", resolved.main_model and _short(resolved.main_model))
    add("model_family", resolved.main_model and _short(resolved.main_model))
    add("n_models", len(model_ops), float(len(model_ops)))
    add("n_preprocessing", roles_count.get(ROLE_PREPROCESSING, 0), float(roles_count.get(ROLE_PREPROCESSING, 0)))
    add("n_augmentation", roles_count.get(ROLE_AUGMENTATION, 0), float(roles_count.get(ROLE_AUGMENTATION, 0)))
    add("has_augmentation", "yes" if roles_count.get(ROLE_AUGMENTATION) else "no")
    add("has_scaler", "yes" if roles_count.get(ROLE_SCALER) else "no")
    add("has_feature_selection", "yes" if roles_count.get(ROLE_FEATURE_SELECTION) else "no")
    add("n_stages", sum(roles_count.values()), float(sum(roles_count.values())))

    return list(rows_by_kv.values())


def _safe_params(node: dict[str, Any]) -> dict[str, Any]:
    import json

    try:
        data = json.loads(node.get("params_json") or "{}")
        return data if isinstance(data, dict) else {}
    except (json.JSONDecodeError, TypeError):
        return {}
