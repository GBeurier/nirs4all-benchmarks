"""Synthetic ``ArenaRunExport`` fixtures with realistic, structured residuals."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import numpy as np

from nirs4all_benchmarks.contract import ArenaRunExport
from nirs4all_benchmarks.datasets import card_to_dataset_block, mock_dataset_card
from nirs4all_benchmarks.identity import fingerprint
from nirs4all_benchmarks.identity.hashing import export_hash as compute_export_hash

# Two reference mock datasets (DESIGN.md Phase 0 I1).
_DATASETS: list[dict[str, Any]] = [
    {"id": "mock_corn", "n": 120, "feat": 700, "axis": (1100.0, 2500.0), "domain": "corn", "mu": 12.0, "sigma": 3.5},
    {"id": "mock_wheat", "n": 90, "feat": 256, "axis": (4000.0, 10000.0), "domain": "wheat", "mu": 11.2, "sigma": 2.1},
]


def _linear_graph(steps: list[dict[str, Any]]) -> dict[str, Any]:
    nodes = []
    edges = []
    for i, step in enumerate(steps):
        nid = f"n{i}"
        nodes.append({"node_id": nid, **step})
        if i > 0:
            edges.append({"src": f"n{i-1}", "dst": nid})
    return {"nodes": nodes, "edges": edges}


def _pls(k: int) -> dict[str, Any]:
    return {"kind": "model", "operator": "sklearn.cross_decomposition.PLSRegression", "params": {"n_components": k}}


def _branch_merge_graph(k: int) -> dict[str, Any]:
    """Two preprocessing branches into a mean merge of two PLS models."""
    return {
        "nodes": [
            {"node_id": "in", "kind": "input", "operator": "X"},
            {"node_id": "snv", "kind": "transform", "operator": "nirs4all.transform.SNV"},
            {"node_id": "sg", "kind": "transform", "operator": "nirs4all.transform.SavitzkyGolay",
             "params": {"window": 11, "polyorder": 2}},
            {"node_id": "pls_a", **_pls(k)},
            {"node_id": "pls_b", **_pls(k)},
            {"node_id": "merge", "kind": "mean", "operator": "nirs4all.merge.MeanEnsemble"},
        ],
        "edges": [
            {"src": "in", "dst": "snv"},
            {"src": "in", "dst": "sg"},
            {"src": "snv", "dst": "pls_a"},
            {"src": "sg", "dst": "pls_b"},
            {"src": "pls_a", "dst": "merge"},
            {"src": "pls_b", "dst": "merge"},
        ],
    }


def _stacking_graph(k: int) -> dict[str, Any]:
    return {
        "nodes": [
            {"node_id": "in", "kind": "input", "operator": "X"},
            {"node_id": "snv", "kind": "transform", "operator": "nirs4all.transform.SNV"},
            {"node_id": "pls", **_pls(k)},
            {"node_id": "rf", "kind": "model", "operator": "sklearn.ensemble.RandomForestRegressor",
             "params": {"n_estimators": 200}},
            {"node_id": "stack", "kind": "stacking", "operator": "sklearn.linear_model.Ridge"},
        ],
        "edges": [
            {"src": "in", "dst": "snv"},
            {"src": "snv", "dst": "pls"},
            {"src": "snv", "dst": "rf"},
            {"src": "pls", "dst": "stack"},
            {"src": "rf", "dst": "stack"},
        ],
    }


def _pls_sweep_graph(k: int) -> dict[str, Any]:
    return _linear_graph(
        [
            {"kind": "transform", "operator": "sklearn.preprocessing.StandardScaler"},
            {"kind": "transform", "operator": "nirs4all.transform.SNV"},
            _pls(k),
        ]
    )


def _pls_skill(k: int, base: float) -> float:
    """U-shaped RMSE as a function of PLS n_components (underfit/overfit)."""
    return base * (1.0 + 0.5 * abs(k - 12) / 12.0) + 0.05 * max(0, k - 18)


def _synth_residuals(
    seed: int,
    sample_ids: list[str],
    y_true: np.ndarray,
    rmse: float,
    n_folds: int,
) -> list[dict[str, Any]]:
    rng = np.random.default_rng(seed)
    n = len(sample_ids)
    fold_assign = rng.permutation(n) % n_folds
    rows: list[dict[str, Any]] = []
    # CV: every sample validated exactly once (OOF).
    noise = rng.normal(0.0, rmse, size=n)
    y_pred = y_true + noise
    for i, sid in enumerate(sample_ids):
        rows.append(
            {
                "sample_id": sid,
                "group_id": None,
                "scope": "cv",
                "fold_id": f"fold{int(fold_assign[i])}",
                "partition": "validation",
                "y_true": float(y_true[i]),
                "y_pred": float(y_pred[i]),
                "residual": float(y_true[i] - y_pred[i]),
                "weight": 1.0,
            }
        )
    # External test: a held-out 25% with slightly worse error.
    n_test = max(8, n // 4)
    test_idx = rng.choice(n, size=n_test, replace=False)
    test_noise = rng.normal(0.0, rmse * 1.1, size=n_test)
    for j, i in enumerate(test_idx):
        rows.append(
            {
                "sample_id": f"{sample_ids[i]}_test",
                "scope": "test",
                "fold_id": None,
                "partition": "test",
                "y_true": float(y_true[i]),
                "y_pred": float(y_true[i] + test_noise[j]),
                "residual": float(-test_noise[j]),
                "weight": 1.0,
            }
        )
    return rows


def _make_export(
    *,
    dataset: dict[str, Any],
    graph: dict[str, Any],
    label: str,
    rmse: float,
    cv_folds: int,
    seed: int,
    main_model: str,
) -> ArenaRunExport:
    card = mock_dataset_card(
        dataset["id"],
        n_samples=dataset["n"],
        n_features=dataset["feat"],
        axis_range=dataset["axis"],
        domain=dataset["domain"],
    )
    dataset_block = card_to_dataset_block(card)
    rng = np.random.default_rng(seed)
    sample_ids = [f"{dataset['id']}_{i:04d}" for i in range(dataset["n"])]
    y_true = rng.normal(dataset["mu"], dataset["sigma"], size=dataset["n"])
    residuals = _synth_residuals(seed, sample_ids, y_true, rmse, cv_folds)

    manifest: dict[str, Any] = {
        "arena_export_schema_version": 1,
        "producer": {"capsule": "fixture", "nirs4all_version": "0.10.0", "dag_ml_version": "0.1.0-alpha"},
        "dataset": dataset_block,
        "task": {"task_hash": fingerprint({"d": dataset["id"], "t": "regression"}), "task_type": "regression",
                 "target_name": "reference", "target_unit": "g/100g"},
        "dataset_variant": {"dataset_variant_hash": fingerprint({"d": dataset["id"], "v": "all"}),
                            "variant_spec": {"size": "all", "aggregation": "none"}},
        "pipeline": {"graph": graph, "human_label": label, "nodes": graph["nodes"]},
        "split": {"method": "kennard_stone", "params": {"test_size": 0.25}},
        "cv": {"method": "kfold", "n_folds": cv_folds, "within_train_only": True,
               "cv_instance_hash": fingerprint({"d": dataset["id"], "folds": cv_folds, "seed": seed})},
        "rng": {"root_seed": seed, "derivation": "sha256-hash-chain",
                "framework_seeds": {"numpy": seed, "sklearn": seed},
                "determinism_flags": {"PYTHONHASHSEED": 0}},
        "refit": {"strategy": "global_best_params_full_train", "selection_scope": "oof", "train_scope": "full_train"},
        "execution": {"execution_id": f"exec-{label}-{dataset['id']}", "status": "ok",
                      "time_ms": float(rng.integers(500, 5000)), "peak_mem_mb": float(rng.integers(200, 900)),
                      "os": "linux", "hardware": "cpu-x86_64"},
        "leakage_attestation": {"oof_enforced": True, "group_leakage_checked": True, "nested_cv_safe": True,
                                "unsafe_flags": []},
        "scores": {"score_version": "1.0", "observations": []},
        "residuals": {"key": "sample_id", "pseudonymized": False,
                      "publishable": {"y_true": True, "y_pred": True, "residual": True},
                      "inline": residuals},
    }
    manifest["pipeline"]["_main_model"] = main_model
    manifest["arena_export_hash"] = compute_export_hash(manifest)
    return ArenaRunExport.model_validate(manifest)


def generate_fixture_exports() -> list[ArenaRunExport]:
    """The full fixture grid: 2 datasets × {PLS sweep, branch/merge, stacking}."""
    exports: list[ArenaRunExport] = []
    seed = 1000
    for dataset in _DATASETS:
        base = dataset["sigma"] * 0.30
        # PLS n_components sweep (the parameter-effect showcase).
        for k in (5, 8, 12, 16, 20):
            seed += 1
            exports.append(
                _make_export(
                    dataset=dataset,
                    graph=_pls_sweep_graph(k),
                    label=f"StdScaler→SNV→PLS(k={k})",
                    rmse=_pls_skill(k, base),
                    cv_folds=5,
                    seed=seed,
                    main_model="sklearn.cross_decomposition.PLSRegression",
                )
            )
        # Branch/merge (beats single branch).
        seed += 1
        exports.append(
            _make_export(
                dataset=dataset,
                graph=_branch_merge_graph(12),
                label="[SNV→PLS | SG→PLS] → mean",
                rmse=_pls_skill(12, base) * 0.88,
                cv_folds=5,
                seed=seed,
                main_model="nirs4all.merge.MeanEnsemble",
            )
        )
        # Stacking.
        seed += 1
        exports.append(
            _make_export(
                dataset=dataset,
                graph=_stacking_graph(12),
                label="stack(SNV→PLS, SNV→RF)",
                rmse=_pls_skill(12, base) * 0.83,
                cv_folds=5,
                seed=seed,
                main_model="sklearn.linear_model.Ridge",
            )
        )
    return exports


def write_fixture_exports(out_dir: str | Path) -> list[Path]:
    """Write each fixture export to ``out_dir`` as canonical JSON; return the paths."""
    out = Path(out_dir)
    out.mkdir(parents=True, exist_ok=True)
    paths: list[Path] = []
    for exp in generate_fixture_exports():
        manifest = exp.to_manifest()
        path = out / f"arena_run_{manifest['arena_export_hash']}.json"
        path.write_text(json.dumps(manifest, indent=2, sort_keys=True), encoding="utf-8")
        paths.append(path)
    return paths


def seed_store(store_root: str | Path, *, collection_id: str = "fixtures") -> dict[str, Any]:
    """Ingest the full fixture grid into a fresh store; return a summary."""
    from nirs4all_benchmarks.ingestion import IngestionPolicy, ingest_export
    from nirs4all_benchmarks.store import ArenaStore

    counts = {"committed": 0, "already": 0, "quarantined": 0, "rejected": 0}
    run_conditions: set[str] = set()
    with ArenaStore(store_root) as store:
        policy = IngestionPolicy(collection_id=collection_id, collection_kind="benchmark_release")
        for exp in generate_fixture_exports():
            res = ingest_export(store, exp, policy=policy)
            key = {"committed": "committed", "already_ingested": "already",
                   "quarantined": "quarantined"}.get(res.status, "rejected")
            counts[key] += 1
            if res.run_condition_hash:
                run_conditions.add(res.run_condition_hash)
    return {**counts, "run_conditions": len(run_conditions)}
