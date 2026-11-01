"""Freeze a balanced, traceable subset of existing catalog-backed research runs.

Run with the lab's Python environment; only this harvesting step needs pandas
and the private producer checkout. The frozen pack imports without either.
"""

from __future__ import annotations

import argparse
import csv
import gzip
import hashlib
import importlib.util
import json
import math
import sys
from collections import Counter, defaultdict
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

# Datasets left out of the pack; the reasons are recorded in the manifest.
EXCLUDED_DATASETS = {
    "ecosis_3d_canopy_level_spectra_reflectance_nirs": (
        "The selected target is the UTM easting coordinate (metres), not a measured property; "
        "its RMSE of 4e3-8e5 dominated every cross-dataset aggregate."
    ),
    "malaria_anopheles_gambiae_sporozoite_nir": (
        "The target is a raw, heavily skewed sporozoite count (0 to 2.4e5); "
        "its unnormalized RMSE of 1e4-4e4 dominated every cross-dataset aggregate."
    ),
    "malaria_anopheles_gambiae_oocyst_nir": (
        "The target is a raw, heavily skewed oocyst count (0 to 6.1e4); "
        "its unnormalized RMSE of 4e3-1e4 dominated every cross-dataset aggregate."
    ),
}


# Derived scores added to every regression export; recorded in the manifest.
DERIVED_METRICS = {
    "nrmse": "RMSE divided by the standard deviation of the target over the whole catalog dataset "
             "(nirs4all-datasets card statistics, recorded in dataset_card.target_scale), so scores "
             "of targets with different units and scales are comparable. Targets whose card statistics "
             "include the undeclared -9999 missing-value sentinel have no reliable scale and get no nRMSE.",
}
MISSING_SENTINEL = -9999.0


def sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def target_scale(datasets_root: Path, dataset_id: str, target: str) -> dict | None:
    """Standard deviation of ``target`` from its nirs4all-datasets card, with provenance.

    None when the card statistics count the -9999 missing-value sentinel as a value.
    """
    path = datasets_root / dataset_id / "card.json"
    card = json.loads(path.read_text(encoding="utf-8"))
    stats = next(v for v in card["variables"] if v["name"] == target)["stats"]
    if stats["min"] == MISSING_SENTINEL:
        return None
    return {"target": target, "sd": stats["std"], "n": stats["n"],
            "card_path": f"nirs4all-datasets/datasets/{dataset_id}/card.json", "card_sha256": sha256(path)}


def add_nrmse(manifest: dict, scale: dict) -> None:
    """Record the target scale and add an nRMSE observation next to each RMSE observation."""
    manifest["dataset"]["dataset_card"]["target_scale"] = scale
    observations = manifest["scores"]["observations"]
    for observation in [o for o in observations if o["metric_name"] == "rmse"]:
        observations.append({**observation, "metric_name": "nrmse",
                             "metric_value": observation["metric_value"] / scale["sd"]})


def collect(workspace: Path, output: Path) -> dict:
    """Read producer CSVs and freeze ordinary ArenaRunExport v1 manifests."""
    import pandas as pd
    import yaml

    from nirs4all_benchmarks.datasets import card_to_dataset_block, load_catalog_card

    producer = workspace / "nirs4all-lab" / "RamanPFN"
    exporter_path = producer / "export_benchmarks.py"
    spec = importlib.util.spec_from_file_location("historical_producer_exporter", exporter_path)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"Cannot load producer exporter: {exporter_path}")
    exporter = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(exporter)
    exporter.DATASETS = workspace / "nirs4all-datasets"
    source_names = ["score_matrix.csv", "catalog_tabpfn_final_benchmark_export.csv"]
    sources = {}
    rows = {}
    for filename in source_names:
        path = producer / "results" / filename
        relative = path.relative_to(workspace).as_posix()
        sources[relative] = {"sha256": sha256(path), "rows": 0}
        with path.open(encoding="utf-8", newline="") as stream:
            for line, row in enumerate(csv.DictReader(stream), 2):
                sources[relative]["rows"] += 1
                if row["status"] != "ok" or row["dataset"] in EXCLUDED_DATASETS:
                    continue
                key = tuple(row[k] for k in ("dataset", "source", "target", "kind", "seed", "method", "origin"))
                # A later final export takes precedence over its earlier aggregate copy.
                rows[key] = (row, relative, line)

    targets = defaultdict(set)
    for row, _, _ in rows.values():
        targets[row["dataset"], row["source"], row["kind"]].add(row["target"])
    selected = {(*key, sorted(names)[0]) for key, names in targets.items()}
    dataset_blocks = {}
    descriptors = {}
    exports = []
    selection = []
    for key, (row, source, line) in sorted(rows.items()):
        if (row["dataset"], row["source"], row["kind"], row["target"]) not in selected:
            continue
        dataset_id = row["dataset"]
        if dataset_id not in descriptors:
            path = workspace / "nirs4all-datasets" / "catalog" / "datasets" / f"{dataset_id}.yaml"
            descriptor = yaml.safe_load(path.read_text(encoding="utf-8"))
            descriptors[dataset_id] = descriptor
            card = load_catalog_card(dataset_id, path.parent)
            block = card_to_dataset_block(card)
            block["dataset_card"].update({
                "dataset_id": dataset_id, "dataset_version": card.dataset_version,
                "source_registry": "nirs4all-datasets", "descriptor_sha256": sha256(path),
                "origin_sources": descriptor.get("origin_sources", []),
            })
            # Catalog metadata are public; retain the raw-data access tier explicitly.
            block["visibility"] = "public" if descriptor["tier"] == "public" else "restricted"
            dataset_blocks[dataset_id] = block
        descriptor = descriptors[dataset_id]
        if row["source"] not in {s["source_id"] for s in descriptor["sources"]}:
            raise ValueError(f"Unknown catalog source: {key}")
        if row["target"] not in {v["name"] for v in descriptor["variables"] if v["role"] == "target"}:
            raise ValueError(f"Unknown catalog target: {key}")
        numeric = dict(row)
        for column in ("n_train", "n_test", "n_features", "seed"):
            numeric[column] = int(float(row[column]))
        for column in exporter.METRICS:
            numeric[column] = float(row[column]) if row[column] else float("nan")
        manifest = exporter.manifest_for(pd.Series(numeric)).to_manifest()
        # Preserve producer identity; enrich its card with the actual catalog metadata.
        dataset = manifest["dataset"]
        enriched = json.loads(json.dumps(dataset_blocks[dataset_id]))
        enriched["dataset_fingerprint"] = dataset["dataset_fingerprint"]
        enriched["n_samples"] = numeric["n_train"] + numeric["n_test"]
        enriched["n_features"] = numeric["n_features"]
        enriched["dataset_card"]["source_id"] = row["source"]
        enriched["dataset_card"]["task_type"] = manifest["task"]["task_type"]
        source_spec = next(s for s in descriptor["sources"] if s["source_id"] == row["source"])
        enriched["dataset_card"]["modality"] = source_spec["modality"]
        enriched["dataset_card"]["signal_type"] = source_spec.get("signal_type")
        enriched["dataset_card"]["axis"] = {
            "unit": source_spec.get("axis_unit"), "n": numeric["n_features"],
            "range": [source_spec.get("axis_min"), source_spec.get("axis_max")],
            "resolution": source_spec.get("axis_resolution"),
        }
        manifest["dataset"] = enriched
        manifest["provenance"] = {"historical_source": {
            "path": source, "sha256": sources[source]["sha256"], "csv_line": line,
            "origin": row["origin"], "experiment_key": list(key),
            "exporter_path": exporter_path.relative_to(workspace).as_posix(),
            "exporter_sha256": sha256(exporter_path),
        }}
        scale = target_scale(workspace / "nirs4all-datasets" / "datasets", dataset_id, row["target"])
        if manifest["task"]["task_type"] == "regression" and scale is not None:
            add_nrmse(manifest, scale)
        if not manifest["scores"]["observations"]:
            raise ValueError(f"Missing metrics: {key}")
        for observation in manifest["scores"]["observations"]:
            if not math.isfinite(observation["metric_value"]):
                raise ValueError(f"Non-finite metric: {key}")
        exports.append(manifest)
    output.mkdir(parents=True, exist_ok=True)
    archive = output / "runs.jsonl.gz"
    with archive.open("wb") as raw, gzip.GzipFile(filename="", mode="wb", fileobj=raw, mtime=0) as compressed:
        for manifest in exports:
            payload = json.dumps(manifest, sort_keys=True, separators=(",", ":"), allow_nan=False) + "\n"
            compressed.write(payload.encode())
    for dataset_id, source_id, kind, target in sorted(selected):
        selection.append({"dataset_id": dataset_id, "source_id": source_id, "kind": kind, "target": target})
    evidence = ["data.py", "run.py", "run_classical_reps.py", "run_aom.py", "run_aom_defaults.py",
                "run_versions.py", "run_version_reps.py", "build_score_matrix.py", "export_benchmarks.py"]
    metadata = {
        "schema_version": 1, "collection_id": "catalog_history_2026_09",
        "name": "Catalog research runs (September 2026)",
        "description": "Measured held-out test scores from existing NIRPFN, TabPFN, PLS and AOM research runs. "
                       "Producer-reported scores; no raw spectra, targets, residuals or model weights included.",
        "archive_sha256": sha256(archive), "executions": len(exports),
        "datasets": len(descriptors), "tasks": len(selected),
        "metric_observations": sum(len(m["scores"]["observations"]) for m in exports),
        "by_method": dict(sorted(Counter(m["pipeline"]["human_label"] for m in exports).items())),
        "by_task_type": dict(Counter(m["task"]["task_type"] for m in exports)),
        "seeds": sorted({m["rng"]["root_seed"] for m in exports}),
        "selection_policy": "First target in lexical order per dataset/source/task kind; all available methods "
                            "and seeds. No selection by score. Final export overrides duplicate experiment keys.",
        "excluded_datasets": EXCLUDED_DATASETS,
        "derived_metrics": DERIVED_METRICS,
        "sources": sources, "selection": selection,
        "protocol_evidence": {f"nirs4all-lab/RamanPFN/{name}": sha256(producer / name) for name in evidence},
    }
    (output / "manifest.json").write_text(json.dumps(metadata, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")
    return {k: metadata[k] for k in ("executions", "datasets", "tasks", "metric_observations")}


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--workspace", type=Path, default=ROOT.parent)
    parser.add_argument("--out", type=Path, default=ROOT / "data" / "catalog-history-2026-09")
    args = parser.parse_args()
    print(json.dumps(collect(args.workspace.resolve(), args.out.resolve()), indent=2))
