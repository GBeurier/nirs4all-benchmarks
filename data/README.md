# Measured catalog benchmark data

`catalog-history-2026-09/` contains 10,824 existing research executions on **104 datasets** from
`nirs4all-datasets`: 153 dataset/source/target/task-kind combinations, 32 method labels, seeds 0/1/2,
and 32,286 metric observations. There are 6,666 regression runs and 4,158 classification runs.
Source-specific views occupy 110 dataset fingerprints; these are not 110 independent datasets.

Methods include PLS, PLS-DA, Ridge, AOM-PLS, AOM-Ridge, NIRPFN, and TabPFN v2.5/v3/v3.5,
with raw, GCU, LVSE, and combined representations. They are already measured results; importing
does not run training, download datasets, or require the private lab checkout.

## Import and inspect

```bash
.venv/bin/n4a-benchmarks ingest-history data/catalog-history-2026-09 --store ./arena-store
.venv/bin/n4a-benchmarks stats --store ./arena-store
.venv/bin/n4a-benchmarks leaderboard --store ./arena-store --metric rmse --scope test
.venv/bin/n4a-benchmarks serve --store ./arena-store
# Or build the existing charts as a static site:
.venv/bin/n4a-benchmarks build-site --store ./arena-store --out ./dist/site
```

Import verifies the archive SHA-256 and execution count and uses the ordinary ArenaRunExport v1
ingestion gate. Repeated imports are idempotent. The Pages workflow reconstructs a fresh store from
this archive, so the published data no longer depend on an untracked local SQLite database.

## Selection and provenance

The source tables are `nirs4all-lab/RamanPFN/results/score_matrix.csv` (63,508 successful runs) and
`catalog_tabpfn_final_benchmark_export.csv` (4,584 final version-comparison rows). The latter updates
matching experiment keys from the former; their row counts must not be added as independent runs.

To keep the first static snapshot bounded while covering every available dataset, retain the first
target in lexical order **per dataset, spectral source, and task kind**, then all available methods
and seeds for that target. Selection never uses metric values. The exact selected tasks, per-method
counts, source table hashes, and producer protocol code hashes are in `manifest.json`.

Datasets named in the manifest's `excluded_datasets` (and `EXCLUDED_DATASETS` in
`scripts/collect_history.py`) are left out, each with its reason. The EcoSIS 3D canopy reflectance
dataset is excluded because its lexically first target is a UTM easting coordinate, not a measured
property; its RMSE values (4e3 to 8e5) dominated every cross-dataset aggregate. The two malaria
*Anopheles gambiae* datasets are excluded because their target is a raw, heavily skewed parasite count
(sporozoites 0 to 2.4e5, oocysts 0 to 6.1e4); their unnormalized RMSE (4e3 to 4e4) dominated those
aggregates as well. They were excluded before the scale-free nRMSE below was added and remain excluded;
pool scores across datasets on nRMSE, since the native RMSE of different targets is not comparable.

`runs.jsonl.gz` contains one standard ArenaRunExport v1 JSON object per line. Every export records
its source CSV path relative to the workspace, SHA-256, original CSV line number (header is line 1),
experiment key, and producer exporter hash in `provenance.historical_source`. Dataset cards are
enriched from the current catalog, with its stable dataset/source/target IDs and descriptor SHA-256.
Every source and target was checked against that descriptor during harvesting. Spectral source
views and historical sample filtering remain explicit in the export. A descriptor hash establishes
metadata provenance; it does not assert that historical dataset bytes match today's canonical bytes.

The producer's existing group-aware 80/20 hold-out and train-only selection attestations are
preserved. The split seed, sample counts, method details, selected parameters, and pipeline graph
are retained. These are **producer-reported historical scores**, imported as a user run collection,
not a newly qualified release. Producer graphs reflect the recorded recipe; they are not certified
complete replay bundles. Producer metrics were copied without rescaling or recomputation; the only
derived score is nRMSE (below).

All observations remain `scope=test`, `partition=test`. Inner-CV method selection does not make a
held-out score a CV observation. Use the **test** lens in the existing charts. No fold scores,
sample residuals, spectra, target vectors, or fitted weights are invented or included. Residual
comparison views therefore have no data for this collection. Raw-data access tiers are retained;
importing scores does not grant access to the underlying datasets. Compare scores within matching
dataset/source/target, metric, and protocol: absolute RMSE values across different targets have
different units and scales. Method coverage is incomplete on some tasks.

### Derived nRMSE

Every regression export whose target has a usable scale carries an extra `nrmse` observation:
RMSE divided by the standard deviation of the target over the whole catalog dataset, taken from
`nirs4all-datasets/datasets/<id>/card.json` and recorded with that card's SHA-256 in
`dataset_card.target_scale` (6,480 of 6,666 regression runs). nRMSE is scale-free, so it is the
default lens for charts that pool datasets. It equals 1/RPD up to the choice of standard deviation:
the whole catalog dataset, not each held-out test split. Two targets get none because their card
statistics count the undeclared `-9999` missing-value sentinel as a value (`EWT_gDW_cm2` in the
NASA FFT leaf dataset and `CN_ratio` in the NGEE Arctic leaf dataset); their RMSE remains available.

## Refresh from the original local sources

Only harvesting requires the lab environment and private producer checkout. It reads source files
without changing them; it launches no experiments:

```bash
../nirs4all-lab/.venv/bin/python scripts/collect_history.py --workspace ..
```

The gzip stream is deterministic (fixed timestamp and sorted export order). Review changes to
`manifest.json` and the data-pack tests before importing a refreshed archive. Other lab/draft
tables were inspected but are outside this first pack because the selected campaign already uses
catalog IDs directly and records an explicit evaluation protocol.

The scored results follow this repository's CC-BY-4.0 content terms; dataset origins and their own
licenses remain separate and are retained in the cards.
