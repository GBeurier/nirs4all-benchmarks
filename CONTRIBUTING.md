# Contributing to nirs4all-benchmarks

Thanks for helping build the Arena. This document is the developer setup and the **green gate** that
every change must pass.

## Setup

```bash
uv venv --python 3.11 .venv
uv pip install --python .venv -e ".[service,dev]"
```

The package is `nirs4all_benchmarks` (src layout). Optional extras:

- `service` — FastAPI + uvicorn (the web app).
- `datasets` — PyYAML + `nirs4all-datasets` (the catalog loader).
- `nirs4all` / `dagml` — the producer libraries (only needed for live, non-degraded ingestion).

The Arena is **standalone by contract**: nothing in `src/` imports a sibling library at module load.
New producer integrations must keep that property (import lazily, degrade gracefully).

## Green gate

Run all three before opening a PR — CI runs the same on Python 3.10/3.11/3.12:

```bash
.venv/bin/ruff check src tests      # lint
.venv/bin/mypy                      # types
.venv/bin/pytest -q                 # tests (currently 58, all green)
```

Frontend (no build step) — syntax-check the SPA modules with any Node ≥ 18:

```bash
for f in src/nirs4all_benchmarks/web/app.js \
         src/nirs4all_benchmarks/web/lib/*.js \
         src/nirs4all_benchmarks/web/views/*.js; do
  cp "$f" "${f%.js}.__c.mjs" && node --check "${f%.js}.__c.mjs" && rm "${f%.js}.__c.mjs"
done
```

## Conventions

- **No artifacts, ever.** The Arena stores recipes, scores, residuals, identity cards — never fitted
  models or weights. Adapters strip weights on read.
- **Content-addressed identity.** Add a new dimension only with a recompute-able content hash; never
  key on a producer UUID. If you add a hash, document which engine fingerprint it derives from.
- **Frozen contracts are frozen.** Changing `ArenaRunExport`, the store schema, or the residuals
  Parquet shape means bumping the relevant version constant in `nirs4all_benchmarks/__init__.py` and
  re-freezing `tests/test_fixtures_regression.py`.
- **Scores are versioned facts.** Never mutate a stored score; recomputation adds a superseding
  `ScoreSet`.
- Python: Google-style docstrings, type hints on public APIs, `ruff` (line length 120), `mypy`.
- Frontend: vanilla ES modules, reuse `lib/dom.js` / `lib/plot.js` / `lib/api.js` and the existing
  CSS classes; charts go through `plot.draw`. No new runtime dependencies, no build step.

## Adding a dataviz view

1. Add `src/nirs4all_benchmarks/web/views/<id>.js` default-exporting
   `{ id, title, subtitle, icon, render(ctx) }` (see `views/leaderboard.js` as the reference).
2. Register it in `web/app.js` (`VIEWS` + a `GROUPS` entry).
3. If it needs new data, add a `Queries` method + a `/api/...` route + an `api.js` client method.

## Tests

- Unit tests per module; the FastAPI service is tested via `TestClient`.
- `tests/test_fixtures_regression.py` freezes fixture identity hashes — update its golden values
  intentionally (with a version bump) if canonicalization legitimately changes.
