# Deploying the Arena

Run nirs4all-benchmarks ("the Arena") as a persistent online service: the FastAPI
app that serves the read APIs plus the no-build Plotly SPA, backed by an on-disk
store.

## Contents

- [What you are deploying](#what-you-are-deploying)
- [Install](#install)
- [Create and seed a store](#create-and-seed-a-store)
- [Run the service](#run-the-service)
- [Store location: NIRS4ALL_BENCHMARKS_STORE](#store-location-nirs4all_benchmarks_store)
- [Docker](#docker)
- [docker-compose with a persistent volume](#docker-compose-with-a-persistent-volume)
- [Reverse proxy](#reverse-proxy)
- [Backup and restore](#backup-and-restore)
- [Operational notes](#operational-notes)

## What you are deploying

The service is a FastAPI application factory, `nirs4all_benchmarks.service.app:create_app`.
It exposes:

- JSON read APIs under `/api/*` (overview, collections, datasets, pipelines,
  operators, parameters, leaderboard, matrix, runs, operator/parameter effect,
  robustness, run detail, residuals, compare) and a health probe at `/api/healthz`.
- A single write endpoint, `POST /api/ingest`, that accepts one `ArenaRunExport`
  manifest.
- The static SPA, mounted at `/` from `src/nirs4all_benchmarks/web/` (served only
  when that directory is present in the installed package; it ships in the wheel).

All of these read from one store directory. The store is plain files on disk —
there is no external database to provision.

## Install

The base package does **not** pull in the web stack. The service needs the
`[service]` extra (FastAPI + Uvicorn).

With `uv`:

```bash
uv pip install 'nirs4all-benchmarks[service]'
```

With `pip`:

```bash
pip install 'nirs4all-benchmarks[service]'
```

This provides the `n4a-benchmarks` and `n4a-arena` console scripts (identical
entry points) and makes `create_app` importable. If you call `create_app` without
the extra installed it raises `RuntimeError` telling you to install
`nirs4all-benchmarks[service]`.

Extras you may also want, depending on what you ingest:

| Extra | Pulls in | When |
|---|---|---|
| `service` | `fastapi`, `uvicorn[standard]` | always, to run the web service |
| `nirs4all` | `nirs4all>=0.10` | ingesting live from a nirs4all workspace |
| `dagml` | `dag-ml-py>=0.1` | ingesting a dag-ml `ExecutionBundle` |
| `datasets` | `nirs4all-datasets`, `pyyaml` | dataset-card enrichment |
| `all` | `[service,datasets]` | both of the above |

Requires Python 3.10+.

## Create and seed a store

The store is a directory. Create an empty one:

```bash
n4a-benchmarks init --store ./arena-store
```

This creates the layout and stamps the schema version:

```
arena-store/
  arena.sqlite      # metadata: collections, datasets, pipelines, scores, ...
  arrays/           # sample-keyed residual Parquet (Zstd), one file per residual_set
  exports/          # ingested ArenaRunExport bundles (audit / replay)
```

`--store` / `-s` defaults to `./arena-store` on every command, so it can be
omitted when you use that path.

To bring up a service with something to look at, seed the synthetic fixture grid
(2 datasets × {PLS n_components sweep, branch/merge, stacking}):

```bash
n4a-benchmarks fixtures --store ./arena-store
```

For real data, ingest one or more `ArenaRunExport` manifests (a single file or a
directory of `*.json`):

```bash
n4a-benchmarks ingest-export ./exports/ --store ./arena-store
# as a scored benchmark release (quarantines on leakage):
n4a-benchmarks ingest-export ./exports/ --store ./arena-store --release
```

Other ingestion entry points: `ingest-workspace` (a nirs4all workspace),
`ingest-bundle` (a dag-ml `ExecutionBundle`). Confirm what landed:

```bash
n4a-benchmarks stats --store ./arena-store
n4a-benchmarks leaderboard --store ./arena-store --metric rmse --scope cv
```

You can also ingest over HTTP once the service is up, by POSTing a manifest to
`/api/ingest` (see the API). Ingestion writes to the same store the service
reads, so a running service picks up new rows on the next query.

## Run the service

The bundled launcher resolves the store path to an absolute path, exports it as
`NIRS4ALL_BENCHMARKS_STORE`, and starts Uvicorn against the factory:

```bash
n4a-benchmarks serve --store ./arena-store --host 0.0.0.0 --port 8000
```

Flags: `--store`/`-s` (default `./arena-store`), `--host` (default `127.0.0.1`),
`--port`/`-p` (default `8000`), `--reload` (dev autoreload). Bind `0.0.0.0` to
accept connections from outside the container/host.

`serve` runs a single Uvicorn worker. For production use Uvicorn (or Gunicorn
with Uvicorn workers) directly against the factory and point it at the store with
the environment variable:

```bash
export NIRS4ALL_BENCHMARKS_STORE=/data/arena-store
uvicorn --factory nirs4all_benchmarks.service.app:create_app \
        --host 0.0.0.0 --port 8000 --workers 4
```

`--factory` is required: `create_app` is a callable that builds and returns the
ASGI app, not a module-level app object. Each worker opens its own SQLite
connection per request (the app opens and closes an `ArenaStore` around each
query), so multiple workers are safe against a shared store on a local
filesystem.

Verify it is up:

```bash
curl -s http://localhost:8000/api/healthz
# {"status":"ok","version":"0.1.0","store":"/data/arena-store","store_exists":true}
```

`store_exists` reflects whether `arena.sqlite` is present under the configured
store root — a quick way to catch a misconfigured `NIRS4ALL_BENCHMARKS_STORE`.

## Store location: NIRS4ALL_BENCHMARKS_STORE

When you start the app via the factory, the store root is resolved in this order:

1. an explicit `store_root` argument to `create_app` (not used by the CLI/Uvicorn
   paths);
2. the `NIRS4ALL_BENCHMARKS_STORE` environment variable;
3. the fallback `./arena-store` (relative to the process working directory).

For any deployment that does not run `n4a-benchmarks serve`, set
`NIRS4ALL_BENCHMARKS_STORE` to an **absolute** path so the store does not depend
on the working directory. The CLI commands take `--store` instead and do not read
this variable (except `serve`, which sets it for the Uvicorn subprocess).

## Docker

A minimal production image using `python:3.11-slim` and `uv`:

```dockerfile
# syntax=docker/dockerfile:1
FROM python:3.11-slim

# uv for fast, reproducible installs
COPY --from=ghcr.io/astral-sh/uv:latest /uv /usr/local/bin/uv

WORKDIR /app
ENV PYTHONUNBUFFERED=1 \
    UV_SYSTEM_PYTHON=1 \
    NIRS4ALL_BENCHMARKS_STORE=/data/arena-store

# Install the package with the web service extra.
RUN uv pip install --system --no-cache 'nirs4all-benchmarks[service]'

# The store lives on a mounted volume, not in the image.
VOLUME ["/data/arena-store"]
EXPOSE 8000

CMD ["uvicorn", "--factory", "nirs4all_benchmarks.service.app:create_app", \
     "--host", "0.0.0.0", "--port", "8000", "--workers", "4"]
```

The SPA ships inside the wheel (`src/nirs4all_benchmarks/web/**`), so no Node
build step is required.

The store is intentionally **not** baked into the image: mount it as a volume so
data survives container replacement. The `arena.sqlite` file is created on first
open, but the directory must be writable. If the volume starts empty, seed it
once (for example by running `n4a-benchmarks fixtures --store /data/arena-store`
or `n4a-benchmarks ingest-export ... --store /data/arena-store` in a one-off
container against the same volume) before or after starting the service.

Build and run:

```bash
docker build -t arena .
docker run -d --name arena -p 8000:8000 \
  -v "$(pwd)/arena-store:/data/arena-store" arena
```

## docker-compose with a persistent volume

```yaml
services:
  arena:
    image: arena:latest          # or: build: .
    command:
      - uvicorn
      - --factory
      - nirs4all_benchmarks.service.app:create_app
      - --host=0.0.0.0
      - --port=8000
      - --workers=4
    environment:
      NIRS4ALL_BENCHMARKS_STORE: /data/arena-store
    ports:
      - "8000:8000"
    volumes:
      - ./arena-store:/data/arena-store   # persistent store on the host
    healthcheck:
      test: ["CMD", "python", "-c",
             "import urllib.request,sys; sys.exit(0 if urllib.request.urlopen('http://localhost:8000/api/healthz').status==200 else 1)"]
      interval: 30s
      timeout: 5s
      retries: 3
    restart: unless-stopped
```

The host-side `./arena-store` directory holds `arena.sqlite`, `arrays/`, and
`exports/`. Back that one directory up and you have backed up the entire Arena.

To seed or ingest into the running deployment's volume without taking the service
down, run a one-off command in the same project:

```bash
docker compose run --rm arena n4a-benchmarks fixtures --store /data/arena-store
docker compose run --rm arena \
  n4a-benchmarks ingest-export /data/arena-store/exports --store /data/arena-store
```

## Reverse proxy

The app serves the SPA at `/` and JSON at `/api/*` from the same origin, so a
reverse proxy can forward everything to the Uvicorn upstream with one location
block. Example nginx:

```nginx
server {
    listen 443 ssl;
    server_name arena.example.org;

    # TLS config omitted

    location / {
        proxy_pass         http://127.0.0.1:8000;
        proxy_set_header   Host              $host;
        proxy_set_header   X-Forwarded-For   $proxy_add_x_forwarded_for;
        proxy_set_header   X-Forwarded-Proto $scheme;
    }
}
```

Notes:

- The SPA and APIs are same-origin, so no CORS configuration is needed for the
  bundled UI.
- The Arena ships no authentication. `POST /api/ingest` is an unauthenticated
  write. For a public deployment, either keep the deployment read-only by
  blocking that route at the proxy, or put the whole service behind your own auth
  (e.g. proxy-level basic auth / SSO). Restricting `POST /api/ingest` while
  leaving `GET /api/*` and `/` open gives a public read-only leaderboard.
- The store is weights-free by design — only canonical pipeline descriptors,
  scores, and publishable residuals are persisted — but the residual values are
  still data; gate writes accordingly.

## Backup and restore

The store is just files, so backup is a directory copy. Stop writers (or accept a
crash-consistent snapshot) and copy the store root:

```bash
# cold copy (service stopped or read-only): everything that matters
cp -a /data/arena-store /backups/arena-store-$(date +%F)
```

Restoring is the reverse: put the directory back and point
`NIRS4ALL_BENCHMARKS_STORE` (or `--store`) at it.

What is in each part:

- `arena.sqlite` — all metadata and scores. With WAL enabled (see below), a live
  copy must include the `arena.sqlite-wal` and `arena.sqlite-shm` sidecar files
  if you copy while the service is running; the safest backup is taken with the
  service stopped, or via `sqlite3 arena.sqlite ".backup ..."` against a quiesced
  store.
- `arrays/` — residual Parquet files, one per `residual_set` (`residuals_<hash>.parquet`).
- `exports/` — the original ingested `ArenaRunExport` bundles, kept for audit and
  replay.

Because identity is content-addressed and writes are idempotent (`INSERT OR
IGNORE`, content-hash file names), you can also reconstruct a store by replaying
the `exports/` bundles into a fresh `init`ed store with `ingest-export` —
re-ingesting the same export is a no-op rather than a duplicate.

## Operational notes

- **WAL mode.** `arena.sqlite` is opened with `PRAGMA journal_mode = WAL` and
  `PRAGMA foreign_keys = ON`. WAL allows concurrent readers alongside a writer,
  which suits the multi-worker read-heavy service. It also means the live
  database is `arena.sqlite` plus `-wal`/`-shm` sidecars — account for them when
  copying a hot store.
- **Schema version guard.** The store stamps `PRAGMA user_version` with the
  library's `ARENA_SCHEMA_VERSION` (currently `1`). Opening a store written by a
  newer library raises `ArenaStoreVersionError` and refuses to run — upgrade the
  package rather than downgrading the data.
- **Residual size cap.** Ingestion caps per-`residual_set` rows at
  `IngestionPolicy.residual_row_cap` (default `1_000_000`). Beyond the cap the run
  is stored **scores-only**: the per-sample residual array is dropped and the
  clean report is annotated `residuals_truncated`. The leaderboard and metric
  views still work; the residual/compare views just have no array for that run.
  Raise or lower the cap by constructing a custom `IngestionPolicy` if you ingest
  programmatically.
- **Shared filesystem only.** SQLite (and the WAL) assume a local filesystem
  with working POSIX locks. Keep the store on a local volume; do not place it on
  NFS or object-storage gateways shared across hosts.
- **Single store per service.** One running service points at exactly one store
  root. To serve multiple stores, run multiple instances on different ports, each
  with its own `NIRS4ALL_BENCHMARKS_STORE`.
