# nirs4all-benchmarks (the Arena) — production image for the dataviz service.
FROM python:3.11-slim AS base

# uv for fast, reproducible installs.
COPY --from=ghcr.io/astral-sh/uv:latest /uv /usr/local/bin/uv

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    UV_SYSTEM_PYTHON=1 \
    UV_COMPILE_BYTECODE=1 \
    NIRS4ALL_BENCHMARKS_STORE=/data/arena-store

WORKDIR /app

# Install dependencies first (cache layer), then the package + web assets.
COPY pyproject.toml README.md LICENSE ./
COPY LICENSES ./LICENSES
COPY src ./src
COPY data ./data
RUN uv pip install --system ".[service]"

# Persistent store lives on a mounted volume.
RUN mkdir -p /data/arena-store
VOLUME ["/data"]
EXPOSE 8000

# Seed nothing by default. Health stays uninitialized (503) until explicit init
# or ingestion; querying an empty service can also create a fresh v2 store.
# First explicitly convert the pinned source to /data/converted-history, then ingest
# that v2 output into the separate fresh /data/arena-store. History stays unranked.
# Existing schema 1 volumes are unsupported; no automatic migration or publication.
CMD ["uvicorn", "--factory", "nirs4all_benchmarks.service.app:create_app", \
     "--host", "0.0.0.0", "--port", "8000"]

HEALTHCHECK --interval=30s --timeout=5s --start-period=10s \
  CMD python -c "import urllib.request,sys; sys.exit(0 if urllib.request.urlopen('http://127.0.0.1:8000/api/healthz').status==200 else 1)"
