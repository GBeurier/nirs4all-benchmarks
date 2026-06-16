"""FastAPI application factory for the Arena service."""

from __future__ import annotations

import os
from collections.abc import Iterator
from contextlib import contextmanager
from pathlib import Path
from typing import Any

from nirs4all_benchmarks import __version__
from nirs4all_benchmarks.store import ArenaStore, Queries

# UploadFile must be importable at MODULE scope: with `from __future__ import
# annotations` the route annotation `file: UploadFile | None` is a string that
# FastAPI resolves against this module's globals (not the factory's locals), so a
# factory-local import would make every multipart file POST raise at TypeAdapter
# build time. Guarded so the package still imports without the `service` extra.
try:
    from fastapi import UploadFile
except ImportError:  # pragma: no cover - optional dependency
    UploadFile = None  # type: ignore[assignment, misc]

_WEB_DIR = Path(__file__).resolve().parent.parent / "web"


def _store_root(explicit: str | Path | None) -> Path:
    root = explicit or os.environ.get("NIRS4ALL_BENCHMARKS_STORE") or "./arena-store"
    return Path(root)


def create_app(store_root: str | Path | None = None) -> Any:
    """Build the ASGI app. Raises ``RuntimeError`` if the ``service`` extra is missing."""
    try:
        from fastapi import Body, FastAPI, File, Form, HTTPException, Query
        from fastapi.responses import JSONResponse
        from fastapi.staticfiles import StaticFiles
    except ImportError as exc:  # pragma: no cover - optional dependency
        raise RuntimeError(
            "the web service needs the 'service' extra: pip install 'nirs4all-benchmarks[service]'"
        ) from exc

    root = _store_root(store_root)

    @contextmanager
    def queries() -> Iterator[Queries]:
        store = ArenaStore(root)
        try:
            yield Queries(store)
        finally:
            store.close()

    app = FastAPI(
        title="nirs4all-benchmarks — the Arena",
        version=__version__,
        description="Reproducible, scored, weights-free NIRS pipeline benchmarks + dataviz.",
    )

    # ── meta ───────────────────────────────────────────────────────────
    @app.get("/api/healthz")
    def healthz() -> dict[str, Any]:
        return {
            "status": "ok", "version": __version__, "store": str(root),
            "store_exists": (root / "arena.sqlite").exists(),
        }

    @app.get("/api/overview")
    def overview() -> dict[str, Any]:
        with queries() as q:
            return q.overview()

    @app.get("/api/collections")
    def collections() -> list[dict[str, Any]]:
        with queries() as q:
            return q.collections()

    @app.get("/api/datasets")
    def datasets() -> list[dict[str, Any]]:
        with queries() as q:
            return q.datasets()

    @app.get("/api/pipelines")
    def pipelines() -> list[dict[str, Any]]:
        with queries() as q:
            return q.pipelines()

    @app.get("/api/operators")
    def operators() -> list[dict[str, Any]]:
        with queries() as q:
            return q.operators()

    @app.get("/api/parameters")
    def parameters() -> list[dict[str, Any]]:
        with queries() as q:
            return q.sweepable_parameters()

    # ── dataviz queries ────────────────────────────────────────────────
    @app.get("/api/leaderboard")
    def leaderboard(
        metric: str = "rmse",
        scope: str = "cv",
        partition: str | None = None,
        dataset: str | None = None,
        task: str | None = None,
        collection: str | None = None,
        include_quarantined: bool = False,
        limit: int = Query(200, le=2000),
    ) -> dict[str, Any]:
        with queries() as q:
            return q.leaderboard(
                metric=metric, scope=scope, partition=partition, dataset_fingerprint=dataset,
                task_hash=task, collection_id=collection, include_quarantined=include_quarantined, limit=limit,
            )

    @app.get("/api/matrix")
    def matrix(metric: str = "rmse", scope: str = "cv", include_quarantined: bool = False) -> dict[str, Any]:
        with queries() as q:
            return q.matrix(metric=metric, scope=scope, include_quarantined=include_quarantined)

    @app.get("/api/runs")
    def runs(
        metric: str = "rmse",
        scope: str = "cv",
        dataset: str | None = None,
        pipeline: str | None = None,
        operator: str | None = None,
        include_quarantined: bool = True,
        limit: int = Query(500, le=5000),
    ) -> list[dict[str, Any]]:
        with queries() as q:
            return q.run_explorer(
                metric=metric, scope=scope, dataset_fingerprint=dataset, pipeline_dag_hash=pipeline,
                operator=operator, include_quarantined=include_quarantined, limit=limit,
            )

    @app.get("/api/operator-effect")
    def operator_effect(metric: str = "rmse", scope: str = "cv") -> dict[str, Any]:
        with queries() as q:
            return q.operator_effect(metric=metric, scope=scope)

    @app.get("/api/parameter-effect")
    def parameter_effect(param: str, metric: str = "rmse", scope: str = "cv") -> dict[str, Any]:
        with queries() as q:
            return q.parameter_effect(param, metric=metric, scope=scope)

    @app.get("/api/robustness")
    def robustness(metric: str = "rmse", scope: str = "cv") -> list[dict[str, Any]]:
        with queries() as q:
            return q.robustness(metric=metric, scope=scope)

    # ── faceting / pivot / playground ──────────────────────────────────
    @app.get("/api/facets")
    def facets() -> list[dict[str, Any]]:
        with queries() as q:
            return q.facets()

    @app.get("/api/facet-values")
    def facet_values(key: str) -> list[dict[str, Any]]:
        with queries() as q:
            return q.facet_values(key)

    @app.get("/api/pivot")
    def pivot(
        group_by: str,
        color_by: str | None = None,
        metric: str = "rmse",
        scope: str = "cv",
        partition: str | None = None,
        dataset: str | None = None,
        include_quarantined: bool = False,
        agg: str = "mean",
    ) -> dict[str, Any]:
        with queries() as q:
            return q.pivot(
                group_by=group_by, color_by=color_by, metric=metric, scope=scope, partition=partition,
                dataset_fingerprint=dataset, include_quarantined=include_quarantined, agg=agg,
            )

    @app.get("/api/parallel")
    def parallel(dimensions: str, metric: str = "rmse", scope: str = "cv",
                 include_quarantined: bool = False) -> dict[str, Any]:
        dims = [d.strip() for d in dimensions.split(",") if d.strip()]
        with queries() as q:
            return q.parallel(dimensions=dims, metric=metric, scope=scope,
                              include_quarantined=include_quarantined)

    @app.get("/api/planned")
    def planned() -> list[dict[str, Any]]:
        with queries() as q:
            return q.planned()

    @app.get("/api/graph")
    def graph(kind: str = "pipelines", metric: str = "rmse", scope: str = "cv",
              min_jaccard: float = 0.34) -> dict[str, Any]:
        with queries() as q:
            if kind == "operators":
                return q.operator_graph(metric=metric, scope=scope)
            return q.pipeline_graph(metric=metric, scope=scope, min_jaccard=min_jaccard)

    @app.get("/api/composition")
    def composition(metric: str = "rmse", scope: str = "cv") -> dict[str, Any]:
        with queries() as q:
            return q.composition(metric=metric, scope=scope)

    @app.get("/api/stats")
    def stats(metric: str = "rmse", scope: str = "cv") -> dict[str, Any]:
        with queries() as q:
            return q.stats(metric=metric, scope=scope)

    @app.get("/api/run/{execution_hash}")
    def run_detail(execution_hash: str) -> dict[str, Any]:
        with queries() as q:
            detail = q.run_detail(execution_hash)
            if detail is None:
                raise HTTPException(status_code=404, detail="execution not found")
            return detail

    @app.get("/api/run/{execution_hash}/residuals")
    def run_residuals(execution_hash: str, partition: str | None = None) -> list[dict[str, Any]]:
        with queries() as q:
            return q.residuals(execution_hash, partition=partition)

    @app.get("/api/compare")
    def compare(a: str, b: str, partition: str = "validation") -> dict[str, Any]:
        with queries() as q:
            return q.residual_compare(a, b, partition=partition)

    # ── ingestion (upload an ArenaRunExport manifest) ──────────────────
    @app.post("/api/ingest")
    def ingest(
        payload: dict[str, Any] = Body(..., description="An ArenaRunExport manifest."),
        collection: str = Query("uploads"),
        as_release: bool = Query(False),
    ) -> dict[str, Any]:
        from nirs4all_benchmarks.ingestion import IngestionPolicy, ingest_export

        store = ArenaStore(root)
        try:
            policy = IngestionPolicy(
                collection_id=collection,
                collection_kind="benchmark_release" if as_release else "user_run_collection",
            )
            result = ingest_export(store, payload, policy=policy)
        finally:
            store.close()
        return {
            "status": result.status,
            "validity_status": result.validity_status,
            "run_condition_hash": result.run_condition_hash,
            "execution_hash": result.execution_hash,
            "arena_export_hash": result.arena_export_hash,
            "issues": result.issues,
            "clean_report": result.clean_report,
        }

    @app.post("/api/upload")
    async def upload_route(
        file: UploadFile | None = File(None),
        text: str | None = Form(None),
        target_datasets: str = Form(""),
        collection: str = Form("uploads"),
        as_release: bool = Form(False),
    ) -> dict[str, Any]:
        """Unified upload: a .n4a / pipeline JSON|YAML / dag-ml bundle / ArenaRunExport.

        Auto-detects the payload and runs the run/store/display state machine, planning
        runs against any ``target_datasets`` (comma-separated fingerprints or names).
        """
        import os
        import tempfile

        from nirs4all_benchmarks.ingestion import upload as do_upload

        targets = [t.strip() for t in target_datasets.split(",") if t.strip()]
        store = ArenaStore(root)
        try:
            if file is not None and file.filename:
                suffix = Path(file.filename).suffix or ".bin"
                fd, tmp_path = tempfile.mkstemp(suffix=suffix)
                try:
                    with os.fdopen(fd, "wb") as fh:
                        fh.write(await file.read())
                    result = do_upload(store, tmp_path, collection_id=collection,
                                       target_datasets=targets, as_release=as_release)
                finally:
                    os.unlink(tmp_path)
            elif text:
                result = do_upload(store, text, collection_id=collection,
                                   target_datasets=targets, as_release=as_release)
            else:
                raise HTTPException(status_code=400, detail="provide a file or text")
            return result.to_json()
        finally:
            store.close()

    @app.exception_handler(ValueError)
    async def _value_error_handler(_request: Any, exc: ValueError) -> Any:
        return JSONResponse(status_code=400, content={"detail": str(exc)})

    # ── static SPA ─────────────────────────────────────────────────────
    if _WEB_DIR.is_dir():
        app.mount("/", StaticFiles(directory=str(_WEB_DIR), html=True), name="web")

    return app
