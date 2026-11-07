"""FastAPI application factory for the Arena service."""

from __future__ import annotations

import importlib
import os
from collections.abc import Iterator
from contextlib import contextmanager
from pathlib import Path
from typing import Any

from nirs4all_benchmarks import __version__
from nirs4all_benchmarks.ranking import RankingRequest, RelativeGainRequest, quality_ranking, quality_relative_gain
from nirs4all_benchmarks.ranking.component_analysis import ComponentAnalysisRequest, component_analysis
from nirs4all_benchmarks.store import ArenaStore, ArenaStoreRefusal, Queries
from nirs4all_benchmarks.store.arena_store import store_header_status

# UploadFile must be importable at MODULE scope: with `from __future__ import
# annotations` the route annotation `file: UploadFile | None` is a string that
# FastAPI resolves against this module's globals (not the factory's locals), so a
# factory-local import would make every multipart file POST raise at TypeAdapter
# build time. Guarded so the package still imports without the `service` extra.
try:
    UploadFile: Any = importlib.import_module("fastapi").UploadFile
except ImportError:  # pragma: no cover - optional dependency
    UploadFile = None

_WEB_DIR = Path(__file__).resolve().parent.parent / "web"


def _store_root(explicit: str | Path | None) -> Path:
    root = explicit or os.environ.get("NIRS4ALL_BENCHMARKS_STORE") or "./arena-store"
    return Path(root)


def create_app(store_root: str | Path | None = None) -> Any:
    """Build the unauthenticated, offline-only ASGI application."""
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
    def healthz() -> Any:
        status = store_header_status(root)
        return JSONResponse(status_code=200 if status["status"] == "schema_3_header" else 503,
                            content={**status, "version":__version__, "fingerprint_checked":False})

    from nirs4all_benchmarks.service.routes import register_routes

    register_routes(app, queries)

    # ── observed ranking and native component analysis (offline service only) ──
    # Deliberately not part of the shared routes: serve-project exposes only the
    # right-scoped query routes of the accepted project service.
    @app.post("/api/component-analysis")
    def component_analysis_post(payload: ComponentAnalysisRequest) -> dict[str, Any]:
        with ArenaStore(root) as store:
            return component_analysis(store, payload)

    @app.get("/api/component-analysis/requests")
    def component_analysis_requests() -> dict[str, Any]:
        return {"mode": "service", "requests": [], "methods": ["mixed_effects", "fanova"]}

    @app.post("/api/quality-relative-gain")
    def quality_relative_gain_post(payload: RelativeGainRequest) -> dict[str, Any]:
        with ArenaStore(root) as store:
            return quality_relative_gain(store, payload)

    @app.get("/api/quality-relative-gain/requests")
    def quality_relative_gain_requests() -> dict[str, Any]:
        return {"mode": "service", "requests": []}

    @app.get("/api/quality-ranking/requests")
    def quality_ranking_requests() -> dict[str, Any]:
        from nirs4all_benchmarks.ranking.quality import CAPABILITIES

        return {"mode": "service", "requests": [], "capabilities": CAPABILITIES}

    @app.post("/api/quality-ranking")
    def quality_ranking_post(payload: RankingRequest) -> dict[str, Any]:
        with ArenaStore(root) as store:
            return quality_ranking(store, payload)

    @app.get("/api/quality-ranking")
    def quality_ranking_get(
        metric: str = "rmse", scope: str = "cv", partition: str = "validation",
        dimension: str = "pipeline", method: str = "bradley_terry", direction: str | None = None,
        dataset: str | None = None, task: str | None = None, collection: str | None = None,
        protocol: str | None = None, operator: str | None = None, parameter: str | None = None,
        split_criterion: str | None = None, tie_tolerance: float = 0.0,
        comparison_mode: str = "observed",
    ) -> dict[str, Any]:
        from pydantic import ValidationError

        try:
            request = RankingRequest(metric=metric, scope=scope, partition=partition, dimension=dimension,
                                     method=method, direction=direction, dataset=dataset, task=task,
                                     collection=collection, protocol=protocol, operator=operator, parameter=parameter,
                                     split_criterion=split_criterion, tie_tolerance=tie_tolerance,
                                     comparison_mode=comparison_mode)
        except ValidationError as exc:
            raise HTTPException(status_code=422, detail=str(exc)) from exc
        with ArenaStore(root) as store:
            return quality_ranking(store, request)

    # ── ingestion (upload an ArenaRunExport manifest) ──────────────────
    @app.post("/api/ingest")
    def ingest(
        payload: dict[str, Any] = Body(..., description="An ArenaRunExport manifest."),
        collection: str = Query("uploads"),
        as_release: bool = Query(False),
    ) -> dict[str, Any]:
        from nirs4all_benchmarks.ingestion import IngestionPolicy, ingest_export, validate_metadata

        validation = validate_metadata(payload)
        if validation.status != "metadata_valid":
            raise HTTPException(status_code=400, detail=validation.issues[0]["code"])
        store = ArenaStore(root)
        try:
            policy = IngestionPolicy(
                collection_id=collection,
                collection_kind="benchmark_release" if as_release else "user_run_collection",
            )
            result = ingest_export(store, payload, policy=policy, _validated=validation)
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
        from nirs4all_benchmarks.ingestion.upload import prepare_upload

        targets = [t.strip() for t in target_datasets.split(",") if t.strip()]
        tmp_path = None
        try:
            if file is not None and file.filename:
                fd, tmp_path = tempfile.mkstemp(suffix=Path(file.filename).suffix or ".bin")
                with os.fdopen(fd, "wb") as fh:
                    fh.write(await file.read())
                payload = prepare_upload(tmp_path)
            elif text:
                payload = prepare_upload(text, allow_paths=False)
            else:
                raise HTTPException(status_code=400, detail="provide a file or text")
            with ArenaStore(root) as store:
                result = do_upload(store, payload, collection_id=collection, target_datasets=targets,
                                   as_release=as_release, allow_paths=tmp_path is not None and payload == tmp_path)
            return result.to_json()
        finally:
            if tmp_path is not None:
                os.unlink(tmp_path)

    @app.exception_handler(ArenaStoreRefusal)
    async def _store_refusal_handler(_request: Any, exc: ArenaStoreRefusal) -> Any:
        return JSONResponse(status_code=503 if exc.code == "arena_store_busy" else 400,
                            content={"detail": exc.code})

    @app.exception_handler(ValueError)
    async def _value_error_handler(_request: Any, exc: ValueError) -> Any:
        return JSONResponse(status_code=400, content={"detail": str(exc)})

    # ── static SPA ─────────────────────────────────────────────────────
    if _WEB_DIR.is_dir():
        app.mount("/", StaticFiles(directory=str(_WEB_DIR), html=True), name="web")

    return app
