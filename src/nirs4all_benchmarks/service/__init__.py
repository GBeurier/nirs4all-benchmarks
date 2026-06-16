"""The Arena web service — REST API + static dataviz SPA (FastAPI).

``create_app(store_root)`` returns an ASGI app that serves the meta-analysis query
API (DATA_MANAGEMENT.md §6) and mounts the no-build dataviz SPA from ``web/``. The
``service`` extra (``fastapi``, ``uvicorn``) is required; import is guarded so the
rest of the package works without it.
"""

from __future__ import annotations

from nirs4all_benchmarks.service.app import create_app

__all__ = ["create_app"]
