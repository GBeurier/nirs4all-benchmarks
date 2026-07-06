"""Bridge nirs4all-repository pipeline recipes into the Arena planner.

This module is intentionally a consumer-only integration: it reads a repository
pipeline recipe, strips artifacts by requesting recipe-only data, and delegates
registration/planning to the existing upload machinery. It does not mutate the
repository and it never executes the pipeline.
"""

from __future__ import annotations

import importlib
from pathlib import Path
from typing import Any

from nirs4all_benchmarks.ingestion.upload import UploadResult, register_pipeline
from nirs4all_benchmarks.store import ArenaStore


def _load_repository_module() -> Any:
    try:
        return importlib.import_module("nirs4all_repository")
    except ModuleNotFoundError as exc:
        if exc.name != "nirs4all_repository":
            raise
        raise ImportError(
            "nirs4all-repository support requires the optional 'nirs4all_repository' package. "
            "Install 'nirs4all-benchmarks[repository]' on Python 3.11+ to register repository pipeline recipes."
        ) from exc


def _repository_kwargs(
    *,
    repository_root: str | Path | None,
    cache_dir: str | Path | None,
    verify: bool,
) -> dict[str, Any]:
    kwargs: dict[str, Any] = {"verify": verify}
    if repository_root is not None:
        kwargs["root"] = repository_root
    if cache_dir is not None:
        kwargs["cache_dir"] = cache_dir
    return kwargs


def _repository_function(repository: Any, *names: str) -> Any:
    for name in names:
        fn = getattr(repository, name, None)
        if callable(fn):
            return fn
    expected = " / ".join(names)
    raise AttributeError(f"nirs4all_repository must expose {expected}")


def _pipeline_label(pipeline: Any, fallback: str) -> str:
    descriptor = getattr(pipeline, "descriptor", None)
    return (
        getattr(pipeline, "label", None)
        or getattr(pipeline, "name", None)
        or getattr(descriptor, "name", None)
        or getattr(pipeline, "id", None)
        or fallback
    )


def register_repository_pipeline(
    store: ArenaStore,
    name: str,
    *,
    collection_id: str = "repository",
    target_datasets: list[str] | None = None,
    repository_root: str | Path | None = None,
    cache_dir: str | Path | None = None,
    verify: bool = True,
) -> UploadResult:
    """Register and plan a pipeline recipe from ``nirs4all-repository``.

    The repository dependency is imported lazily. The fetched pipeline is asked
    for its recipe only (``with_artifacts=False``), then the existing
    ``register_pipeline`` path handles Arena identity rows and planned runs.
    Provider-style ``get_pipeline(...)`` is preferred when present, with a
    fallback to the historical ``get(...)`` API.
    """
    repository = _load_repository_module()
    get_fn = _repository_function(repository, "get_pipeline", "get")

    pipeline = get_fn(
        name,
        **_repository_kwargs(repository_root=repository_root, cache_dir=cache_dir, verify=verify),
        with_artifacts=False,
    )
    recipe_fn = getattr(pipeline, "recipe", None)
    if not callable(recipe_fn):
        raise TypeError(f"repository pipeline '{name}' does not expose a callable recipe() method")
    recipe = recipe_fn()

    return register_pipeline(
        store,
        recipe,
        collection_id=collection_id,
        target_datasets=target_datasets,
        source=f"nirs4all-repository:{name}",
        human_label=_pipeline_label(pipeline, name),
    )


def list_repository_pipelines(
    *,
    framework: str | None = None,
    task: str | None = None,
    tag: str | None = None,
    kind: str | None = None,
    trust: str | None = None,
    repository_root: str | Path | None = None,
) -> Any:
    """Return ``nirs4all-repository`` pipeline listings through its lazy import.

    Provider-style ``get_pipeline_list(...)`` is preferred when present, then
    ``list_pipelines(...)``, then the original ``list(...)`` function.
    """
    repository = _load_repository_module()
    list_fn = _repository_function(repository, "get_pipeline_list", "list_pipelines", "list")
    return list_fn(framework=framework, task=task, tag=tag, kind=kind, trust=trust, root=repository_root)
