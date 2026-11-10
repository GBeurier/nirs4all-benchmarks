"""Shared Arena query routes."""

from __future__ import annotations

from typing import Any


def register_routes(app: Any, queries: Any, *, project: bool = False) -> None:
    from fastapi import HTTPException, Query

    def scoped(value: Any) -> Any:
        if isinstance(value, list):
            for row in value:
                row["pipeline_label_scope"] = "pipeline_first_accepted"
        else:
            value["pipeline_label_scope"] = "pipeline_first_accepted"
        return value

    def execution_rows(q: Any, value: Any) -> Any:
        rows = value if isinstance(value, list) else value["rows"]
        if project:
            for row in rows:
                row.pop("pipeline_label", None)
                row["pipeline_human_label"] = q.submission_context(row["execution_hash"])["pipeline_human_label"]
            return value
        return scoped(value)

    @app.get("/api/unknown-context")
    def unknown_context(limit: int = Query(200, ge=1, le=2000)) -> dict[str, Any]:
        with queries() as q:
            return execution_rows(q, q.unknown_context_rows(limit=limit))

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
            return q.datasets_api()

    @app.get("/api/pipelines")
    def pipelines() -> list[dict[str, Any]]:
        with queries() as q:
            return scoped(q.pipelines())

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
        scope: str = "cv", aggregation_level: str = "sample", source_fold_id: str | None = None,
        partition: str | None = None,
        dataset: str | None = None,
        task: str | None = None,
        collection: str | None = None,
        include_quarantined: bool = False,
        limit: int = Query(200, ge=1, le=2000) if project else Query(200, le=2000),
    ) -> dict[str, Any]:
        with queries() as q:
            return scoped(q.leaderboard(
                metric=metric,
                scope=scope,
                partition=partition,
                aggregation_level=aggregation_level,
                source_fold_id=source_fold_id,
                dataset_fingerprint=dataset,
                task_hash=task,
                collection_id=collection,
                include_quarantined=include_quarantined,
                limit=limit,
            ))

    @app.get("/api/matrix")
    def matrix(
        metric: str = "rmse",
        scope: str = "cv",
        aggregation_level: str = "sample",
        source_fold_id: str | None = None,
        partition: str | None = None,
        dataset: str | None = None,
        task: str | None = None,
        include_quarantined: bool = False,
    ) -> dict[str, Any]:
        with queries() as q:
            return scoped(q.matrix(
                metric=metric,
                scope=scope,
                partition=partition,
                aggregation_level=aggregation_level,
                source_fold_id=source_fold_id,
                dataset_fingerprint=dataset,
                task_hash=task,
                include_quarantined=include_quarantined,
            ))

    @app.get("/api/runs")
    def runs(
        metric: str = "rmse",
        scope: str = "cv",
        aggregation_level: str = "sample",
        source_fold_id: str | None = None,
        partition: str | None = None,
        dataset: str | None = None,
        task: str | None = None,
        pipeline: str | None = None,
        operator: str | None = None,
        include_quarantined: bool = True,
        # the Run explorer asks for every run (web/views/runs.js MAX_RUNS)
        limit: int = Query(500, ge=1, le=2000) if project else Query(500, le=100000),
    ) -> list[dict[str, Any]]:
        with queries() as q:
            return execution_rows(q, q.run_explorer(
                metric=metric,
                scope=scope,
                partition=partition,
                aggregation_level=aggregation_level,
                source_fold_id=source_fold_id,
                dataset_fingerprint=dataset,
                task_hash=task,
                pipeline_dag_hash=pipeline,
                operator=operator,
                include_quarantined=include_quarantined,
                limit=limit,
            ))

    @app.get("/api/operator-effect")
    def operator_effect(
        metric: str = "rmse",
        scope: str = "cv",
        aggregation_level: str = "sample",
        source_fold_id: str | None = None,
        partition: str | None = None,
        dataset: str | None = None,
        task: str | None = None,
    ) -> dict[str, Any]:
        with queries() as q:
            return q.operator_effect(
                metric=metric,
                scope=scope,
                partition=partition,
                aggregation_level=aggregation_level,
                source_fold_id=source_fold_id,
                dataset_fingerprint=dataset,
                task_hash=task,
            )

    @app.get("/api/parameter-effect")
    def parameter_effect(
        param: str,
        metric: str = "rmse",
        scope: str = "cv",
        aggregation_level: str = "sample",
        source_fold_id: str | None = None,
        partition: str | None = None,
        dataset: str | None = None,
        task: str | None = None,
    ) -> dict[str, Any]:
        with queries() as q:
            return scoped(q.parameter_effect(
                param,
                metric=metric,
                scope=scope,
                partition=partition,
                aggregation_level=aggregation_level,
                source_fold_id=source_fold_id,
                dataset_fingerprint=dataset,
                task_hash=task,
            ))

    @app.get("/api/robustness")
    def robustness(
        metric: str = "rmse",
        scope: str = "cv",
        aggregation_level: str = "sample",
        source_fold_id: str | None = None,
        partition: str | None = None,
        dataset: str | None = None,
        task: str | None = None,
    ) -> list[dict[str, Any]]:
        with queries() as q:
            return scoped(q.robustness(
                metric=metric,
                scope=scope,
                partition=partition,
                aggregation_level=aggregation_level,
                source_fold_id=source_fold_id,
                dataset_fingerprint=dataset,
                task_hash=task,
            ))

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
        scope: str = "cv", aggregation_level: str = "sample", source_fold_id: str | None = None,
        partition: str | None = None,
        dataset: str | None = None,
        task: str | None = None,
        include_quarantined: bool = False,
        agg: str = "mean",
    ) -> dict[str, Any]:
        with queries() as q:
            return q.pivot(
                group_by=group_by,
                color_by=color_by,
                metric=metric,
                scope=scope,
                partition=partition,
                aggregation_level=aggregation_level,
                source_fold_id=source_fold_id,
                dataset_fingerprint=dataset,
                task_hash=task,
                include_quarantined=include_quarantined,
                agg=agg,
            )

    @app.get("/api/parallel")
    def parallel(
        dimensions: str,
        metric: str = "rmse",
        scope: str = "cv",
        aggregation_level: str = "sample",
        source_fold_id: str | None = None,
        partition: str | None = None,
        dataset: str | None = None,
        task: str | None = None,
        include_quarantined: bool = False,
    ) -> dict[str, Any]:
        dims = [d.strip() for d in dimensions.split(",") if d.strip()]
        with queries() as q:
            return execution_rows(q, q.parallel(
                dimensions=dims,
                metric=metric,
                scope=scope,
                partition=partition,
                aggregation_level=aggregation_level,
                source_fold_id=source_fold_id,
                dataset_fingerprint=dataset,
                task_hash=task,
                include_quarantined=include_quarantined,
            ))

    @app.get("/api/planned")
    def planned() -> list[dict[str, Any]]:
        with queries() as q:
            return scoped(q.planned())

    @app.get("/api/graph")
    def graph(
        kind: str = "pipelines",
        metric: str = "rmse",
        scope: str = "cv",
        aggregation_level: str = "sample",
        source_fold_id: str | None = None,
        partition: str | None = None,
        dataset: str | None = None,
        task: str | None = None,
        min_jaccard: float = 0.34,
    ) -> dict[str, Any]:
        with queries() as q:
            if kind == "operators":
                return q.operator_graph(
                    metric=metric,
                    scope=scope,
                    partition=partition,
                    aggregation_level=aggregation_level,
                    source_fold_id=source_fold_id,
                    dataset_fingerprint=dataset,
                task_hash=task,
                )
            return scoped(q.pipeline_graph(
                metric=metric,
                scope=scope,
                partition=partition,
                aggregation_level=aggregation_level,
                source_fold_id=source_fold_id,
                dataset_fingerprint=dataset,
                task_hash=task,
                min_jaccard=min_jaccard,
            ))

    @app.get("/api/composition")
    def composition(
        metric: str = "rmse",
        scope: str = "cv",
        aggregation_level: str = "sample",
        source_fold_id: str | None = None,
        partition: str | None = None,
        dataset: str | None = None,
        task: str | None = None,
    ) -> dict[str, Any]:
        with queries() as q:
            return q.composition(
                metric=metric,
                scope=scope,
                partition=partition,
                aggregation_level=aggregation_level,
                source_fold_id=source_fold_id,
                dataset_fingerprint=dataset,
                task_hash=task,
            )

    @app.get("/api/stats")
    def stats(
        metric: str = "rmse",
        scope: str = "cv",
        aggregation_level: str = "sample",
        source_fold_id: str | None = None,
        partition: str | None = None,
        dataset: str | None = None,
        task: str | None = None,
    ) -> dict[str, Any]:
        with queries() as q:
            return q.stats(
                metric=metric,
                scope=scope,
                partition=partition,
                aggregation_level=aggregation_level,
                source_fold_id=source_fold_id,
                dataset_fingerprint=dataset,
                task_hash=task,
            )

    @app.get("/api/run/{execution_hash}")
    def run_detail(execution_hash: str) -> dict[str, Any]:
        with queries() as q:
            detail = q.run_api_detail(execution_hash, project=project)
            if detail is None:
                raise HTTPException(status_code=404, detail="execution not found")
            if project and detail.get("residual_set"):
                detail["residual_set"].pop("parquet_path", None)
            return detail

    @app.get("/api/run/{execution_hash}/residuals")
    def run_residuals(execution_hash: str, partition: str | None = None) -> list[dict[str, Any]]:
        with queries() as q:
            if project and q.run_detail(execution_hash) is None:
                raise HTTPException(status_code=404, detail="not found")
            return q.residuals(execution_hash, partition=partition)

    @app.get("/api/compare")
    def compare(a: str, b: str, partition: str = "validation") -> dict[str, Any]:
        with queries() as q:
            if project and (q.run_detail(a) is None or q.run_detail(b) is None):
                raise HTTPException(status_code=404, detail="not found")
            return q.residual_compare(a, b, partition=partition)

