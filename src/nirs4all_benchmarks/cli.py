"""``n4a-benchmarks`` / ``n4a-arena`` — the Arena command line.

Init a store, ingest from any producer (export / nirs4all workspace / dag-ml bundle
/ ``.n4a``), seed fixtures, run the dataviz service, and query the store from the
terminal. The CLI is a thin shell over the same ``ingest_export`` / ``Queries`` API
the service uses.
"""

from __future__ import annotations

from pathlib import Path

import typer
from rich.console import Console
from rich.table import Table

from nirs4all_benchmarks import __version__

app = typer.Typer(
    name="n4a-benchmarks",
    help="The Arena — reproducible, scored, weights-free NIRS pipeline benchmarks + dataviz.",
    no_args_is_help=True,
    add_completion=False,
)
console = Console()

_DEFAULT_STORE = "./arena-store"
StoreOpt = typer.Option(_DEFAULT_STORE, "--store", "-s", help="Arena store directory.")


def _store(path: str):
    from nirs4all_benchmarks.store import ArenaStore

    return ArenaStore(path)


@app.command()
def version() -> None:
    """Print the version."""
    console.print(f"nirs4all-benchmarks [bold green]{__version__}[/]")


@app.command()
def init(store: str = StoreOpt) -> None:
    """Create (or open) an empty Arena store."""
    with _store(store) as s:
        console.print(f"[green]✓[/] store ready at [bold]{s.root}[/] (schema v{s.get_meta('arena_schema_version')})")


@app.command("ingest-export")
def ingest_export_cmd(
    path: Path = typer.Argument(..., exists=True, help="ArenaRunExport manifest JSON (or directory of them)."),
    store: str = StoreOpt,
    collection: str = typer.Option("default", "--collection", "-c"),
    release: bool = typer.Option(False, "--release", help="Ingest as a benchmark release (quarantine on leakage)."),
) -> None:
    """Ingest one or more ``ArenaRunExport`` manifests."""
    from nirs4all_benchmarks.ingestion import IngestionPolicy, ingest_export

    files = sorted(path.glob("*.json")) if path.is_dir() else [path]
    policy = IngestionPolicy(
        collection_id=collection,
        collection_kind="benchmark_release" if release else "user_run_collection",
    )
    counts: dict[str, int] = {}
    with _store(store) as s:
        for f in files:
            res = ingest_export(s, f, policy=policy)
            counts[res.status] = counts.get(res.status, 0) + 1
            console.print(f"  {f.name}: [bold]{res.status}[/] ({res.validity_status})")
    console.print(f"[green]✓[/] {counts}")


@app.command("ingest-workspace")
def ingest_workspace_cmd(
    workspace: Path = typer.Argument(..., exists=True, help="nirs4all workspace dir (store.sqlite + arrays/)."),
    store: str = StoreOpt,
    collection: str = typer.Option("nirs4all-workspace", "--collection", "-c"),
) -> None:
    """Adapter A — ingest a nirs4all workspace (artifacts ignored)."""
    from nirs4all_benchmarks.adapters import WorkspaceAdapter
    from nirs4all_benchmarks.ingestion import IngestionPolicy, ingest_export

    policy = IngestionPolicy(collection_id=collection, collection_kind="user_run_collection")
    counts: dict[str, int] = {}
    with _store(store) as s:
        for exp in WorkspaceAdapter(workspace).iter_exports():
            res = ingest_export(s, exp, policy=policy)
            counts[res.status] = counts.get(res.status, 0) + 1
    console.print(f"[green]✓[/] workspace ingested: {counts}")


@app.command("ingest-bundle")
def ingest_bundle_cmd(
    bundle: Path = typer.Argument(..., exists=True, help="dag-ml ExecutionBundle JSON."),
    store: str = StoreOpt,
    graph: Path | None = typer.Option(None, "--graph", help="Canonical GraphSpec JSON."),
    envelope: Path | None = typer.Option(None, "--envelope", help="io CoordinatorDataPlanEnvelope JSON."),
    collection: str = typer.Option("dag-ml", "--collection", "-c"),
    release: bool = typer.Option(True, "--release/--user", help="dag-ml runs are leakage-safe by construction."),
) -> None:
    """Adapter B — ingest a dag-ml ExecutionBundle (+ optional graph/envelope)."""
    from nirs4all_benchmarks.adapters import bundle_to_export
    from nirs4all_benchmarks.ingestion import IngestionPolicy, ingest_export

    exp = bundle_to_export(bundle, graph=graph, dataset_envelope=envelope)
    policy = IngestionPolicy(
        collection_id=collection,
        collection_kind="benchmark_release" if release else "user_run_collection",
    )
    with _store(store) as s:
        res = ingest_export(s, exp, policy=policy)
    console.print(f"[green]✓[/] bundle ingested: {res.status} ({res.validity_status})")


@app.command("ingest-pipeline")
def ingest_pipeline_cmd(
    path: Path = typer.Argument(..., exists=True, help="A pipeline recipe: .n4a, .json, or .yaml/.yml."),
    store: str = StoreOpt,
    datasets: str | None = typer.Option(None, "--datasets", "-d",
                                        help="Comma-separated target datasets (fingerprints or names)."),
    collection: str = typer.Option("uploads", "--collection", "-c"),
) -> None:
    """Register a pipeline (weights stripped) and plan/inspect it on target datasets."""
    from nirs4all_benchmarks.ingestion import upload

    targets = [t.strip() for t in (datasets or "").split(",") if t.strip()]
    with _store(store) as s:
        res = upload(s, path, collection_id=collection, target_datasets=targets)
    console.print(f"[green]✓[/] {res.kind}: {res.message}")
    for d in res.datasets:
        mark = "already run" if d["status"] == "already_run" else "planned"
        console.print(f"  • {d['token']}: [bold]{mark}[/]"
                      + (f" ({d['n_executions']} runs)" if d.get("n_executions") else ""))


@app.command("inspect-n4a")
def inspect_n4a_cmd(path: Path = typer.Argument(..., exists=True, help="A .n4a bundle.")) -> None:
    """Extract a ``.n4a`` recipe and show its canonical pipeline identity (weights stripped)."""
    from nirs4all_benchmarks.adapters import extract_n4a_recipe, n4a_pipeline_identity

    recipe = extract_n4a_recipe(path)
    pid = n4a_pipeline_identity(path)
    console.print(f"steps: [bold]{len(recipe['steps'])}[/]  stripped artifacts: {len(recipe['stripped_artifacts'])}")
    console.print(f"pipeline_dag_hash: [green]{pid.pipeline_dag_hash}[/]")
    console.print(f"nirs4all_identity_hash: {pid.nirs4all_identity_hash}")


@app.command()
def fixtures(
    store: str = StoreOpt,
    collection: str = typer.Option("fixtures", "--collection", "-c"),
    basic: bool = typer.Option(False, "--basic", help="Seed the small regression set instead of the rich demo."),
    write_to: Path | None = typer.Option(None, "--write-to", help="Also write fixture JSON exports here."),
) -> None:
    """Seed the store with synthetic data for the dataviz (rich demo by default)."""
    from nirs4all_benchmarks.fixtures import seed_store, write_fixture_exports

    summary = seed_store(store, collection_id=collection, demo=not basic)
    console.print(f"[green]✓[/] seeded ({'basic' if basic else 'rich demo'}): {summary}")
    if write_to:
        paths = write_fixture_exports(write_to)
        console.print(f"[green]✓[/] wrote {len(paths)} fixture exports to {write_to}")


@app.command()
def stats(store: str = StoreOpt) -> None:
    """Print store overview counts."""
    from nirs4all_benchmarks.store import Queries

    with _store(store) as s:
        ov = Queries(s).overview()
    table = Table(title="Arena store overview", show_header=False)
    for k, v in ov.items():
        if k == "metrics":
            v = ", ".join(v)
        table.add_row(k, str(v))
    console.print(table)


@app.command()
def leaderboard(
    store: str = StoreOpt,
    metric: str = typer.Option("rmse", "--metric", "-m"),
    scope: str = typer.Option("cv", "--scope"),
    dataset: str | None = typer.Option(None, "--dataset"),
    limit: int = typer.Option(15, "--limit", "-n"),
) -> None:
    """Print a leaderboard."""
    from nirs4all_benchmarks.store import Queries

    with _store(store) as s:
        lb = Queries(s).leaderboard(metric=metric, scope=scope, dataset_fingerprint=dataset, limit=limit)
    table = Table(title=f"Leaderboard — {metric} ({scope}, {lb['direction']})")
    table.add_column("#", justify="right")
    table.add_column("mean", justify="right")
    table.add_column("pipeline")
    table.add_column("main model", style="dim")
    for r in lb["rows"]:
        table.add_row(str(r["rank"]), f"{r['mean']:.4f}", r["pipeline_label"] or r["pipeline_dag_hash"][:10],
                      (r["main_model"] or "").split(".")[-1])
    console.print(table)


@app.command()
def serve(
    store: str = StoreOpt,
    host: str = typer.Option("127.0.0.1", "--host"),
    port: int = typer.Option(8000, "--port", "-p"),
    reload: bool = typer.Option(False, "--reload"),
) -> None:
    """Run the dataviz web service."""
    import os

    try:
        import uvicorn
    except ImportError as exc:
        raise typer.Exit(code=1) from exc
    os.environ["NIRS4ALL_BENCHMARKS_STORE"] = str(Path(store).resolve())
    console.print(f"[green]✓[/] serving Arena at [bold]http://{host}:{port}[/] (store: {store})")
    uvicorn.run("nirs4all_benchmarks.service.app:create_app", factory=True, host=host, port=port, reload=reload)


def main() -> None:
    app()


if __name__ == "__main__":
    main()
