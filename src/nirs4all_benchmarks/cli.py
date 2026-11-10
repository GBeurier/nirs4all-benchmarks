"""``n4a-benchmarks`` / ``n4a-arena`` — the Arena command line.

Init a store, ingest from any producer (export / nirs4all workspace / dag-ml bundle
/ ``.n4a``), seed fixtures, run the dataviz service, and query the store from the
terminal. The CLI is a thin shell over the same ``ingest_export`` / ``Queries`` API
the service uses.
"""

from __future__ import annotations

import json
from contextlib import contextmanager
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


@contextmanager
def _store(path: str):
    from nirs4all_benchmarks.store import ArenaStore, ArenaStoreRefusal

    try:
        with ArenaStore(path) as store:
            yield store
    except ArenaStoreRefusal as exc:
        typer.echo(f"rejected: {exc.code}")
        raise typer.Exit(code=2) from None


@app.command()
def version() -> None:
    """Print the version."""
    console.print(f"nirs4all-benchmarks [bold green]{__version__}[/]")


@app.command()
def init(store: str = StoreOpt) -> None:
    """Create (or open) an empty Arena store."""
    with _store(store) as s:
        console.print(f"[green]✓[/] store ready at [bold]{s.root}[/] (schema v{s.get_meta('arena_schema_version')})")


@app.command("validate-export")
def validate_export_cmd(
    path: Path = typer.Argument(..., exists=True, dir_okay=False, help="ArenaRunExport JSON; no store is opened."),
) -> None:
    """Validate local metadata only. Does not execute or scientifically verify a run."""
    import json

    from nirs4all_benchmarks.ingestion.offline import validate_metadata

    result = validate_metadata(path)
    typer.echo(json.dumps(result.to_json(), sort_keys=True))
    if result.status == "rejected":
        raise typer.Exit(code=2)


@app.command("ingest-export")
def ingest_export_cmd(
    path: Path = typer.Argument(..., exists=True, help="ArenaRunExport manifest JSON (or directory of them)."),
    store: str = StoreOpt,
    collection: str = typer.Option("default", "--collection", "-c"),
    release: bool = typer.Option(False, "--release", help="Ingest as a benchmark release (quarantine on leakage)."),
) -> None:
    """Ingest one or more ``ArenaRunExport`` manifests."""
    from nirs4all_benchmarks.ingestion import AtomicIngestionError, IngestionPolicy, ingest_exports_atomic

    files = sorted(path.glob("*.json")) if path.is_dir() else [path]
    policy = IngestionPolicy(
        collection_id=collection,
        collection_kind="benchmark_release" if release else "user_run_collection",
    )
    counts: dict[str, int] = {}
    from nirs4all_benchmarks.contract.strict_json import strict_loads
    from nirs4all_benchmarks.ingestion import validate_metadata

    try:
        payloads = [strict_loads(f.read_text(encoding="utf-8")) for f in files]
    except (OSError, ValueError, UnicodeError, RecursionError):
        typer.echo("rejected: manifest_invalid")
        raise typer.Exit(code=2) from None
    validations = [validate_metadata(payload) for payload in payloads]
    models = []
    for validation, payload in zip(validations, payloads, strict=True):
        if validation.status != "metadata_valid":
            typer.echo("rejected: " + validation.issues[0]["code"])
            raise typer.Exit(code=2)
        assert validation.model is not None
        models.append(payload)
    with _store(store) as s:
        try:
            results = ingest_exports_atomic(s, models, policy=policy)
        except AtomicIngestionError as exc:
            typer.echo(f"rejected: {exc.code}")
            raise typer.Exit(code=2) from None
        for result in results:
            counts[result.status] = counts.get(result.status, 0) + 1
    console.print(counts)


@app.command("ingest-workspace")
def ingest_workspace_cmd(
    workspace: Path = typer.Argument(..., exists=True, help="nirs4all workspace dir (store.sqlite + arrays/)."),
    store: str = StoreOpt,
    collection: str = typer.Option("nirs4all-workspace", "--collection", "-c"),
) -> None:
    """Adapter A — ingest a nirs4all workspace (artifacts ignored)."""
    from nirs4all_benchmarks.adapters import WorkspaceAdapterError, workspace_to_exports
    from nirs4all_benchmarks.ingestion import AtomicIngestionError, IngestionPolicy, ingest_exports_atomic

    policy = IngestionPolicy(collection_id=collection, collection_kind="user_run_collection")
    counts: dict[str, int] = {}
    try:
        exports = workspace_to_exports(workspace)
    except WorkspaceAdapterError as exc:
        typer.echo(f"rejected: {exc.code}")
        raise typer.Exit(code=2) from None
    except (ValueError, TypeError, KeyError, OSError):
        typer.echo("rejected: workspace_metadata_unsupported")
        raise typer.Exit(code=2) from None
    with _store(store) as s:
        try:
            results = ingest_exports_atomic(s, exports, policy=policy)
        except AtomicIngestionError as exc:
            typer.echo(f"rejected: {exc.code}")
            raise typer.Exit(code=2) from None
        for result in results:
            counts[result.status] = counts.get(result.status, 0)+1
    console.print(f"workspace metadata ingested: {counts}; scientific_verification=not_performed")


@app.command("ingest-history")
def ingest_history_cmd(
    path: Path = typer.Argument(..., exists=True, file_okay=False, help="Historical data pack directory."),
    store: str = StoreOpt,
) -> None:
    """Import checked-in real research scores (no synthetic fixtures or fitted artifacts)."""
    from nirs4all_benchmarks.history import ingest_history, load_history_exports
    from nirs4all_benchmarks.ingestion import AtomicIngestionError

    try:
        loaded = load_history_exports(path)
    except ValueError as exc:
        typer.echo(f"rejected: {exc}")
        raise typer.Exit(code=2) from None
    with _store(store) as s:
        try:
            summary = ingest_history(
                s, path, _loaded=loaded, progress=lambda done, total: console.print(f"Staged {done}/{total}")
            )
        except AtomicIngestionError as exc:
            typer.echo(f"rejected: {exc.code}")
            raise typer.Exit(code=2) from None
    console.print(summary)


@app.command("ingest-bundle")
def ingest_bundle_cmd(
    bundle: Path = typer.Argument(..., exists=True, help="dag-ml ExecutionBundle JSON."),
    store: str = StoreOpt,
    graph: Path | None = typer.Option(None, "--graph", help="Canonical GraphSpec JSON."),
    envelope: Path | None = typer.Option(None, "--envelope", help="io CoordinatorDataPlanEnvelope JSON."),
    metadata: Path | None = typer.Option(
        None, "--metadata", help="Owner-supplied ArenaRunExport JSON with genuine identities and context."),
    collection: str = typer.Option("dag-ml", "--collection", "-c"),
    release: bool = typer.Option(False, "--release/--user", help="Release mode requires producer OOF attestation."),
) -> None:
    """Adapter B — ingest a dag-ml ExecutionBundle (+ optional graph/envelope)."""
    from nirs4all_benchmarks.adapters import bundle_to_export
    from nirs4all_benchmarks.ingestion import IngestionPolicy, ingest_export

    try:
        exp = bundle_to_export(bundle, graph=graph, dataset_envelope=envelope, owner_export=metadata)
    except (ValueError, TypeError, KeyError, OSError) as exc:
        code = str(exc) if str(exc).startswith("dagml_") else "dagml_metadata_unsupported"
        typer.echo(f"rejected: {code}")
        raise typer.Exit(code=2) from None
    policy = IngestionPolicy(
        collection_id=collection,
        collection_kind="benchmark_release" if release else "user_run_collection",
    )
    with _store(store) as s:
        res = ingest_export(s, exp, policy=policy)
    console.print(f"bundle: {res.status}; metadata={res.metadata_status}; scientific_verification=not_performed")
    if not res.ok:
        raise typer.Exit(code=2)


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
    from nirs4all_benchmarks.ingestion.upload import prepare_upload

    try:
        prepared = prepare_upload(path)
    except ValueError as exc:
        typer.echo(f"rejected: {exc}")
        raise typer.Exit(code=2) from None
    targets = [t.strip() for t in (datasets or "").split(",") if t.strip()]
    with _store(store) as s:
        res = upload(s, prepared, collection_id=collection, target_datasets=targets)
    console.print(f"{res.kind}: {res.status}: {res.message}")
    if res.status == "rejected":
        raise typer.Exit(code=2)
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
    partition: str | None = typer.Option(None, "--partition", help="Required for refit; CV defaults to validation."),
    aggregation_level: str = typer.Option("sample", "--aggregation-level", help="sample, group or target."),
    source_fold_id: str | None = typer.Option(
        None, "--source-fold-id", help="Exact source aggregate label; required for view."),
    dataset: str | None = typer.Option(None, "--dataset"),
    limit: int = typer.Option(15, "--limit", "-n"),
) -> None:
    """Print a leaderboard."""
    from nirs4all_benchmarks.store import Queries

    with _store(store) as s:
        try:
            lb = Queries(s).leaderboard(
                metric=metric, scope=scope, partition=partition, aggregation_level=aggregation_level,
                source_fold_id=source_fold_id, dataset_fingerprint=dataset, limit=limit)
        except ValueError as exc:
            raise typer.BadParameter(str(exc)) from exc
    table = Table(title=f"Leaderboard — {metric} ({scope}, {lb['partition']}, "
                        f"{aggregation_level}, {source_fold_id or 'raw'}, {lb['direction']})")
    table.add_column("#", justify="right")
    table.add_column("mean", justify="right")
    table.add_column("pipeline")
    table.add_column("main model", style="dim")
    for r in lb["rows"]:
        table.add_row(str(r["rank"]), f"{r['mean']:.4f}", r["pipeline_label"] or r["pipeline_dag_hash"][:10],
                      (r["main_model"] or "").split(".")[-1])
    console.print(table)


@app.command("build-site")
def build_site_cmd(
    store: str = StoreOpt,
    out: Path = typer.Option(Path("site"), "--out", "-o", help="Output directory for the static site."),
    domain: str = typer.Option("benchmarks.nirs4all.org", "--domain", help="Custom domain (CNAME); empty to skip."),
    unavailable_results: bool = typer.Option(
        False, "--unavailable-results", help="Build only the data-free unavailable-results page; never open a store.",
    ),
    ranking_requests: Path | None = typer.Option(
        None, "--ranking-requests", exists=True, dir_okay=False,
        help="JSON file containing an array of exact ranking request objects to precompute.",
    ),
    relative_gain_requests: Path | None = typer.Option(
        None, "--relative-gain-requests", exists=True, dir_okay=False,
        help="JSON file containing an array of exact paired relative gain request objects to precompute.",
    ),
    component_analysis_requests: Path | None = typer.Option(
        None, "--component-analysis-requests", exists=True, dir_okay=False,
        help="JSON file containing exact mixed-effects or fANOVA analysis requests to precompute.",
    ),
) -> None:
    """Build a static Arena page; result snapshots remain closed."""
    from nirs4all_benchmarks.site_build import build_site

    if unavailable_results:
        if any(path is not None for path in (ranking_requests, relative_gain_requests, component_analysis_requests)):
            raise typer.BadParameter("--unavailable-results cannot include analysis request files")
        summary = build_site(store, out, domain=domain or None, unavailable_results=True)
        console.print(f"[green]✓[/] unavailable-results page built at [bold]{summary['out']}[/]; "
                      "no scientific records published")
        return

    from pydantic import ValidationError

    from nirs4all_benchmarks.ranking import ComponentAnalysisRequest, RankingRequest, RelativeGainRequest

    def read_requests(path: Path | None, model: type, option: str) -> list:
        if path is None:
            return []
        try:
            values = json.loads(path.read_text(encoding="utf-8"))
            if not isinstance(values, list) or not all(isinstance(value, dict) for value in values):
                raise ValueError("expected a JSON array of request objects")
            return [model(**value) for value in values]
        except (OSError, UnicodeError, ValueError, ValidationError) as exc:
            raise typer.BadParameter(str(exc), param_hint=option) from exc

    rankings = read_requests(ranking_requests, RankingRequest, "--ranking-requests")
    gains = read_requests(relative_gain_requests, RelativeGainRequest, "--relative-gain-requests")
    analyses = read_requests(component_analysis_requests, ComponentAnalysisRequest, "--component-analysis-requests")
    if len(rankings) + len(gains) + len(analyses) > 64:
        raise typer.BadParameter("At most 64 ranking, relative gain and component-analysis snapshots may be requested")
    summary = build_site(store, out, domain=domain or None, ranking_requests=rankings, relative_gain_requests=gains,
                         component_analysis_requests=analyses)
    console.print(f"[green]✓[/] static site built at [bold]{summary['out']}[/]: "
                  f"{summary['executions']} runs · {summary['pipelines']} pipelines · {summary['datasets']} datasets"
                  + (f" · {domain}" if domain else ""))


@app.command("perf-compare")
def perf_compare_cmd(
    plan: Path = typer.Option(..., "--plan", exists=True, dir_okay=False, help="Explicit performance plan JSON."),
    workspace_root: Path = typer.Option(..., "--workspace-root", exists=True, file_okay=False),
    archive: Path | None = typer.Option(None, "--archive", help="Explicit Archive V2 witness."),
    adapter: list[str] | None = typer.Option(None, "--adapter", help="Repeat SURFACE=/absolute/executable."),
    repeats: int = typer.Option(3, "--repeats", min=1),
    timeout: float = typer.Option(120.0, "--timeout", min=0.1),
    evidence_kind: str = typer.Option("local_real", "--evidence-kind"),
    json_out: Path | None = typer.Option(None, "--json-out", help="Write the full report as JSON."),
    markdown_out: Path | None = typer.Option(None, "--markdown-out", help="Write the markdown summary to a file."),
    handoff_dir: Path | None = typer.Option(None, "--handoff-dir"),
) -> None:
    """Compare one Archive V2 across four native product surfaces."""
    from nirs4all_benchmarks.performance_compare import (
        parse_adapter_overrides,
        render_markdown,
        run_comparison,
        write_web_handoff,
    )

    try:
        adapters = parse_adapter_overrides(adapter or [])
        report = run_comparison(
            plan_path=plan,
            workspace_root=workspace_root,
            adapters=adapters,
            archive=archive,
            repeats=repeats,
            timeout_seconds=timeout,
            evidence_kind=evidence_kind,
        )
    except ValueError as error:
        console.print(f"[red]refused:[/] {error}")
        raise typer.Exit(code=2) from error
    console.print(render_markdown(report))
    if json_out:
        json_out.parent.mkdir(parents=True, exist_ok=True)
        json_out.write_text(__import__("json").dumps(report, indent=2, sort_keys=True) + "\n", encoding="utf-8")
        console.print(f"[green]✓[/] wrote JSON report to [bold]{json_out}[/]")
    if markdown_out:
        markdown = render_markdown(report)
        markdown_out.parent.mkdir(parents=True, exist_ok=True)
        markdown_out.write_text(markdown + "\n", encoding="utf-8")
        console.print(f"[green]✓[/] wrote markdown report to [bold]{markdown_out}[/]")
    if handoff_dir:
        write_web_handoff(handoff_dir, report)
    if report["overall_disposition"] != "passed":
        raise typer.Exit(code=1)


@app.command("soak-run")
def soak_run_cmd(
    plan: Path = typer.Option(..., "--plan", exists=True, dir_okay=False),
    workspace_root: Path = typer.Option(..., "--workspace-root", exists=True, file_okay=False),
    json_out: Path = typer.Option(..., "--json-out", dir_okay=False),
) -> None:
    """Run a bounded local workload and integrity probe."""
    from nirs4all_benchmarks.soak_probe import run_plan, write_report

    try:
        report = run_plan(plan, workspace_root=workspace_root)
    except ValueError as error:
        console.print(f"[red]refused:[/] {error}")
        raise typer.Exit(code=2) from error
    write_report(json_out, report)
    if report["overall_status"] != "passed":
        raise typer.Exit(code=1)


@app.command("qualify-artifacts")
def qualify_artifacts_cmd(
    manifest: Path = typer.Argument(..., exists=True, dir_okay=False, resolve_path=True),
    json_out: Path = typer.Option(..., "--json-out"),
) -> None:
    """Qualify explicit Python/Core/Studio/Web artifacts."""
    import json

    from nirs4all_benchmarks.qualification import run_qualification

    report = run_qualification(manifest)
    json_out.parent.mkdir(parents=True, exist_ok=True)
    json_out.write_text(json.dumps(report, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    if report["overall_disposition"] == "failed":
        raise typer.Exit(code=1)
    if report["overall_disposition"] == "refused":
        raise typer.Exit(code=2)


@app.command()
def serve(
    store: str = StoreOpt,
    host: str = typer.Option("127.0.0.1", "--host"),
    port: int = typer.Option(8000, "--port", "-p"),
    reload: bool = typer.Option(False, "--reload"),
) -> None:
    """Run the unauthenticated offline-only dataviz service."""
    import os

    try:
        import uvicorn
    except ImportError as exc:
        raise typer.Exit(code=1) from exc
    os.environ["NIRS4ALL_BENCHMARKS_STORE"] = str(Path(store).resolve())
    console.print("Unauthenticated offline-only service; do not expose it to a network.")
    console.print(f"[green]✓[/] serving Arena at [bold]http://{host}:{port}[/] (store: {store})")
    uvicorn.run("nirs4all_benchmarks.service.app:create_app", factory=True, host=host, port=port, reload=reload)


@app.command("provision-project")
def provision_project_cmd(root: Path = typer.Option(..., "--root"),
                          project_id: str = typer.Option(..., "--project-id")) -> None:
    """Create an isolated project store before starting the service."""
    from nirs4all_benchmarks.service.project import provision_project

    try:
        provision_project(root, project_id)
    except Exception:
        typer.echo("Project provisioning failed.", err=True)
        raise typer.Exit(code=1) from None


@app.command("serve-project")
def serve_project_cmd(root: Path = typer.Option(..., "--root"),
                      credentials: Path = typer.Option(..., "--credentials"),
                      host: str = typer.Option("127.0.0.1", "--host"),
                      port: int = typer.Option(8000, "--port")) -> None:
    """Serve provisioned projects on a loopback interface."""
    import logging

    try:
        import uvicorn
        from nirs4all_access import load_credential_store, require_secure_transport

        from nirs4all_benchmarks.service.project import create_project_app

        require_secure_transport(host, tls=False)
        application = create_project_app(root, load_credential_store(credentials))
        logger = logging.getLogger()
        logger.setLevel(logging.WARNING)
        logger.handlers.clear()
        for name in ("uvicorn", "uvicorn.error", "uvicorn.access"):
            owned = logging.getLogger(name)
            owned.handlers.clear()
            owned.setLevel(logging.WARNING)
        uvicorn.Server(uvicorn.Config(application, host=host, port=port, workers=1, reload=False,
            http="h11", ws="none", lifespan="off", log_config=None, log_level="warning", access_log=False,
            proxy_headers=False, server_header=False, h11_max_incomplete_event_size=16384,
            timeout_graceful_shutdown=10)).run()
    except Exception:
        typer.echo("Project service unavailable.", err=True)
        raise typer.Exit(code=1) from None


def main() -> None:
    app()



@app.command("convert-frozen-history")
def convert_frozen_history_cmd(
    out: Path = typer.Option(..., "--out"),
    source: Path = typer.Option(Path("data/catalog-history-2026-09"), "--source"),
) -> None:
    """Convert only the pinned immutable reference pack; never a generic v1 reader."""
    from nirs4all_benchmarks.frozen_history import FrozenHistoryRefusal, convert_frozen_history
    try:
        result = convert_frozen_history(
            source, out, progress=lambda done, total: console.print(f"Validated {done}/{total}")
        )
    except FrozenHistoryRefusal as exc:
        typer.echo(f"rejected: {exc.code}")
        raise typer.Exit(code=2) from None
    console.print(result["output"])


if __name__ == "__main__":
    main()
