"""The public status artifact must have no path to scientific or private data."""

from __future__ import annotations

from html.parser import HTMLParser
from pathlib import Path

import pytest
from typer.testing import CliRunner

from nirs4all_benchmarks import site_build
from nirs4all_benchmarks.cli import app


class _Page(HTMLParser):
    def __init__(self) -> None:
        super().__init__()
        self.tags: list[tuple[str, dict[str, str | None]]] = []

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        self.tags.append((tag, dict(attrs)))


@pytest.mark.parametrize("existing_store", [False, True])
@pytest.mark.parametrize("domain", [None, "benchmarks.nirs4all.org"])
def test_unavailable_artifact_has_no_store_or_executable_data_path(
    tmp_path: Path, monkeypatch, existing_store: bool, domain: str | None,
) -> None:
    store, out = tmp_path / "private-store", tmp_path / "site"
    if existing_store:
        store.mkdir()
        (store / "private.json").write_text('{"label":"private sentinel","score":42}')
    before = {p.relative_to(store): p.read_bytes() for p in store.rglob("*") if p.is_file()}

    def forbidden(*args, **kwargs):
        pytest.fail("unavailable-results must not open a store or query publication evidence")

    monkeypatch.setattr(site_build, "ArenaStore", forbidden)
    monkeypatch.setattr(site_build, "_publication_evidence", forbidden)
    summary = site_build.build_site(store, out, domain=domain, unavailable_results=True)
    assert summary["state"] == "unavailable"
    assert summary["executions"] == summary["metric_rows"] == 0
    assert store.exists() == existing_store
    assert {p.relative_to(store): p.read_bytes() for p in store.rglob("*") if p.is_file()} == before
    expected = {"index.html", "styles.css", "brand/icon.svg", ".nojekyll"}
    if domain:
        expected.add("CNAME")
        assert (out / "CNAME").read_text() == domain + "\n"
    assert {p.relative_to(out).as_posix() for p in out.rglob("*") if p.is_file()} == expected
    page = _Page()
    html = (out / "index.html").read_text()
    page.feed(html)
    assert "Benchmark results are unavailable" in html
    assert "private sentinel" not in html
    assert not {tag for tag, _ in page.tags} & {"script", "form", "input", "iframe", "object", "embed"}
    for tag, attrs in page.tags:
        assert not any(key.startswith("on") for key in attrs)
        if tag in {"link", "img"}:
            assert attrs.get("src", attrs.get("href")) in {"/brand/icon.svg", "/styles.css"}
    # The two reused presentation assets cannot load a hidden runtime or data.
    assert "@import" not in (out / "styles.css").read_text()
    assert "url(" not in (out / "styles.css").read_text()
    svg = (out / "brand/icon.svg").read_text()
    assert "<script" not in svg and "href=" not in svg


@pytest.mark.parametrize("kind", ["nonempty", "symlink", "file"])
def test_unavailable_refuses_stale_or_aliased_output(tmp_path: Path, kind: str) -> None:
    store, out = tmp_path / "store", tmp_path / "site"
    if kind == "file":
        out.write_text("preserve")
    else:
        target = tmp_path / "old-site"
        target.mkdir()
        (target / "bundle.json").write_text('{"private":true}')
        if kind == "symlink":
            out.symlink_to(target, target_is_directory=True)
        else:
            out = target
    before = {p.relative_to(tmp_path): p.read_bytes() for p in tmp_path.rglob("*") if p.is_file()}
    with pytest.raises(ValueError, match="site output must be an empty directory"):
        site_build.build_site(store, out, unavailable_results=True)
    assert {p.relative_to(tmp_path): p.read_bytes() for p in tmp_path.rglob("*") if p.is_file()} == before
    assert not store.exists()


@pytest.mark.parametrize("key", ["ranking_requests", "relative_gain_requests", "component_analysis_requests"])
def test_unavailable_forbids_analysis_even_with_empty_requests(tmp_path: Path, key: str) -> None:
    out = tmp_path / "site"
    with pytest.raises(ValueError, match="unavailable_results_forbids_analysis_requests"):
        site_build.build_site(tmp_path / "store", out, unavailable_results=True, **{key: []})
    assert not out.exists()


def test_cli_builds_unavailable_without_creating_default_store(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.chdir(tmp_path)
    result = CliRunner().invoke(app, ["build-site", "--unavailable-results", "--out", "site", "--domain", ""])
    assert result.exit_code == 0, result.output
    assert "no scientific records published" in result.output
    assert (tmp_path / "site/index.html").is_file()
    assert not (tmp_path / "arena-store").exists()
    assert not (tmp_path / "site/data").exists()


@pytest.mark.parametrize("option", ["--ranking-requests", "--relative-gain-requests", "--component-analysis-requests"])
def test_cli_forbids_status_page_analysis_files(tmp_path: Path, option: str) -> None:
    requests = tmp_path / "requests.json"
    requests.write_text("[]")
    out = tmp_path / "site"
    result = CliRunner().invoke(app, ["build-site", "--unavailable-results", "--out", str(out), option, str(requests)])
    assert result.exit_code == 2
    assert not out.exists()
