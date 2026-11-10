"""Explicit CLI requests retain publication refusals and useful validation errors."""

from __future__ import annotations

import json
import re
from pathlib import Path

import pytest
from typer import BadParameter
from typer.testing import CliRunner

from nirs4all_benchmarks.cli import app


def test_build_site_cli_refuses_result_publication_with_explicit_snapshot_files(tmp_path: Path) -> None:
    ranking = tmp_path / "rankings.json"
    ranking.write_text(json.dumps([{"method": "trueskill", "scope": "cv", "partition": "validation"}]))
    gain = tmp_path / "gains.json"
    gain.write_text(json.dumps([{"baseline_id": "A", "candidate_id": "B"}]))
    site = tmp_path / "site"
    store = tmp_path / "store"
    result = CliRunner().invoke(
        app,
        [
            "build-site",
            "--store",
            str(store),
            "--out",
            str(site),
            "--domain",
            "",
            "--ranking-requests",
            str(ranking),
            "--relative-gain-requests",
            str(gain),
        ],
    )
    assert result.exit_code != 0
    assert isinstance(result.exception, ValueError)
    assert str(result.exception) == "publication_gate_closed"
    assert not site.exists()
    assert not store.exists()


@pytest.mark.parametrize(
    ("contents", "diagnostic"),
    [
        ("{broken", "Expecting property name"),
        ("{}", "expected a JSON array"),
        ("[1]", "expected a JSON array"),
        ('[{"unknown": true}]', "unknown"),
        ('[{"partition":"unknown"}]', "partition"),
    ],
)
@pytest.mark.parametrize(("color", "columns"), [(False, "42"), (True, "42"), (True, "120")])
def test_invalid_snapshot_files_fail_before_writing_site(
    tmp_path: Path,
    contents: str,
    diagnostic: str,
    color: bool,
    columns: str,
) -> None:
    path = tmp_path / "requests.json"
    path.write_text(contents)
    site = tmp_path / "site"
    result = CliRunner().invoke(
        app,
        ["build-site", "--out", str(site), "--ranking-requests", str(path)],
        color=color,
        env={"COLUMNS": columns, "TERM": "xterm-256color"},
    )
    assert result.exit_code == 2
    # Rich may place ANSI styles inside an option name or wrap after its hyphen.
    # Check the option through Click's structured diagnostic, then independently
    # verify the useful error content remains visible in the rendered output.
    error = result.exception
    while error is not None and not isinstance(error, BadParameter):
        error = error.__context__
    assert isinstance(error, BadParameter)
    assert error.param_hint == "--ranking-requests"
    assert diagnostic in error.message
    plain = re.sub(r"\x1b\[[0-?]*[ -/]*[@-~]", "", result.output).replace("│", " ")
    rendered = " ".join(plain.split())
    assert "Invalid value for" in rendered
    assert diagnostic in rendered
    assert not site.exists()


def test_snapshot_file_limit_applies_across_both_kinds(tmp_path: Path) -> None:
    ranking = tmp_path / "rankings.json"
    ranking.write_text(json.dumps([{}] * 64))
    gain = tmp_path / "gains.json"
    gain.write_text(json.dumps([{"baseline_id": "A", "candidate_id": "B"}]))
    result = CliRunner().invoke(
        app,
        [
            "build-site",
            "--out",
            str(tmp_path / "site"),
            "--ranking-requests",
            str(ranking),
            "--relative-gain-requests",
            str(gain),
        ],
    )
    assert result.exit_code == 2
    assert "64" in result.output
    assert not (tmp_path / "site").exists()
