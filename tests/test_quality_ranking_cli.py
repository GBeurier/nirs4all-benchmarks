"""Explicit CLI request files produce snapshots or useful validation errors."""
from __future__ import annotations

import json
from pathlib import Path

import pytest
from typer.testing import CliRunner

from nirs4all_benchmarks.cli import app


def test_build_site_cli_accepts_both_explicit_snapshot_request_files(tmp_path: Path, empty_store) -> None:
    ranking = tmp_path / "rankings.json"
    ranking.write_text(json.dumps([{"method": "trueskill", "scope": "cv", "partition": "validation"}]))
    gain = tmp_path / "gains.json"
    gain.write_text(json.dumps([{"baseline_id": "A", "candidate_id": "B"}]))
    site = tmp_path / "site"
    result = CliRunner().invoke(app, ["build-site", "--store", str(empty_store.root), "--out", str(site),
                                     "--domain", "", "--ranking-requests", str(ranking), "--relative-gain-requests", str(gain)])
    assert result.exit_code == 0, result.output
    bundle = json.loads((site / "data/bundle.json").read_text())
    assert len(bundle["quality_rankings"]) == len(bundle["quality_relative_gains"]) == 1
    assert bundle["quality_rankings"][0]["request"]["method"] == "trueskill"
    assert bundle["quality_relative_gains"][0]["request"]["baseline_id"] == "A"
    assert bundle["quality_rankings"][0]["state"] == bundle["quality_relative_gains"][0]["state"] == "unavailable"


@pytest.mark.parametrize("contents", ["{broken", "{}", "[1]", '[{"unknown": true}]', '[{"partition":"unknown"}]'])
def test_invalid_snapshot_files_fail_before_writing_site(tmp_path: Path, contents: str) -> None:
    path = tmp_path / "requests.json"
    path.write_text(contents)
    site = tmp_path / "site"
    result = CliRunner().invoke(app, ["build-site", "--out", str(site), "--ranking-requests", str(path)])
    assert result.exit_code == 2
    assert "ranking-requests" in result.output
    assert not site.exists()


def test_snapshot_file_limit_applies_across_both_kinds(tmp_path: Path) -> None:
    ranking = tmp_path / "rankings.json"
    ranking.write_text(json.dumps([{}] * 64))
    gain = tmp_path / "gains.json"
    gain.write_text(json.dumps([{"baseline_id": "A", "candidate_id": "B"}]))
    result = CliRunner().invoke(app, ["build-site", "--out", str(tmp_path / "site"), "--ranking-requests", str(ranking),
                                     "--relative-gain-requests", str(gain)])
    assert result.exit_code == 2
    assert "64" in result.output
    assert not (tmp_path / "site").exists()
