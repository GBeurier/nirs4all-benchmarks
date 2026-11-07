"""Backend publication gate: refusal must not create or overwrite any output."""
from pathlib import Path

import pytest

from nirs4all_benchmarks.site_build import build_site
from nirs4all_benchmarks.store import ArenaStore


@pytest.mark.parametrize("existing", [False, True])
def test_build_site_refuses_before_any_output_or_store_mutation(tmp_path: Path, existing):
    root, out = tmp_path/"store", tmp_path/"site"
    if existing:
        with ArenaStore(root):
            pass
        out.mkdir()
        (out/"existing-private-record.json").write_text('{"private": true}')
    before = {p.relative_to(tmp_path):p.read_bytes() for p in tmp_path.rglob("*") if p.is_file()}
    with pytest.raises(ValueError,match="publication_gate_closed"):
        build_site(root,out,domain=None)
    assert {p.relative_to(tmp_path):p.read_bytes() for p in tmp_path.rglob("*") if p.is_file()}==before
    assert root.exists()==out.exists()==existing
