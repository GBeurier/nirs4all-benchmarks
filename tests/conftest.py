"""Shared pytest fixtures."""

from __future__ import annotations

from pathlib import Path

import pytest

from nirs4all_benchmarks.fixtures import seed_store
from nirs4all_benchmarks.store import ArenaStore, Queries


@pytest.fixture
def empty_store(tmp_path: Path) -> ArenaStore:
    store = ArenaStore(tmp_path / "arena")
    yield store
    store.close()


@pytest.fixture
def seeded_root(tmp_path: Path) -> Path:
    root = tmp_path / "seeded"
    seed_store(root, collection_id="fixtures")
    return root


@pytest.fixture
def seeded_store(seeded_root: Path) -> ArenaStore:
    store = ArenaStore(seeded_root)
    yield store
    store.close()


@pytest.fixture
def queries(seeded_store: ArenaStore) -> Queries:
    return Queries(seeded_store)
