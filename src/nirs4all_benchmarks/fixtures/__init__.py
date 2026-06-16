"""Deterministic fixtures — 2 datasets × several DAGs with synthetic residuals.

Used by the test suite (frozen ``run_condition_hash`` regression contract,
DATA_MANAGEMENT.md §9 step 5) and by ``n4a-benchmarks fixtures`` to seed a store
with realistic data for the dataviz demo. The synthetic skill of each pipeline is
shaped so effect plots are meaningful (PLS ``n_components`` U-curve, branch/merge
beating single branches, etc.).
"""

from __future__ import annotations

from nirs4all_benchmarks.fixtures.generate import (
    generate_fixture_exports,
    seed_store,
    write_fixture_exports,
)

__all__ = ["generate_fixture_exports", "seed_store", "write_fixture_exports"]
