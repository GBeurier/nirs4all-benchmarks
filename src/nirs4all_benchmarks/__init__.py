"""nirs4all-benchmarks — *the Arena*.

A reproducible, scored, **weights-free** benchmark store and dataviz service for
NIRS pipelines produced by ``nirs4all`` and/or ``dag-ml``.

The Arena stores identity cards, canonical pipeline DAGs, versioned scores, and
sample-keyed residuals — never fitted artifacts. It ingests a single, versioned,
content-addressed bundle (the :class:`~nirs4all_benchmarks.contract.ArenaRunExport`)
and serves meta-analysis queries and dataviz over the
``model × pipeline × split × cv × rng × refit × dataset`` space.

See ``DESIGN.md``, ``DATA_MANAGEMENT.md`` and ``PERSISTENCE_FORMATS.md`` for the
conceptual model, the ingestion contract, and the producer-format reference.
"""

from __future__ import annotations

from nirs4all_benchmarks.version import __version__

# Arena store / export contract versions. These are *frozen* numbers — bumping
# one signals an incompatible on-disk or wire change (see DATA_MANAGEMENT.md §8).
ARENA_SCHEMA_VERSION = 1
ARENA_EXPORT_SCHEMA_VERSION = 1
RESIDUALS_SCHEMA_VERSION = 1

__all__ = [
    "ARENA_EXPORT_SCHEMA_VERSION",
    "ARENA_SCHEMA_VERSION",
    "RESIDUALS_SCHEMA_VERSION",
    "__version__",
]
