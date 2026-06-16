"""Producer adapters — two producers, one contract (PERSISTENCE_FORMATS.md §6.1).

Both adapters emit the same :class:`~nirs4all_benchmarks.contract.ArenaRunExport`;
the Arena never unifies the source containers (DESIGN.md §5.2). Adapter A
(nirs4all workspace) is live today; Adapter B (dag-ml bundle) is the future engine.
``.n4a`` recipe extraction strips weights and recovers the pipeline identity.

These adapters read the *documented* producer formats (PERSISTENCE_FORMATS.md §2/§3)
and never import the producer libraries — honoring "touch no other library". They
degrade gracefully: nirs4all residuals are emitted ``positional`` (no stable sample
id today, §4.4); dag-ml residuals are emitted ``sample_id``-keyed.
"""

from __future__ import annotations

from nirs4all_benchmarks.adapters.dagml_bundle import bundle_to_export
from nirs4all_benchmarks.adapters.n4a_bundle import extract_n4a_recipe, n4a_pipeline_identity
from nirs4all_benchmarks.adapters.nirs4all_workspace import WorkspaceAdapter, workspace_to_exports

__all__ = [
    "WorkspaceAdapter",
    "bundle_to_export",
    "extract_n4a_recipe",
    "n4a_pipeline_identity",
    "workspace_to_exports",
]
