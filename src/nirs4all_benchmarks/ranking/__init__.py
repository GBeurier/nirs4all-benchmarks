"""Host comparison cohorts; numerical inference belongs to nirs4all-methods."""

from .quality import RankingRequest, quality_ranking
from .relative_gain import RelativeGainRequest, quality_relative_gain

__all__ = ["RankingRequest", "RelativeGainRequest", "quality_ranking", "quality_relative_gain"]
