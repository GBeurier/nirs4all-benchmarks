"""Host comparison cohorts; numerical inference belongs to nirs4all-methods."""

from .component_analysis import AnalysisFactor, ComponentAnalysisRequest, component_analysis
from .quality import RankingRequest, quality_ranking
from .relative_gain import RelativeGainRequest, quality_relative_gain

__all__ = [
    "AnalysisFactor",
    "ComponentAnalysisRequest",
    "RankingRequest",
    "RelativeGainRequest",
    "component_analysis",
    "quality_ranking",
    "quality_relative_gain",
]
