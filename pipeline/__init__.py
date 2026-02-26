"""Multi-source data aggregation & normalization pipeline."""
from .pipeline import Pipeline
from .models import NormalizedRecord, SourceConfig, PipelineStats

__all__ = ["Pipeline", "NormalizedRecord", "SourceConfig", "PipelineStats"]
