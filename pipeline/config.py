"""Pipeline configuration schema."""
from typing import List, Dict, Any, Literal
from dataclasses import dataclass, field

SourceType = Literal["rest", "csv", "websocket", "graphql", "file"]

@dataclass
class SourceConfig:
    """Configuration for a single data source."""
    name: str
    type: SourceType
    url: str
    timeout: float = 10.0
    params: Dict[str, Any] = field(default_factory=dict)
    headers: Dict[str, str] = field(default_factory=dict)
    field_mapping: Dict[str, str] = field(default_factory=dict)  # bonus feature

@dataclass
class PipelineConfig:
    """Main pipeline configuration."""
    sources: List[SourceConfig]
    output_path: str = "output.json"
    max_concurrent: int = 10
    retry_attempts: int = 2
    incremental: bool = False  # bonus feature
