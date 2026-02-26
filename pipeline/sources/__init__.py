"""Data source implementations."""
from .base import DataSource
from .rest import RESTSource
from .csv_source import CSVSource
from .graphql import GraphQLSource
from .file import FileSource

try:
    from .websocket import WebSocketSource
except ImportError:  # pragma: no cover
    WebSocketSource = None  # type: ignore[assignment,misc]

__all__ = [
    "DataSource",
    "RESTSource",
    "CSVSource",
    "WebSocketSource",
    "GraphQLSource",
    "FileSource",
]
