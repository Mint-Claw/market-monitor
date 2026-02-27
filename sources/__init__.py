"""
Source plugins for the market-monitor aggregation pipeline.

Each source type is a subclass of BaseSource and is auto-registered
via the SOURCE_REGISTRY.  To add a new source:

    1. Create ``sources/my_source.py``
    2. Subclass ``BaseSource``, implement ``fetch()``, decorate with
       ``@register_source("my_type")``
    3. Import it here (or let the pipeline auto-discover it)
    4. Add entries with ``"type": "my_type"`` in ``config.json``

No other code changes required.
"""

from sources.base import BaseSource, register_source, get_source_class, SOURCE_REGISTRY

# Import every built-in source so its @register_source decorator runs.
from sources.rest_source import RestSource          # noqa: F401
from sources.csv_source import CsvSource            # noqa: F401
from sources.websocket_source import WebSocketSource  # noqa: F401
from sources.graphql_source import GraphQLSource    # noqa: F401
from sources.file_source import FileSource          # noqa: F401

__all__ = [
    "BaseSource",
    "register_source",
    "get_source_class",
    "SOURCE_REGISTRY",
]
