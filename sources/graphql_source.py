"""GraphQL source — sends a query and extracts records from the response."""

from __future__ import annotations

import asyncio
import logging
from typing import Any

import aiohttp

from sources.base import BaseSource, register_source

logger = logging.getLogger(__name__)


@register_source("graphql")
class GraphQLSource(BaseSource):
    """Execute a GraphQL query via HTTP POST and return records.
    
    Supports pagination via 'paginate' config:
    - paginate.enabled: true to enable
    - paginate.page_var: variable name for page number (default: "page")
    - paginate.max_pages: maximum pages to fetch
    """

    async def _fetch_page(
        self, session: aiohttp.ClientSession, url: str, query: str, 
        headers: dict, variables: dict | None, data_path: str
    ) -> tuple[list[dict], bool]:
        """Fetch a single page and return (records, has_more)."""
        payload = {"query": query}
        if variables:
            payload["variables"] = variables
            
        async with session.post(
            url,
            json=payload,
            headers=headers,
            timeout=aiohttp.ClientTimeout(total=15),
        ) as resp:
            resp.raise_for_status()
            body = await resp.json()

        # Navigate into nested response using dot-separated data_path
        data = body.get("data", body)
        if data_path:
            for key in data_path.split("."):
                if isinstance(data, dict):
                    data = data.get(key, data)
                else:
                    break

        # Check for pagination info (common patterns)
        has_more = False
        records = []
        
        if isinstance(data, dict):
            # Handle paginated response with info.next or results
            if "results" in data:
                records = data["results"]
                info = data.get("info", {})
                has_more = info.get("next") is not None
            else:
                records = [data]
        elif isinstance(data, list):
            records = data
            
        return records, has_more

    async def fetch(self) -> list[dict[str, Any]]:
        url = self.config["url"]
        query = self.config["query"]
        data_path = self.config.get("data_path", "")
        headers = self.config.get("headers", {"Content-Type": "application/json"})
        max_records = self.config.get("max_records")
        
        # Pagination config
        paginate = self.config.get("paginate", {})
        paginate_enabled = paginate.get("enabled", False)
        page_var = paginate.get("page_var", "page")
        max_pages = paginate.get("max_pages", 10)

        logger.info("GQL   POST %s (paginate=%s)", url, paginate_enabled)
        
        all_records = []
        async with aiohttp.ClientSession() as session:
            if paginate_enabled:
                # Fetch multiple pages
                for page in range(1, max_pages + 1):
                    variables = {page_var: page}
                    records, has_more = await self._fetch_page(
                        session, url, query, headers, variables, data_path
                    )
                    all_records.extend(records)
                    logger.info("GQL   %s page %d -> %d records (total: %d)", 
                               self.name, page, len(records), len(all_records))
                    
                    if max_records and len(all_records) >= max_records:
                        break
                    if not has_more:
                        break
                    # Small delay between pages
                    await asyncio.sleep(0.1)
            else:
                # Single query (original behavior)
                records, _ = await self._fetch_page(
                    session, url, query, headers, None, data_path
                )
                all_records = records

        if max_records:
            all_records = all_records[:max_records]

        logger.info("GQL   %s -> %d records", self.name, len(all_records))
        return all_records
