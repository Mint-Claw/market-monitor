"""Main pipeline orchestration."""
import asyncio
import json
import time
from typing import List, Dict, Any
from pathlib import Path
from .config import PipelineConfig, SourceConfig
from .fetchers import get_fetcher

class Pipeline:
    """Async data aggregation pipeline."""
    
    def __init__(self, config: PipelineConfig):
        self.config = config
        self.results: List[Dict[str, Any]] = []
        self.stats = {
            "total_records": 0,
            "sources_succeeded": 0,
            "sources_failed": 0,
            "duration_seconds": 0,
            "errors": []
        }
    
    async def run(self) -> Dict[str, Any]:
        """Execute the pipeline."""
        start_time = time.time()
        
        # Create semaphore for concurrency control
        semaphore = asyncio.Semaphore(self.config.max_concurrent)
        
        # Fetch from all sources concurrently
        tasks = [
            self._fetch_with_retry(source, semaphore)
            for source in self.config.sources
        ]
        
        results = await asyncio.gather(*tasks, return_exceptions=True)
        
        # Collect and normalize results
        all_records = []
        for i, result in enumerate(results):
            source_name = self.config.sources[i].name
            
            if isinstance(result, Exception):
                self.stats["sources_failed"] += 1
                self.stats["errors"].append({
                    "source": source_name,
                    "error": str(result)
                })
            else:
                self.stats["sources_succeeded"] += 1
                all_records.extend(result)
        
        # Sort by timestamp
        all_records.sort(key=lambda x: x.get("timestamp", ""))
        
        self.results = all_records
        self.stats["total_records"] = len(all_records)
        self.stats["duration_seconds"] = round(time.time() - start_time, 2)
        
        # Save output
        output = {
            "data": self.results,
            "stats": self.stats
        }
        
        await self._save_output(output)
        
        return output
    
    async def _fetch_with_retry(
        self,
        source: SourceConfig,
        semaphore: asyncio.Semaphore
    ) -> List[Dict[str, Any]]:
        """Fetch from a source with retry logic."""
        async with semaphore:
            for attempt in range(self.config.retry_attempts):
                try:
                    fetcher = get_fetcher(source)
                    raw_data = await fetcher.fetch()
                    normalized = fetcher.normalize(raw_data)
                    return normalized
                except Exception as e:
                    if attempt == self.config.retry_attempts - 1:
                        raise
                    await asyncio.sleep(0.5 * (attempt + 1))
            
            return []
    
    async def _save_output(self, output: Dict[str, Any]) -> None:
        """Save output to JSON file."""
        path = Path(self.config.output_path)
        path.parent.mkdir(parents=True, exist_ok=True)
        
        content = json.dumps(output, indent=2, ensure_ascii=False)
        await asyncio.to_thread(path.write_text, content)


async def run_pipeline(config: PipelineConfig) -> Dict[str, Any]:
    """Convenience function to run pipeline."""
    pipeline = Pipeline(config)
    return await pipeline.run()
