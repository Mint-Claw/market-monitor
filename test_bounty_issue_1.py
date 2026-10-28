import pytest
from unittest.mock import patch

@pytest.mark.asyncio
async def test_issue_resolution():
    from solution import IssueResolutionPipeline, BountyItemModel
    pipeline = IssueResolutionPipeline()
    with patch("httpx.AsyncClient.get") as mock_get:
        mock_get.return_value.status_code = 200
        mock_get.return_value.json.return_value = [{"id": 1, "title": "Test Issue", "html_url": "https://github.com/Mint-Claw/market-monitor/issues/1"}]
        items = await pipeline.execute_task("mock/issues")
        assert len(items) == 1
        assert isinstance(items[0], BountyItemModel)
        assert items[0].title == "Test Issue"
