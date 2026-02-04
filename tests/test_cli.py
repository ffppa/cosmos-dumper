import asyncio
import json
import os
import pytest
from unittest.mock import AsyncMock, MagicMock, patch
from cosmos_dumper.cli import export_cmd, import_cmd

async def _async_gen(items):
    """Helper to create an async generator from a list."""
    for item in items:
        yield item

@pytest.mark.asyncio
async def test_export_cmd_basic(tmp_path):
    """
    Test the basic functionality of the export_cmd.
    It mocks the CosmosClient and its methods to simulate an export process.
    """
    # Mock arguments
    args = MagicMock()
    args.url = "https://example.documents.azure.com:443/"
    args.key = "master_key"
    args.db = "test_db"
    args.container = None
    args.jsonl = True

    # Mock CosmosClient and hierarchy
    with patch("cosmos_dumper.cli.CosmosClient", new_callable=MagicMock) as MockClient:
        mock_client_instance = MockClient.return_value
        mock_client_instance.__aenter__.return_value = mock_client_instance
        mock_db = MagicMock()
        mock_client_instance.get_database_client.return_value = mock_db
        
        # Mock containers list
        mock_container_properties = {"id": "test_container"}
        mock_db.list_containers.side_effect = lambda: _async_gen([mock_container_properties])
        
        mock_container_client = MagicMock()
        mock_db.get_container_client.return_value = mock_container_client
        
        # Mock feed ranges
        mock_container_client.read_feed_ranges.return_value = _async_gen([{"id": "range1"}])
        
        # Mock query items (by_page)
        mock_query_iterable = MagicMock()
        mock_page = MagicMock()
        
        async def mock_page_aiter():
            for item in [{"id": "item1"}, {"id": "item2"}]:
                yield item
        mock_page.__aiter__.side_effect = mock_page_aiter
        
        async def mock_by_page():
            yield mock_page
        mock_query_iterable.by_page.side_effect = mock_by_page
        mock_container_client.query_items.return_value = mock_query_iterable

        # Mock os.makedirs and time.time
        with patch("os.makedirs"), patch("time.time", return_value=1000):
             await export_cmd(args)

        # Verify calls
        mock_client_instance.get_database_client.assert_called_with("test_db")
        mock_db.get_container_client.assert_called_with("test_container")
        mock_container_client.read_feed_ranges.assert_called()

@pytest.mark.asyncio
async def test_export_parallelism_efficiency():
    """
    Test that the export process parallelizes across multiple feed ranges.
    This ensures that for a single collection, multiple workers are spawned
    based on the number of partition ranges (Feed Ranges).
    """
    args = MagicMock()
    args.url = "https://example.documents.azure.com:443/"
    args.key = "master_key"
    args.db = "test_db"
    args.container = "parallel_container"
    args.jsonl = True

    with patch("cosmos_dumper.cli.CosmosClient", new_callable=MagicMock) as MockClient:
        mock_client_instance = MockClient.return_value
        mock_client_instance.__aenter__.return_value = mock_client_instance
        mock_db = MagicMock()
        mock_client_instance.get_database_client.return_value = mock_db
        
        # Mock container with 10 partition ranges to test parallelism
        ranges = [{"id": f"range{i}"} for i in range(10)]
        
        mock_container_client = MagicMock()
        mock_container_client.read_feed_ranges.return_value = _async_gen(ranges)
        mock_db.get_container_client.return_value = mock_container_client
        mock_db.list_containers.side_effect = lambda: _async_gen([{"id": "parallel_container"}])

        # Mock query_items to simulate data fetching for each range
        mock_query_iterable = MagicMock()
        async def mock_by_page():
            mock_page = MagicMock()
            mock_page.__aiter__.side_effect = lambda: _async_gen([{"id": "data"}])
            yield mock_page
            
        mock_query_iterable.by_page.side_effect = mock_by_page
        mock_container_client.query_items.return_value = mock_query_iterable

        # Execute export
        with patch("os.makedirs"), patch("time.time", return_value=1000):
            await export_cmd(args)

        # Efficiency Check: 
        # 1. Verify that query_items was called exactly 10 times (once for each feed_range)
        # This confirms that the logic correctly parallelizes the work per partition range.
        assert mock_container_client.query_items.call_count == 10
        
        # 2. Verify each call was passed a different feed_range
        called_ranges = [call.kwargs.get('feed_range') for call in mock_container_client.query_items.call_args_list]
        for r in ranges:
            assert r in called_ranges

@pytest.mark.asyncio
async def test_import_cmd_basic(tmp_path):
    """
    Test the basic functionality of the import_cmd.
    It mocks the CosmosClient and its methods to simulate an import process.
    """
    # Create a dummy export file
    export_dir = tmp_path / "export_dir"
    export_dir.mkdir()
    # Ensure the file matches the expected pattern *_export.jsonl
    export_file = export_dir / "test_container_export.jsonl"
    export_file.write_text(json.dumps({"id": "item1"}) + "\n" + json.dumps({"id": "item2"}))

    # Mock arguments
    args = MagicMock()
    args.url = "https://example.documents.azure.com:443/"
    args.key = "master_key"
    args.db = "test_db"
    args.path = str(export_dir)
    args.container = None
    args.from_container = None

    # Mock CosmosClient and hierarchy
    with patch("cosmos_dumper.cli.CosmosClient", new_callable=MagicMock) as MockClient:
        mock_client_instance = MockClient.return_value
        mock_client_instance.__aenter__.return_value = mock_client_instance
        mock_db = MagicMock()
        mock_client_instance.get_database_client.return_value = mock_db
        mock_container_client = MagicMock()
        mock_db.get_container_client.return_value = mock_container_client
        
        # Mock upsert_item
        mock_container_client.upsert_item = AsyncMock()
        
        # Mock create_container_if_not_exists
        mock_db.create_container_if_not_exists = AsyncMock()

        await import_cmd(args)

        # Verify calls
        mock_db.get_container_client.assert_called_with("test_container")
        assert mock_container_client.upsert_item.call_count == 2
        mock_db.create_container_if_not_exists.assert_called()
