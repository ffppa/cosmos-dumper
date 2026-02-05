import asyncio
import json
import os
import pytest
from unittest.mock import AsyncMock, MagicMock, patch
from cosmos_dumper.cli import export_container_task


@pytest.mark.asyncio
async def test_export_container_split_json(tmp_path):
    # Mock arguments
    args = MagicMock()
    args.url = "https://localhost:8081"
    args.key = "C2y6yDjf5/R+ob0N8A7Cgv30VRDJIWEHLM+4QDU5DE2nQ9nDuVTqobD4b8mGGyPMbIZnqyMsEcaGQy67XIw/Jw=="
    args.db = "testdb"
    args.jsonl = False
    args.max_file_size = (
        0.0000001  # Very small size to trigger split (approx 100 bytes)
    )

    container_properties = {"id": "testcontainer"}
    export_folder = str(tmp_path)

    # Mock items: each item is approx 50 bytes when dumped
    items = [{"id": f"item_{i}", "data": "x" * 20} for i in range(10)]

    # Mock CosmosClient
    with patch("cosmos_dumper.cli.CosmosClient") as mock_client_class:
        mock_client = MagicMock()
        mock_client_class.return_value.__aenter__.return_value = mock_client
        # These are synchronous calls returning clients
        mock_db = MagicMock()
        mock_client.get_database_client.return_value = mock_db
        mock_container = MagicMock()
        mock_db.get_container_client.return_value = mock_container

        # Mock read_feed_ranges to return an async iterable
        async def mock_read_feed_ranges():
            yield "range1"

        mock_container.read_feed_ranges.side_effect = mock_read_feed_ranges

        # Mock query_items
        mock_query_iterable = MagicMock()
        mock_container.query_items.return_value = mock_query_iterable

        async def mock_by_page():
            # Create an async generator that yields one page with all items
            async def page_gen():
                for item in items:
                    yield item

            yield page_gen()

        mock_query_iterable.by_page.side_effect = mock_by_page

        # Run the export task
        await export_container_task(args, container_properties, export_folder)

        # Check created files
        files = sorted(os.listdir(export_folder))
        # We expect multiple files because max_file_size is very small
        # Naming: testcontainer_export_1.json, testcontainer_export_2.json, ...
        export_files = [f for f in files if f.startswith("testcontainer_export_")]
        assert len(export_files) > 1

        # Verify content of the first file
        with open(os.path.join(export_folder, export_files[0]), "r") as f:
            content = f.read()
            assert content.startswith("[")
            assert content.endswith("]")
            data = json.loads(content)
            assert len(data) > 0

        # Verify content of the last file
        with open(os.path.join(export_folder, export_files[-1]), "r") as f:
            content = f.read()
            assert content.startswith("[")
            assert content.endswith("]")
            data = json.loads(content)
            assert len(data) > 0


@pytest.mark.asyncio
async def test_export_container_split_jsonl(tmp_path):
    # Mock arguments
    args = MagicMock()
    args.url = "https://localhost:8081"
    args.key = "fake_key"
    args.db = "testdb"
    args.jsonl = True
    args.max_file_size = 0.0000001  # small size to trigger split

    container_properties = {"id": "testcontainer"}
    export_folder = str(tmp_path)

    items = [{"id": f"item_{i}", "data": "y" * 30} for i in range(10)]

    with patch("cosmos_dumper.cli.CosmosClient") as mock_client_class:
        mock_client = MagicMock()
        mock_client_class.return_value.__aenter__.return_value = mock_client
        mock_db = MagicMock()
        mock_client.get_database_client.return_value = mock_db
        mock_container = MagicMock()
        mock_db.get_container_client.return_value = mock_container

        async def mock_read_feed_ranges():
            yield "range1"

        mock_container.read_feed_ranges.side_effect = mock_read_feed_ranges

        mock_query_iterable = MagicMock()
        mock_container.query_items.return_value = mock_query_iterable

        async def mock_by_page():
            async def page_gen():
                for item in items:
                    yield item

            yield page_gen()

        mock_query_iterable.by_page.side_effect = mock_by_page

        await export_container_task(args, container_properties, export_folder)

        files = sorted(os.listdir(export_folder))
        export_files = [f for f in files if f.startswith("testcontainer_export_")]
        assert len(export_files) > 1

        # Verify JSONL format
        for filename in export_files:
            with open(os.path.join(export_folder, filename), "r") as f:
                lines = f.readlines()
                for line in lines:
                    if line.strip():
                        json.loads(line)
