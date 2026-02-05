import asyncio
import json
import os
import pytest
from unittest.mock import AsyncMock, MagicMock, patch
from cosmos_dumper.cli import import_cmd

@pytest.mark.asyncio
async def test_import_with_concurrency(tmp_path):
    """
    Reproduction test for concurrency in import_cmd.
    Ensures that items are processed and concurrency is respected.
    """
    # Create a dummy export file with many items to test concurrency logic
    export_dir = tmp_path / "export_dir"
    export_dir.mkdir()
    export_file = export_dir / "test_container_export.jsonl"
    
    items = [{"id": f"item{i}", "pk": f"pk{i}"} for i in range(100)]
    content = "\n".join([json.dumps(item) for item in items])
    export_file.write_text(content)

    # Mock arguments
    args = MagicMock()
    args.url = "https://example.documents.azure.com:443/"
    args.key = "master_key"
    args.db = "test_db"
    args.path = str(export_file)
    args.container = None
    args.from_container = None
    args.concurrency = 10
    args.shuffle = True

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
        
        # Mock get_container_properties for indexing policy restoration
        mock_container_client.get_container_properties = AsyncMock(return_value={
            "indexingPolicy": {"indexingMode": "consistent"}
        })
        mock_db.replace_container = AsyncMock()

        await import_cmd(args)

        # Verify all items were imported
        assert mock_container_client.upsert_item.call_count == 100
        mock_db.create_container_if_not_exists.assert_called()
        
        # Verify indexing policy was set to none and then restored
        create_call_args = mock_db.create_container_if_not_exists.call_args
        assert create_call_args.kwargs["indexing_policy"]["indexingMode"] == "none"
        
        assert mock_db.replace_container.called
        replace_call_args = mock_db.replace_container.call_args
        assert replace_call_args.args[0] == "test_container"
        assert replace_call_args.args[1]["indexingMode"] == "consistent"
