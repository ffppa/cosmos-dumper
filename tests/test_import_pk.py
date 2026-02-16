import os
import json
import pytest
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch
from cosmos_dumper.cli import import_file_task

@pytest.mark.asyncio
async def test_import_with_different_partition_key(tmp_path):
    # Setup mock data
    container_name = "test-container"
    file_path = tmp_path / f"{container_name}_export.jsonl"
    
    items = [
        {"id": "1", "pk": "val1", "data": "foo"},
        {"id": "2", "pk": "val2", "data": "bar"},
        {"id": "3", "pk": "val3", "data": "poo"},
        {"id": "4", "pk": "val4", "data": "spo"}
    ]
    
    with open(file_path, "w") as f:
        for item in items:
            f.write(json.dumps(item) + "\n")
            
    args = SimpleNamespace(
        url="https://example.com",
        key="secret",
        db="testdb",
        concurrency=1,
        shuffle=False,
        mongo=False
    )
    
    # Mock CosmosClient
    with patch("cosmos_dumper.cli.CosmosClient") as mock_client_cls:
        mock_client = MagicMock()
        mock_client_cls.return_value.__aenter__.return_value = mock_client
        
        mock_db = MagicMock()
        mock_client.get_database_client.return_value = mock_db
        
        mock_container = MagicMock()
        mock_db.get_container_client.return_value = mock_container
        
        mock_container.get_container_properties = AsyncMock(return_value={
            "partitionKey": {"paths": ["/pk"], "kind": "Hash"},
            "indexingPolicy": {"indexingMode": "consistent"}
        })
        
        mock_db.create_container_if_not_exists = AsyncMock()
        mock_container.replace_container_properties = AsyncMock()
        
        mock_container.upsert_item = AsyncMock()
        
        await import_file_task(args, container_name, str(file_path))
        
        assert mock_container.upsert_item.call_count == 4
        
        calls = mock_container.upsert_item.call_args_list

        assert calls[0].args[0] == items[0]
        assert calls[1].args[0] == items[1]

    file_path_missing = tmp_path / "missing_pk.jsonl"
    item_missing = {"id": "3", "wrong_pk": "val3", "data": "baz"}
    with open(file_path_missing, "w") as f:
        f.write(json.dumps(item_missing) + "\n")

    with patch("cosmos_dumper.cli.CosmosClient") as mock_client_cls:
        mock_client = MagicMock()
        mock_client_cls.return_value.__aenter__.return_value = mock_client
        mock_db = MagicMock()
        mock_client.get_database_client.return_value = mock_db
        mock_container = MagicMock()
        mock_db.get_container_client.return_value = mock_container
        mock_container.get_container_properties = AsyncMock(return_value={
            "partitionKey": {"paths": ["/pk"], "kind": "Hash"},
            "indexingPolicy": {"indexingMode": "consistent"}
        })
        mock_db.create_container_if_not_exists = AsyncMock()
        mock_container.replace_container_properties = AsyncMock()
        
        mock_container.upsert_item = AsyncMock(side_effect=[Exception("Partition key not found")])
        
        await import_file_task(args, container_name, str(file_path_missing))
        
        assert mock_container.upsert_item.call_count == 1

