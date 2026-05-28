import json
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from cosmos_dumper.cli import import_file_task


class Fake429Error(Exception):
    def __init__(self, message="TooManyRequests"):
        super().__init__(message)
        self.status_code = 429
        self.retry_after = 0


@pytest.mark.asyncio
async def test_import_retries_429_and_succeeds(tmp_path):
    container_name = "test-container"
    file_path = tmp_path / f"{container_name}_export.jsonl"
    file_path.write_text(json.dumps({"id": "1", "data": "ok"}) + "\n")

    args = SimpleNamespace(
        url="https://example.com",
        key="secret",
        db="testdb",
        concurrency=1,
        shuffle=False,
        mongo=False,
        max_retries=3,
        retry_base_delay=0.01,
        retry_max_delay=0.02,
        failed_items_path=None,
    )

    with patch("cosmos_dumper.cli.CosmosClient") as mock_client_cls, patch(
        "cosmos_dumper.cli.asyncio.sleep", new_callable=AsyncMock
    ) as mock_sleep:
        mock_client = MagicMock()
        mock_client_cls.return_value.__aenter__.return_value = mock_client

        mock_db = MagicMock()
        mock_client.get_database_client.return_value = mock_db

        mock_container = MagicMock()
        mock_db.get_container_client.return_value = mock_container

        mock_container.get_container_properties = AsyncMock(
            return_value={"indexingPolicy": {"indexingMode": "consistent"}}
        )
        mock_db.create_container_if_not_exists = AsyncMock()
        mock_db.replace_container = AsyncMock()

        mock_container.upsert_item = AsyncMock(side_effect=[Fake429Error(), None])

        await import_file_task(args, container_name, str(file_path))

        assert mock_container.upsert_item.call_count == 2
        assert mock_sleep.await_count == 1


@pytest.mark.asyncio
async def test_import_persists_failed_items_after_retry_exhaustion(tmp_path):
    container_name = "test-container"
    file_path = tmp_path / f"{container_name}_export.jsonl"
    file_path.write_text(json.dumps({"id": "1", "data": "ko"}) + "\n")
    failed_items_path = tmp_path / "failed_items.jsonl"

    args = SimpleNamespace(
        url="https://example.com",
        key="secret",
        db="testdb",
        concurrency=1,
        shuffle=False,
        mongo=False,
        max_retries=1,
        retry_base_delay=0.01,
        retry_max_delay=0.02,
        failed_items_path=str(failed_items_path),
    )

    with patch("cosmos_dumper.cli.CosmosClient") as mock_client_cls, patch(
        "cosmos_dumper.cli.asyncio.sleep", new_callable=AsyncMock
    ) as mock_sleep:
        mock_client = MagicMock()
        mock_client_cls.return_value.__aenter__.return_value = mock_client

        mock_db = MagicMock()
        mock_client.get_database_client.return_value = mock_db

        mock_container = MagicMock()
        mock_db.get_container_client.return_value = mock_container

        mock_container.get_container_properties = AsyncMock(
            return_value={"indexingPolicy": {"indexingMode": "none"}}
        )
        mock_db.create_container_if_not_exists = AsyncMock()

        mock_container.upsert_item = AsyncMock(side_effect=Fake429Error())

        await import_file_task(args, container_name, str(file_path))

        assert mock_container.upsert_item.call_count == 2
        assert mock_sleep.await_count == 1

    assert failed_items_path.exists()
    lines = failed_items_path.read_text().splitlines()
    assert len(lines) == 1

    payload = json.loads(lines[0])
    assert payload["id"] == "1"

