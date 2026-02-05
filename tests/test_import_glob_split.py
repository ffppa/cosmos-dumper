import os
import json
import pytest
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

from cosmos_dumper.cli import import_cmd_async


@pytest.mark.asyncio
async def test_import_glob_picks_split_files(tmp_path):
    # Create split export files for the same container
    container = "mycoll"
    f1 = tmp_path / f"{container}_export.json"
    f2 = tmp_path / f"{container}_export_1.json"
    f3 = tmp_path / f"{container}_export_2.jsonl"

    # Minimal valid JSON/JSONL contents
    f1.write_text("[]")
    f2.write_text("[]")
    f3.write_text("{}\n{}\n")

    args = SimpleNamespace(
        url="https://example/",
        key="key",
        db="db",
        path=str(tmp_path),
        workers=1,
        container=None,
        from_container=None,
        concurrency=1,
        shuffle=False,
    )

    with patch(
        "cosmos_dumper.cli.import_file_task", new_callable=AsyncMock
    ) as mock_import:
        mock_import.return_value = 0.0
        await import_cmd_async(args)

        # Should be called for each of the three files
        assert mock_import.await_count == 3
        called_containers = {call.args[1] for call in mock_import.call_args_list}
        assert called_containers == {container}

        called_files = {
            os.path.basename(call.args[2]) for call in mock_import.call_args_list
        }
        assert called_files == {f1.name, f2.name, f3.name}
