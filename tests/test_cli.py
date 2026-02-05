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

def test_export_cmd_basic(tmp_path):
    # Mocking export_cmd_async instead of export_cmd
    with patch("cosmos_dumper.cli.export_cmd_async", new_callable=AsyncMock) as mock_async:
        from cosmos_dumper.cli import export_cmd
        args = MagicMock()
        args.workers = 1
        export_cmd(args)
        assert mock_async.called

def test_import_cmd_basic(tmp_path):
    with patch("cosmos_dumper.cli.import_cmd_async", new_callable=AsyncMock) as mock_async:
        from cosmos_dumper.cli import import_cmd
        args = MagicMock()
        args.workers = 1
        args.path = "some_path"
        import_cmd(args)
        assert mock_async.called
