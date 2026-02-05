import asyncio
import json
import os
import pytest
from unittest.mock import AsyncMock, MagicMock, patch
from cosmos_dumper.cli import import_cmd


def test_import_with_concurrency(tmp_path):
    with patch(
        "cosmos_dumper.cli.import_cmd_async", new_callable=AsyncMock
    ) as mock_async:
        from cosmos_dumper.cli import import_cmd

        args = MagicMock()
        args.workers = 1
        import_cmd(args)
        assert mock_async.called
