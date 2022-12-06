import os
import unittest
from pathlib import Path
from unittest import IsolatedAsyncioTestCase

from dotenv import load_dotenv
from dune_client.file.interface import FileIO

from src.fetch.dune import DuneFetcher
from src.models.block_range import BlockRange
from src.sync.app_data import AppDataHandler, SYNC_TABLE
from src.sync.config import AppDataSyncConfig


class TestSyncAppData(IsolatedAsyncioTestCase):
    def setUp(self) -> None:
        load_dotenv()
        self.dune = DuneFetcher(os.environ["DUNE_API_KEY"])
        self.config = AppDataSyncConfig(
            volume_path=Path(os.environ["VOLUME_PATH"]),
            missing_files_name="missing_app_hashes.json",
            max_retries=3,
            give_up_threshold=10,
        )

    async def test_fetch_content_and_filter(self):
        block_range = BlockRange(
            block_from=15082187,
            block_to=16082187,
        )
        data_handler = AppDataHandler(
            file_manager=FileIO(self.config.volume_path / str(SYNC_TABLE)),
            new_rows=await self.dune.get_app_hashes(block_range),
            block_range=block_range,
            config=self.config,
            missing_file_name=self.config.missing_files_name,
        )
        max_retries = self.config.max_retries
        give_up_threshold = self.config.give_up_threshold

        await data_handler.fetch_content_and_filter(max_retries, give_up_threshold)
        self.assertEqual(True, False)  # add assertion here


if __name__ == "__main__":
    unittest.main()
