import os
import shutil
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
            max_retries=2,
            give_up_threshold=3,
        )
        self.file_manager = FileIO(self.config.volume_path / str(SYNC_TABLE))

    def tearDown(self) -> None:
        shutil.rmtree(self.config.volume_path)

    async def test_fetch_content_and_filter(self):
        retries = self.config.max_retries
        give_up = self.config.give_up_threshold
        missing_files = self.config.missing_files_name
        # block numbers
        a, b, c = 15582187, 16082187, 16100000
        self.assertTrue(a < b < c)

        block_range_1 = BlockRange(
            block_from=a,
            block_to=b,
        )
        data_handler = AppDataHandler(
            file_manager=self.file_manager,
            new_rows=await self.dune.get_app_hashes(block_range_1),
            block_range=block_range_1,
            config=self.config,
            missing_file_name=missing_files,
            ipfs_access_key=os.environ["IPFS_ACCESS_KEY"],
        )

        print(f"Beginning Content Fetching on {len(data_handler.new_rows)} records")
        await data_handler.fetch_content_and_filter(retries, give_up)
        data_handler.write_found_content()
        self.assertEqual(0, len(data_handler.new_rows))

        block_range_2 = BlockRange(
            block_from=b,
            block_to=c,
        )
        data_handler = AppDataHandler(
            file_manager=self.file_manager,
            new_rows=await self.dune.get_app_hashes(block_range_2),
            block_range=block_range_2,
            config=self.config,
            missing_file_name=missing_files,
            ipfs_access_key=os.environ["IPFS_ACCESS_KEY"],
        )
        print(
            f"Beginning Second Run Content Fetching on {len(data_handler.new_rows)} records"
        )
        await data_handler.fetch_content_and_filter(retries, give_up)
        data_handler.write_found_content()

        self.assertEqual(0, len(data_handler.new_rows))
        # Two runs with retries = 2 and give_up = 3 implies no more missing records.
        self.assertEqual(0, len(data_handler._not_found))


if __name__ == "__main__":
    unittest.main()
