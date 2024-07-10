import os
import unittest
import time

from unittest import IsolatedAsyncioTestCase

from dotenv import load_dotenv
from dune_client.client import DuneClient

from src.fetch.dune import DuneFetcher
from src.fetch.orderbook import OrderbookFetcher
from src.sync import sync_app_data
from src.sync.config import AppDataSyncConfig

class TestSyncAppData(IsolatedAsyncioTestCase):
    def setUp(self) -> None:
        load_dotenv()
        self.dune = DuneClient(os.environ["DUNE_API_KEY"])
        self.fetcher = DuneFetcher(self.dune)
        self.namespace = "cowprotocol"
        self.config = AppDataSyncConfig(table="app_data_test")
        self.query = self.dune.create_query(
            name="Fetch app data hashes for test",
            query_sql=f"SELECT * FROM dune.{self.namespace}.dataset_{self.config.table}",
        )

    def tearDown(self) -> None:
        self.dune.delete_table(
          namespace=self.namespace,
          table_name="dataset_" + self.config.table
        )
        self.dune.archive_query(self.query.base.query_id)

    async def test_fetch_content_and_filter(self):
        print("Beginning Sync")
        await sync_app_data(
            orderbook=OrderbookFetcher(),
            dune=self.dune,
            config=self.config,
            dry_run=False,
        )
        print("Finished Sync")
        # Wait some time for the table to be exposed
        time.sleep(5)
        result = self.dune.run_query(self.query.base).result.rows
        print(f"Found {len(result)} results")
        self.assertGreater(len(result), 0)

if __name__ == "__main__":
    unittest.main()
