import os
import unittest

import pandas as pd
from dotenv import load_dotenv

from src.fetch.postgres import PostgresFetcher
from src.models.block_range import BlockRange


@unittest.skip("skipping")
class TestPostgresWarehouseFetching(unittest.TestCase):
    def setUp(self) -> None:
        load_dotenv()
        # TODO - deploy test DB and populate with some records...
        # self.fetcher = PostgresFetcher(db_url=os.environ["WAREHOUSE_URL"])

    def test_latest_block_reasonable(self):
        self.assertGreater(self.fetcher.get_latest_block(), 17273090)

    def test_get_imbalances(self):
        imbalance_df = self.fetcher.get_internal_imbalances(
            BlockRange(17236982, 17236983)
        )
        expected = pd.DataFrame(
            {
                "block_number": pd.Series([17236983, 17236983], dtype="int64"),
                "tx_hash": [
                    "0x9dc611149c7d6a936554b4be0e4fde455c015a9d5c81650af1433df5e904c791",
                    "0x9dc611149c7d6a936554b4be0e4fde455c015a9d5c81650af1433df5e904c791",
                ],
                "token": [
                    "0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48",
                    "0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2",
                ],
                "amount": ["-1438513324", "789652004205719637"],
            },
        )
        self.assertIsNone(pd.testing.assert_frame_equal(expected, imbalance_df))


if __name__ == "__main__":
    unittest.main()
