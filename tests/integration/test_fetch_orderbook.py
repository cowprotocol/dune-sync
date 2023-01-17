import unittest

import pandas as pd

from src.fetch.orderbook import OrderbookFetcher
from src.models.block_range import BlockRange


class TestFetchOrderbook(unittest.TestCase):
    def test_latest_block_reasonable(self):
        self.assertGreater(OrderbookFetcher.get_latest_block(), 16020300)

    def test_latest_block_increasing(self):
        latest_block = OrderbookFetcher.get_latest_block()
        self.assertGreaterEqual(OrderbookFetcher.get_latest_block(), latest_block)

    def test_get_order_rewards(self):
        block_number = 16000000
        block_range = BlockRange(block_number, block_number + 50)
        rewards_df = OrderbookFetcher.get_orderbook_rewards(block_range)
        print(rewards_df.keys())
        expected = pd.DataFrame(
            {
                "block_number": [16000018, 16000050],
                "order_uid": [
                    "0xb52fecfe3df73f0e93f1f9b27c92e3def50322960f9c62d0b97bc2ceee36c07a0639dda84198dc06f5bc91bddbb62cd2e38c2f9a6378140f",
                    "0xf61cba0b42ed3e956f9db049c0523e123967723c5bcf76ccac0b179a66305b2a7fee439ed7a6bb1b8e7ca1ffdb0a5ca8d993c030637815ad",
                ],
                "solver": [
                    "0x3cee8c7d9b5c8f225a8c36e7d3514e1860309651",
                    "0xc9ec550bea1c64d779124b23a26292cc223327b6",
                ],
                "tx_hash": [
                    "0xb6f7df8a1114129f7b61f2863b3f81b3620e95f73e5b769a62bb7a87ab6983f4",
                    "0x2ce77009e78c291cdf39eb6f8ddf7e2c3401b4f962ef1240bdac47e632f8eb7f",
                ],
                "surplus_fee": ["0", "0"],
                "amount": [40.70410, 39.00522],
            }
        )

        self.assertIsNone(pd.testing.assert_frame_equal(expected, rewards_df))


if __name__ == "__main__":
    unittest.main()
