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
        rewards_df = OrderbookFetcher.get_order_rewards(block_range)
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

    def test_get_batch_rewards(self):
        block_number = 16735007
        block_range = BlockRange(block_number, block_number + 50)
        rewards_df = OrderbookFetcher.get_batch_rewards(block_range)

        expected = pd.DataFrame(
            {
                "block_number": [16734995, 16734998],
                "block_deadline": [16735008, 16735011],
                "tx_hash": [
                    "0x203bac6edde8f4dd2e18e7a5e2d81cb721d8b4f1f021217d0d4b55a799efe3f0",
                    "0xce494850e80b3308d71a5896b7485de0d777af924ebad064d74be4320d027cba",
                ],
                "solver": [
                    "0xde786877a10dbb7eba25a4da65aecf47654f08ab",
                    "0xde786877a10dbb7eba25a4da65aecf47654f08ab",
                ],
                "execution_cost": [7538092113186786.0, 7116851210101934.0],
                "surplus": [53999674326241.0, 58558783891722.0],
                "fee": [
                    0.0,
                    0.0,
                ],
                "uncapped_payment_eth": [-11707681268602884.0, -11238052424133296.0],
                "capped_payment": [-10000000000000000.0, -10000000000000000.0],
                "winning_score": [11761680942929144.0, 11296611208025016.0],
                "reference_score": [11761680942929126.0, 11296611208025016.0],
                "participating_solvers": [
                    [
                        "0x8a4e90e9afc809a69d2a3bdbe5fff17a12979609",
                        "0xde786877a10dbb7eba25a4da65aecf47654f08ab",
                        "0xe33062a24149f7801a48b2675ed5111d3278f0f5",
                    ],
                    [
                        "0x0a308697e1d3a91dcb1e915c51f8944aaec9015f",
                        "0x109bf9e0287cc95cc623fbe7380dd841d4bdeb03",
                        "0x8a4e90e9afc809a69d2a3bdbe5fff17a12979609",
                        "0xdae69affe582d36f330ee1145995a53fab670962",
                        "0xde786877a10dbb7eba25a4da65aecf47654f08ab",
                        "0xe33062a24149f7801a48b2675ed5111d3278f0f5",
                    ],
                ],
            }
        )
        self.assertIsNone(pd.testing.assert_frame_equal(expected, rewards_df))


if __name__ == "__main__":
    unittest.main()
