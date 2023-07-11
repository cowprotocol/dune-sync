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
                "quote_solver": [None, None],
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
        block_number = 16846500
        block_range = BlockRange(block_number, block_number + 25)
        rewards_df = OrderbookFetcher.get_batch_rewards(block_range)
        expected = pd.DataFrame(
            {
                "block_number": pd.Series([16846495, 16846502, pd.NA], dtype="Int64"),
                "block_deadline": [16846509, 16846516, 16846524],
                "tx_hash": [
                    "0x2189c2994dcffcd40cc92245e216b0fda42e0f30573ce4b131341e8ac776ed75",
                    "0x8328fa642f47adb61f751363cf718d707dafcdc258898fa953945afd42aa020f",
                    None,
                ],
                "solver": [
                    "0xb20b86c4e6deeb432a22d773a221898bbbd03036",
                    "0x55a37a2e5e5973510ac9d9c723aec213fa161919",
                    "0x55a37a2e5e5973510ac9d9c723aec213fa161919",
                ],
                "execution_cost": [
                    "5417013431615490",
                    "14681404168612460",
                    "0",
                ],
                "surplus": [
                    "5867838023808109",
                    "104011002982952097",
                    "0",
                ],
                "fee": [
                    "7751978767036064",
                    "10350680045815651",
                    "0",
                ],
                "uncapped_payment_eth": [
                    "7232682540629268",
                    "82825156151734420",
                    "-3527106002507021",
                ],
                "capped_payment": [
                    "7232682540629268",
                    "24681404168612460",
                    "-3527106002507021",
                ],
                "winning_score": [
                    "6537976145828389",
                    "95640781782532198",
                    "3527282436747751",
                ],
                "reference_score": [
                    "6387134250214905",
                    "31536526877033328",
                    "3527106002507021",
                ],
                "participating_solvers": [
                    [
                        "0x398890be7c4fac5d766e1aeffde44b2ee99f38ef",
                        "0xb20b86c4e6deeb432a22d773a221898bbbd03036",
                    ],
                    [
                        "0x55a37a2e5e5973510ac9d9c723aec213fa161919",
                        "0x97ec0a17432d71a3234ef7173c6b48a2c0940896",
                        "0xa21740833858985e4d801533a808786d3647fb83",
                        "0xb20b86c4e6deeb432a22d773a221898bbbd03036",
                        "0xbff9a1b539516f9e20c7b621163e676949959a66",
                        "0xc9ec550bea1c64d779124b23a26292cc223327b6",
                        "0xda869be4adea17ad39e1dfece1bc92c02491504f",
                    ],
                    [
                        "0x149d0f9282333681ee41d30589824b2798e9fb47",
                        "0x3cee8c7d9b5c8f225a8c36e7d3514e1860309651",
                        "0x55a37a2e5e5973510ac9d9c723aec213fa161919",
                        "0x7a0a8890d71a4834285efdc1d18bb3828e765c6a",
                        "0x97ec0a17432d71a3234ef7173c6b48a2c0940896",
                        "0xa21740833858985e4d801533a808786d3647fb83",
                        "0xb20b86c4e6deeb432a22d773a221898bbbd03036",
                        "0xbff9a1b539516f9e20c7b621163e676949959a66",
                        "0xc9ec550bea1c64d779124b23a26292cc223327b6",
                        "0xda869be4adea17ad39e1dfece1bc92c02491504f",
                        "0xe9ae2d792f981c53ea7f6493a17abf5b2a45a86b",
                    ],
                ],
            },
        )
        self.assertIsNone(pd.testing.assert_frame_equal(expected, rewards_df))


if __name__ == "__main__":
    unittest.main()
