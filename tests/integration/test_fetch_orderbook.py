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
        block_number = 17500000
        block_range = BlockRange(block_number, block_number + 30)
        rewards_df = OrderbookFetcher.get_order_rewards(block_range)
        expected = pd.DataFrame(
            {
                "block_number": [17500006, 17500022],
                "order_uid": [
                    "0x115bb7fd4ec0c8cec1bed470b599cd187173825bfd32eec84903faf5eb7038546d335b1f889a82a6d9a7fb6db86cab15685e3c3e648dc436",
                    "0x49f9cf94a59301548bbfaa75c06b1126cb3194d0b18a9e516ebeffabfb7e2d659cf8986e78d6215d1491c66d64409a54cf6268e4648dd550",
                ],
                "solver": [
                    "0x31a9ec3a6e29039c74723e387de42b79e6856fd8",
                    "0x31a9ec3a6e29039c74723e387de42b79e6856fd8",
                ],
                "quote_solver": [None, None],
                "tx_hash": [
                    "0xb3d59ac16f874a739ce93a58139ba5848d6ce521fa70fad226f3c3d2d47b2aa6",
                    "0x22916b61ad5207a493d66b3662a27f5af1c2ac76e66672cfd9ae57041a1ca2a2",
                ],
                "surplus_fee": ["0", "0"],
                "amount": [0, 0],
                "protocol_fee": ["0", "0"],
                "protocol_fee_token": [None, None],
                "protocol_fee_native_price": [0.0, 0.0],
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
                "protocol_fee": [
                    "0",
                    "0",
                    "0",
                ],
                "network_fee": [
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

    def test_get_batch_rewards_with_protocol_fees(self):
        block_number = 19034333
        block_range = BlockRange(block_number, block_number + 9)
        rewards_df = OrderbookFetcher.get_batch_rewards(block_range)
        print(rewards_df)
        expected = pd.DataFrame(
            {
                "block_number": pd.Series([19034331, 19034333], dtype="Int64"),
                "block_deadline": [19034340, 19034341],
                "tx_hash": [
                    "0x60cfcecab62c2fc03596be4e9a9c7c1113d41a10eb9f821da04b096aacb7e73d",
                    "0xc725ce0a051955d5c6c98e039cb52a72f96b56b5ea3e95b23ff92c746c0ae4c3",
                ],
                "solver": [
                    "0x8616dcdfcecbde13ccd89eac358dc5abda79ec31",
                    "0x01246d541e732d7f15d164331711edff217e4665",
                ],
                "execution_cost": [
                    "6874093717444341",
                    "3885032282790366",
                ],
                "surplus": [
                    "1917140833491803",
                    "45868496778113149",
                ],
                "protocol_fee": [
                    "0",
                    "463318149273870",
                ],
                "network_fee": [
                    "5746294767802878",
                    "6412406319694589",
                ],
                "uncapped_payment_eth": [
                    "7613796432942231",
                    "52744221247081608",
                ],
                "capped_payment": [
                    "7613796432942231",
                    "13885032282790366",
                ],
                "winning_score": [
                    "280137833843581",
                    "47509854752448587",
                ],
                "reference_score": [
                    "49639168352450",
                    "0",
                ],
                "participating_solvers": [
                    [
                        "0x01246d541e732d7f15d164331711edff217e4665",
                        "0x2456a4c1241e43e11b0b8f80e31c940bebd9090f",
                        "0x8616dcdfcecbde13ccd89eac358dc5abda79ec31",
                    ],
                    ["0x01246d541e732d7f15d164331711edff217e4665"],
                ],
            },
        )
        self.assertIsNone(pd.testing.assert_frame_equal(expected, rewards_df))


if __name__ == "__main__":
    unittest.main()
