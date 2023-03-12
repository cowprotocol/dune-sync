import unittest

import pandas as pd

from src.models.batch_rewards_schema import BatchRewards

ONE_ETH = 1000000000000000000


class TestModelBatchRewards(unittest.TestCase):
    def test_order_rewards_transformation(self):
        sample_df = pd.DataFrame(
            {
                "block_number": [123, 456],
                "block_deadline": [789, 1011],
                "tx_hash": [
                    "0x71",
                    "0x72",
                ],
                "solver": [
                    "0x51",
                    "0x52",
                ],
                "execution_cost": [9999 * ONE_ETH, 1],
                "surplus": [2 * ONE_ETH, 3 * ONE_ETH],
                "fee": [
                    0.01 * ONE_ETH,
                    0.0,
                ],
                "uncapped_payment_eth": [0.0, -10 * ONE_ETH],
                "capped_payment": [-0.001 * ONE_ETH, -0.001 * ONE_ETH],
                "winning_score": [11761680942929144.0, 11296611208025016.0],
                "reference_score": [11761680942929126.0, 11296611208025016.0],
                "participating_solvers": [
                    [
                        "0x51",
                        "0x52",
                        "0x53",
                    ],
                    [
                        "0x51",
                        "0x52",
                        "0x53",
                        "0x54",
                        "0x55",
                        "0x56",
                    ],
                ],
            }
        )

        self.assertEqual(
            [
                {
                    "block_deadline": 789,
                    "block_number": 123,
                    "data": {
                        "capped_payment": "-1000000000000000.0",
                        "execution_cost": "9999000000000000000000",
                        "fee": "1e+16",
                        "participating_solvers": ["0x51", "0x52", "0x53"],
                        # TODO - We can't have scientific notation here!
                        #  Must force Dataframe to have string type!
                        "reference_score": "1.1761680942929126e+16",
                        "surplus": "2000000000000000000",
                        "uncapped_payment_eth": "0.0",
                        "winning_score": "1.1761680942929144e+16",
                    },
                    "solver": "0x51",
                    "tx_hash": "0x71",
                },
                {
                    "block_deadline": 1011,
                    "block_number": 456,
                    "data": {
                        "capped_payment": "-1000000000000000.0",
                        "execution_cost": "1",
                        "fee": "0.0",
                        "participating_solvers": [
                            "0x51",
                            "0x52",
                            "0x53",
                            "0x54",
                            "0x55",
                            "0x56",
                        ],
                        "reference_score": "1.1296611208025016e+16",
                        "surplus": "3000000000000000000",
                        "uncapped_payment_eth": "-10000000000000000000",
                        "winning_score": "1.1296611208025016e+16",
                    },
                    "solver": "0x52",
                    "tx_hash": "0x72",
                },
            ],
            BatchRewards.from_pdf_to_dune_records(sample_df),
        )


if __name__ == "__main__":
    unittest.main()
