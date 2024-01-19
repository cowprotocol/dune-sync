import unittest

import pandas
import pandas as pd

from src.models.batch_rewards_schema import BatchRewards

ONE_ETH = 1000000000000000000


class TestModelBatchRewards(unittest.TestCase):
    def test_order_rewards_transformation(self):
        max_uint = 115792089237316195423570985008687907853269984665640564039457584007913129639936
        sample_df = pd.DataFrame(
            {
                "block_number": pd.Series([123, pandas.NA], dtype="Int64"),
                "block_deadline": [789, 1011],
                "tx_hash": [
                    "0x71",
                    None,
                ],
                "solver": [
                    "0x51",
                    "0x52",
                ],
                "execution_cost": [9999 * ONE_ETH, 1],
                "surplus": [2 * ONE_ETH, 3 * ONE_ETH],
                "fee": [
                    1000000000000000,
                    max_uint,
                ],
                "protocol_fee": [2000000000000000, 0],
                "uncapped_payment_eth": [0, -10 * ONE_ETH],
                "capped_payment": [-1000000000000000, -1000000000000000],
                "winning_score": [123456 * ONE_ETH, 6789 * ONE_ETH],
                "reference_score": [ONE_ETH, 2 * ONE_ETH],
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
                        "capped_payment": -1000000000000000,
                        "execution_cost": 9999000000000000000000,
                        "fee": 1000000000000000,
                        "participating_solvers": ["0x51", "0x52", "0x53"],
                        "protocol_fee": 2000000000000000,
                        "reference_score": 1000000000000000000,
                        "surplus": 2000000000000000000,
                        "uncapped_payment_eth": 0,
                        "winning_score": 123456000000000000000000,
                    },
                    "solver": "0x51",
                    "tx_hash": "0x71",
                },
                {
                    "block_deadline": 1011,
                    "block_number": None,
                    "data": {
                        "capped_payment": -1000000000000000,
                        "execution_cost": 1,
                        "fee": max_uint,
                        "participating_solvers": [
                            "0x51",
                            "0x52",
                            "0x53",
                            "0x54",
                            "0x55",
                            "0x56",
                        ],
                        "protocol_fee": 0,
                        "reference_score": 2000000000000000000,
                        "surplus": 3000000000000000000,
                        "uncapped_payment_eth": -10000000000000000000,
                        "winning_score": 6789000000000000000000,
                    },
                    "solver": "0x52",
                    "tx_hash": None,
                },
            ],
            BatchRewards.from_pdf_to_dune_records(sample_df),
        )


if __name__ == "__main__":
    unittest.main()
