import unittest

import pandas as pd

from src.models.order_rewards_schema import OrderRewards


class TestModelOrderRewards(unittest.TestCase):
    def test_order_rewards_transformation(self):
        rewards_df = pd.DataFrame(
            {
                "order_uid": ["0x01", "0x02", "0x03"],
                "solver": ["0x51", "0x52", "0x53"],
                "tx_hash": ["0x71", "0x72", "0x73"],
                "surplus_fee": [12345678910111213, 0, 0],
                "amount": [40.70410, 39.00522, 0],
                "safe_liquidity": [None, True, False],
            }
        )

        self.assertEqual(
            [
                {
                    "order_uid": "0x01",
                    "solver": "0x51",
                    "tx_hash": "0x71",
                    "data": {
                        "surplus_fee": "12345678910111213",
                        "amount": 40.70410,
                        "safe_liquidity": None,
                    },
                },
                {
                    "order_uid": "0x02",
                    "solver": "0x52",
                    "tx_hash": "0x72",
                    "data": {
                        "surplus_fee": "0",
                        "amount": 39.00522,
                        "safe_liquidity": True,
                    },
                },
                {
                    "order_uid": "0x03",
                    "solver": "0x53",
                    "tx_hash": "0x73",
                    "data": {
                        "surplus_fee": "0",
                        "amount": 0.0,
                        "safe_liquidity": False,
                    },
                },
            ],
            OrderRewards.from_pdf_to_dune_records(rewards_df),
        )


if __name__ == "__main__":
    unittest.main()
