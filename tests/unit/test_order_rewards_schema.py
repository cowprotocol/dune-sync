import unittest

import pandas as pd

from src.models.order_rewards_schema import OrderRewards


class TestModelOrderRewards(unittest.TestCase):
    def test_order_rewards_transformation(self):
        rewards_df = pd.DataFrame(
            {
                "block_number": [1, 2, 3],
                "order_uid": ["0x01", "0x02", "0x03"],
                "solver": ["0x51", "0x52", "0x53"],
                "tx_hash": ["0x71", "0x72", "0x73"],
                "quote_solver": ["0x21", None, "0x22"],
                "surplus_fee": [12345678910111213, 0, 0],
                "amount": [40.70410, 39.00522, 0],
                "protocol_fee": [1000000000000000, 123123123123123, 0],
                "protocol_fee_token": ["0x91", "0x92", None],
                "protocol_fee_native_price": [1.0, 0.1, 0.0],
                "quote_sell_amount": [10000000000000000, 2000000000000000, 35000],
                "quote_buy_amount": [1000, 2000, 10],
                "quote_gas_cost": [
                    5000000000000000.15,
                    6000000000000000,
                    12000000000000000,
                ],
                "quote_sell_token_price": [1.0, 250000000, 100000000000000.0],
                "protocol_fee_recipient": None,
            }
        )

        self.assertEqual(
            [
                {
                    "block_number": 1,
                    "order_uid": "0x01",
                    "tx_hash": "0x71",
                    "solver": "0x51",
                    "data": {
                        "surplus_fee": "12345678910111213",
                        "amount": 40.70410,
                        "quote_solver": "0x21",
                        "protocol_fee": "1000000000000000",
                        "protocol_fee_token": "0x91",
                        "protocol_fee_native_price": 1.0,
                        "quote_sell_amount": "10000000000000000",
                        "quote_buy_amount": "1000",
                        "quote_gas_cost": 5000000000000000.15,
                        "quote_sell_token_price": 1.0,
                        "protocol_fee_recipient": None,
                    },
                },
                {
                    "block_number": 2,
                    "order_uid": "0x02",
                    "tx_hash": "0x72",
                    "solver": "0x52",
                    "data": {
                        "surplus_fee": "0",
                        "amount": 39.00522,
                        "quote_solver": None,
                        "protocol_fee": "123123123123123",
                        "protocol_fee_token": "0x92",
                        "protocol_fee_native_price": 0.1,
                        "quote_sell_amount": "2000000000000000",
                        "quote_buy_amount": "2000",
                        "quote_gas_cost": 6000000000000000,
                        "quote_sell_token_price": 250000000,
                        "protocol_fee_recipient": None,
                    },
                },
                {
                    "block_number": 3,
                    "order_uid": "0x03",
                    "tx_hash": "0x73",
                    "solver": "0x53",
                    "data": {
                        "surplus_fee": "0",
                        "amount": 0.0,
                        "quote_solver": "0x22",
                        "protocol_fee": "0",
                        "protocol_fee_token": None,
                        "protocol_fee_native_price": 0.0,
                        "quote_sell_amount": "35000",
                        "quote_buy_amount": "10",
                        "quote_gas_cost": 12000000000000000,
                        "quote_sell_token_price": 100000000000000.0,
                        "protocol_fee_recipient": None,
                    },
                },
            ],
            OrderRewards.from_pdf_to_dune_records(rewards_df),
        )


if __name__ == "__main__":
    unittest.main()
