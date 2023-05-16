import unittest

import pandas as pd

from src.models.token_imbalance_schema import TokenImbalance


class TestModelTokenImbalance(unittest.TestCase):
    def test_token_imbalance_schema(self):
        max_uint = 115792089237316195423570985008687907853269984665640564039457584007913129639936
        sample_df = pd.DataFrame(
            {
                "block_number": pd.Series([123, 456], dtype="int64"),
                "tx_hash": [
                    "0x71",
                    "0x72",
                ],
                "token": [
                    "0xa0",
                    "0xa1",
                ],
                "amount": [9999, max_uint],
            }
        )

        self.assertEqual(
            [
                {
                    "amount": 9999,
                    "block_number": 123,
                    "token": "0xa0",
                    "tx_hash": "0x71",
                },
                {
                    "amount": 115792089237316195423570985008687907853269984665640564039457584007913129639936,
                    "block_number": 456,
                    "token": "0xa1",
                    "tx_hash": "0x72",
                },
            ],
            TokenImbalance.from_pdf_to_dune_records(sample_df),
        )


if __name__ == "__main__":
    unittest.main()
