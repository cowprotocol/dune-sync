"""Model for Order Rewards Data"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from pandas import DataFrame


@dataclass
class OrderRewards:
    """
    This class provides a transformation interface for the Dataframe we fetch from the orderbook
    """

    @classmethod
    def from_pdf_to_dune_records(cls, rewards_df: DataFrame) -> list[dict[str, Any]]:
        """Converts Pandas DataFrame into the expected stream type for Dune"""
        return [
            {
                "block_number": int(row["block_number"]),
                "order_uid": row["order_uid"],
                "tx_hash": row["tx_hash"],
                "solver": row["solver"],
                "data": {
                    "surplus_fee": str(row["surplus_fee"]),
                    "amount": float(row["amount"]),
                    "quote_solver": row["quote_solver"],
                    "protocol_fee": str(row["protocol_fee"]),
                    "protocol_fee_token": row["protocol_fee_token"],
                    "protocol_fee_native_price": float(
                        row["protocol_fee_native_price"]
                    ),
                    "quote_sell_amount": str(row["quote_sell_amount"]),
                    "quote_buy_amount": str(row["quote_buy_amount"]),
                    "quote_gas_cost": float(row["quote_gas_cost"]),
                    "quote_sell_token_price": float(row["quote_sell_token_price"]),
                },
            }
            for row in rewards_df.to_dict(orient="records")
        ]
