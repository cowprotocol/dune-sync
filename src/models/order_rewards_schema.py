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
                },
            }
            for row in rewards_df.to_dict(orient="records")
        ]
