"""Model for Order Rewards Data"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from pandas import DataFrame


@dataclass
class OrderRewards:
    """
    This class provides a transformation interface for the Dataframe we fetch from the orderbook
    Within are several methods for converting from the Dataframe to list[DuneRecords]
    """

    order_uid: str
    tx_hash: str
    solver: str
    data: dict[str, Any]

    @classmethod
    def from_pdf(cls, rewards_df: DataFrame) -> list[OrderRewards]:
        """Multi Record Constructor from Pandas Dataframe"""
        return [
            cls(
                order_uid=row["order_uid"],
                tx_hash=row["tx_hash"],
                solver=row["solver"],
                data={
                    "surplus_fee": str(row["surplus_fee"]),
                    "amount": float(row["amount"]),
                    "safe_liquidity": row["safe_liquidity"],
                },
            )
            for row in rewards_df.to_dict(orient="records")
        ]

    def as_dune_record(self) -> dict[str, Any]:
        """Convert to Dune Record"""
        return {
            "order_uid": self.order_uid,
            "tx_hash": self.tx_hash,
            "solver": self.solver,
            "data": self.data,
        }

    @classmethod
    def from_pdf_to_dune_records(cls, rewards_df: DataFrame) -> list[dict[str, Any]]:
        """Converts Pandas DataFrame into the expected stream type for Dune"""
        return [
            {
                "order_uid": row["order_uid"],
                "tx_hash": row["tx_hash"],
                "solver": row["solver"],
                "data": {
                    "surplus_fee": str(row["surplus_fee"]),
                    "amount": float(row["amount"]),
                    "safe_liquidity": row["safe_liquidity"],
                },
            }
            for row in rewards_df.to_dict(orient="records")
        ]
