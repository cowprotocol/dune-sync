"""Model for Batch Rewards Data"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from pandas import DataFrame


@dataclass
class TokenImbalance:
    """
    This class provides a transformation interface (to JSON) for Dataframe
    """

    @classmethod
    def from_pdf_to_dune_records(cls, frame: DataFrame) -> list[dict[str, Any]]:
        """Converts Pandas DataFrame into the expected stream type for Dune"""
        return [
            {
                "block_number": int(row["block_number"]),
                "tx_hash": row["tx_hash"],
                "token": row["token"],
                "amount": str(row["amount"]),
            }
            for row in frame.to_dict(orient="records")
        ]
