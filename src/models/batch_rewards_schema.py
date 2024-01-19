"""Model for Batch Rewards Data"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Any

import pandas
from pandas import DataFrame


@dataclass
class BatchRewards:
    """
    This class provides a transformation interface for the Dataframe we fetch from the orderbook
    """

    @classmethod
    def from_pdf_to_dune_records(cls, rewards_df: DataFrame) -> list[dict[str, Any]]:
        """Converts Pandas DataFrame into the expected stream type for Dune"""
        return [
            {
                "block_number": int(row["block_number"])
                if not pandas.isna(row["block_number"])
                else None,
                "tx_hash": row["tx_hash"],
                "solver": row["solver"],
                "block_deadline": int(row["block_deadline"]),
                "data": {
                    # All the following values are in WEI.
                    "uncapped_payment_eth": int(row["uncapped_payment_eth"]),
                    "capped_payment": int(row["capped_payment"]),
                    "execution_cost": int(row["execution_cost"]),
                    "surplus": int(row["surplus"]),
                    "fee": int(row["fee"]),
                    "protocol_fee": int(row["protocol_fee"]),
                    "winning_score": int(row["winning_score"]),
                    "reference_score": int(row["reference_score"]),
                    "participating_solvers": row["participating_solvers"],
                },
            }
            for row in rewards_df.to_dict(orient="records")
        ]
