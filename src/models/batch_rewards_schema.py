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
                    "uncapped_payment_eth": str(row["uncapped_payment_eth"]),
                    "capped_payment": str(row["capped_payment"]),
                    "execution_cost": str(row["execution_cost"]),
                    "surplus": str(row["surplus"]),
                    "fee": str(row["fee"]),
                    "winning_score": str(row["winning_score"]),
                    "reference_score": str(row["reference_score"]),
                    # TODO - Not sure yet how to parse this bytea[]
                    #  Will need to experiment with this.
                    "participating_solvers": row["participating_solvers"],
                },
            }
            for row in rewards_df.to_dict(orient="records")
        ]
