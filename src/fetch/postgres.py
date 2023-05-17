"""Generic Postgres Adapter for executing queries on a postgres DB."""
from dataclasses import dataclass
from typing import Optional

import pandas as pd
from pandas import DataFrame
from sqlalchemy import create_engine
from sqlalchemy.engine import Engine

from src.fetch.orderbook import REORG_THRESHOLD
from src.models.block_range import BlockRange
from src.utils import open_query


@dataclass
class PostgresFetcher:
    """
    Basic Postgres interface
    """

    engine: Engine

    def __init__(self, db_url: str):
        db_string = f"postgresql+psycopg2://{db_url}"

        self.engine = create_engine(db_string)

    def _read_query(
        self, query: str, data_types: Optional[dict[str, str]] = None
    ) -> DataFrame:
        return pd.read_sql_query(query, con=self.engine, dtype=data_types)  # type: ignore

    def get_latest_block(self) -> int:
        """
        Fetches the latest mutually synced block from orderbook databases (with REORG protection)
        """
        data_types = {"latest": "int64"}
        res = self._read_query(open_query("warehouse/latest_block.sql"), data_types)
        assert len(res) == 1, "Expecting single record"
        return int(res["latest"][0]) - REORG_THRESHOLD

    def get_internal_imbalances(self, block_range: BlockRange) -> DataFrame:
        """
        Fetches and validates Internal Token Imbalances
        """
        cow_reward_query = (
            open_query("warehouse/token_imbalances.sql")
            .replace("{{start_block}}", str(block_range.block_from))
            .replace("{{end_block}}", str(block_range.block_to))
        )
        data_types = {
            "block_number": "int64",
        }
        return self._read_query(cow_reward_query, data_types)
