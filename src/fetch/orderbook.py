"""Basic client for connecting to postgres database with login credentials"""
from __future__ import annotations

import os
from dataclasses import dataclass
from enum import Enum
from typing import Optional

import pandas as pd
from dotenv import load_dotenv
from pandas import DataFrame
from sqlalchemy import create_engine
from sqlalchemy.engine import Engine

from src.models.block_range import BlockRange
from src.utils import open_query

REORG_THRESHOLD = 65


class OrderbookEnv(Enum):
    """
    Enum for distinguishing between CoW Protocol's staging and production environment
    """

    BARN = "BARN"
    PROD = "PROD"

    def __str__(self) -> str:
        return str(self.value)


@dataclass
class OrderbookFetcher:
    """
    A pair of Dataframes primarily intended to store query results
    from production and staging orderbook databases
    """

    @staticmethod
    def _pg_engine(db_env: OrderbookEnv) -> Engine:
        """Returns a connection to postgres database"""
        load_dotenv()
        db_url = os.environ[f"{db_env}_DB_URL"]
        db_string = f"postgresql+psycopg2://{db_url}"
        return create_engine(db_string)

    @classmethod
    def _read_query_for_env(
        cls, query: str, env: OrderbookEnv, data_types: Optional[dict[str, str]] = None
    ) -> DataFrame:
        # It seems there is a bug in mypy on the dtype field (with error [arg-type]).
        # They expect types that should align with what we pass.
        # More context at: https://github.com/cowprotocol/dune-sync/issues/24
        return pd.read_sql_query(query, con=cls._pg_engine(env), dtype=data_types)  # type: ignore

    @classmethod
    def _query_both_dbs(
        cls, query: str, data_types: Optional[dict[str, str]] = None
    ) -> tuple[DataFrame, DataFrame]:
        barn = cls._read_query_for_env(query, OrderbookEnv.BARN, data_types)
        prod = cls._read_query_for_env(query, OrderbookEnv.PROD, data_types)
        return barn, prod

    @classmethod
    def get_latest_block(cls) -> int:
        """
        Fetches the latest mutually synced block from orderbook databases (with REORG protection)
        """
        data_types = {"latest": "int64"}
        barn, prod = cls._query_both_dbs(
            open_query("orderbook/latest_block.sql"), data_types
        )
        assert len(barn) == 1 == len(prod), "Expecting single record"
        return min(int(barn["latest"][0]), int(prod["latest"][0])) - REORG_THRESHOLD

    @classmethod
    def get_orderbook_rewards(cls, block_range: BlockRange) -> DataFrame:
        """
        Fetches and validates Orderbook Reward DataFrame as concatenation from Prod and Staging DB
        """
        cow_reward_query = (
            open_query("orderbook/order_rewards.sql")
            .replace("{{start_block}}", str(block_range.block_from))
            .replace("{{end_block}}", str(block_range.block_to))
        )
        data_types = {"block_number": "int64", "amount": "float64"}
        barn, prod = cls._query_both_dbs(cow_reward_query, data_types)

        # Solvers do not appear in both environments!
        assert set(prod.solver).isdisjoint(set(barn.solver)), "solver overlap!"
        return pd.concat([prod, barn])
