"""Basic client for connecting to postgres database with login credentials"""
from __future__ import annotations

import os
from dataclasses import dataclass
from enum import Enum

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
        port = os.environ.get("DB_PORT", 5432)
        user = os.environ["DB_USER"]
        database = os.environ["DB_NAME"]
        host = os.environ[f"{db_env}_ORDERBOOK_HOST"]
        password = os.environ[f"{db_env}_ORDERBOOK_PASSWORD"]
        db_string = f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{database}"
        return create_engine(db_string)

    @classmethod
    def _query_both_dbs(cls, query: str) -> tuple[DataFrame, DataFrame]:
        barn = pd.read_sql(sql=query, con=cls._pg_engine(OrderbookEnv.PROD))
        prod = pd.read_sql(sql=query, con=cls._pg_engine(OrderbookEnv.BARN))
        return barn, prod

    @classmethod
    def get_latest_block(cls) -> int:
        """
        Fetches the latest mutually synced block from orderbook databases (with REORG protection)
        """
        barn, prod = cls._query_both_dbs(open_query("orderbook/latest_block.sql"))
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
        barn, prod = cls._query_both_dbs(cow_reward_query)

        # Solvers do not appear in both environments!
        assert set(prod.solver).isdisjoint(set(barn.solver)), "solver overlap!"
        return pd.concat([prod, barn])
