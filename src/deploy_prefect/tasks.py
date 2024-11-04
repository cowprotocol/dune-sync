"""Prefect Deployment for Order Rewards Data"""

import os
from io import StringIO
from datetime import datetime, timezone

import requests
import pandas as pd
from dotenv import load_dotenv
from dune_client.client import DuneClient

# pylint: disable=import-error
from prefect import task, get_run_logger  # type: ignore

from src.models.block_range import BlockRange
from src.fetch.orderbook import OrderbookFetcher
from src.deploy_prefect.utils import get_last_monday_midnight_utc
from src.deploy_prefect.models import Config

load_dotenv()


@task  # type: ignore[misc]
def get_block_range() -> BlockRange:
    """Returns the blockrange from last monday midnight until now"""
    etherscan_api = "https://api.etherscan.io/api"
    api_key = os.environ["ETHERSCAN_API_KEY"]
    start = (
        requests.get(
            etherscan_api,
            {  # type: ignore
                "module": "block",
                "action": "getblocknobytime",
                "timestamp": get_last_monday_midnight_utc(),
                "closest": "before",
                "apikey": api_key,
            },
            timeout=60,
        )
        .json()
        .get("result")
    )
    end = (
        requests.get(
            etherscan_api,
            {  # type: ignore
                "module": "block",
                "action": "getblocknobytime",
                "timestamp": int(datetime.now(timezone.utc).timestamp()),
                "closest": "before",
                "apikey": api_key,
            },
            timeout=60,
        )
        .json()
        .get("result")
    )

    blockrange = BlockRange(block_from=start, block_to=end)
    return blockrange


@task  # type: ignore[misc]
def fetch_orderbook(blockrange: BlockRange) -> pd.DataFrame:
    """Runs the query to get the order book for a specified blockrange"""
    orderbook = OrderbookFetcher()
    return orderbook.get_order_rewards(blockrange)


@task  # type: ignore[misc]
def cast_orderbook_to_dune_string(orderbook: pd.DataFrame) -> str:
    """Casts the dataframe to a string in csv format for uploading to Dune"""
    csv_buffer = StringIO()
    orderbook.to_csv(csv_buffer, index=False)
    return csv_buffer.getvalue()


@task  # type: ignore[misc]
def upload_data_to_dune(
    data: str, block_start: int, block_end: int, config: Config
) -> str:
    """
    Uploads the order rewards data to Dune,
    either creating a new query or updating an existing one
    """
    table_name = f"order_rewards_{config.env}_{block_start}"
    dune = DuneClient.from_env()
    dune.upload_csv(  # type: ignore[attr-defined]
        data=data,
        description=f"Order rewards data for blocks {block_start}-{block_end}",
        table_name=table_name,
        is_private=False,
    )
    return table_name


@task  # type: ignore[misc]
def update_aggregate_query(table_name: str, config: Config) -> None:
    """
    Query example:
    WITH aggregate AS (
        SELECT * FROM dune.cowprotocol.order_rewards_1
        UNION ALL
        SELECT * FROM dune.cowprotocol.order_rewards_2
    )

    SELECT DISTINCT * FROM aggregate;
    """

    logger = get_run_logger()
    dune = DuneClient.from_env()
    query = dune.get_query(config.dune_query_id)  # type: ignore[attr-defined]
    sql_query = query.sql

    if table_name not in sql_query:
        logger.info(f"Table name not found, updating table with {table_name}")
        insertion_point = insertion_point = sql_query.rfind(")")
        updated_sql_query = (
            sql_query[:insertion_point].strip()
            + f"\n    UNION ALL\n    SELECT * FROM dune.cowprotocol.dataset_{table_name}\n"
            + sql_query[insertion_point:]
        )
        dune.update_query(  # type: ignore[attr-defined]
            query_sql=updated_sql_query, query_id=config.dune_query_id
        )
    else:
        logger.info("Table already in query, not updating query")
