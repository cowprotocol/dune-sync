"""Prefect Deployment for Order Rewards Data"""

import os
from io import StringIO
from datetime import datetime, timedelta, timezone

import requests
import pandas as pd
from dotenv import load_dotenv
from dune_client.client import DuneClient

# pylint: disable=import-error
from prefect import flow, task, get_run_logger  # type: ignore

# pylint: disable=import-error
from prefect_github.repository import GitHubRepository  # type: ignore

from src.models.block_range import BlockRange
from src.fetch.orderbook import OrderbookFetcher

load_dotenv()


def get_last_monday_midnight_utc() -> int:
    """Get the timestamp of last monday at midnight UTC"""
    now = datetime.now(timezone.utc)
    current_weekday = now.weekday()
    days_since_last_monday = current_weekday if current_weekday != 0 else 7
    last_monday = now - timedelta(days=days_since_last_monday)
    last_monday_midnight = last_monday.replace(
        hour=0, minute=0, second=0, microsecond=0
    )
    timestamp = int(last_monday_midnight.timestamp())
    return timestamp


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
def upload_data_to_dune(data: str, block_start: int, block_end: int) -> str:
    """
    Uploads the order rewards data to Dune,
    either creating a new query or updating an existing one
    """
    table_name = f"order_rewards_{block_start}"
    dune = DuneClient.from_env()
    dune.upload_csv(  # type: ignore[attr-defined]
        data=data,
        description=f"Order rewards data for blocks {block_start}-{block_end}",
        table_name=table_name,
        is_private=False,
    )
    return table_name


@task  # type: ignore[misc]
def update_aggregate_query(table_name: str) -> None:
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
    query_id = os.environ["AGGREGATE_QUERY_ID"]
    query = dune.get_query(query_id)  # type: ignore[attr-defined]
    sql_query = query.sql

    if table_name not in sql_query:
        logger.info(f"Table name not found, updating table with {table_name}")
        insertion_point = insertion_point = sql_query.rfind(")")
        updated_sql_query = (
            sql_query[:insertion_point].strip()
            + f"\n    UNION ALL\n    SELECT * FROM {table_name}\n"
            + sql_query[insertion_point:]
        )
        dune.update_query(  # type: ignore[attr-defined]
            query_sql=updated_sql_query, query_id=query_id
        )
    else:
        logger.info("Table already in query, not updating query")


@flow(retries=3, retry_delay_seconds=60, log_prints=True)  # type: ignore[misc]
def order_rewards() -> None:
    """Defines a flow for updating the order_rewards table"""
    blockrange = get_block_range()
    orderbook = fetch_orderbook(blockrange)
    data = cast_orderbook_to_dune_string(orderbook)
    table_name = upload_data_to_dune(data, blockrange.block_from, blockrange.block_to)
    update_aggregate_query(table_name)


if __name__ == "__main__":
    github_repository_block = GitHubRepository.load("dune-sync")
    deployment = order_rewards.deploy(
        flow=order_rewards,
        name="dune-sync-order-rewards",
        cron="0 */3 * * *",  # Once every 3 hours
        storage=github_repository_block,
        tags=["solver", "dune-sync"],
        description="Run the dune sync order_rewards query",
        version="0.0.1",
    )
    deployment.apply()
