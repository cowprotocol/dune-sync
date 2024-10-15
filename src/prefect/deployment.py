import os
import re
import sys
import logging
import requests
import pandas as pd
from io import StringIO
from dotenv import load_dotenv
from datetime import datetime, timedelta, timezone
from dune_client.client import DuneClient

from typing import Any
from prefect import flow, task, get_run_logger
from prefect_github.repository import GitHubRepository

from src.models.block_range import BlockRange
from src.fetch.orderbook import OrderbookFetcher
from src.models.order_rewards_schema import OrderRewards

load_dotenv()

def get_last_monday_midnight_utc():
    now = datetime.now(timezone.utc)
    current_weekday = now.weekday()
    days_since_last_monday = current_weekday if current_weekday != 0 else 7
    last_monday = now - timedelta(days=days_since_last_monday)
    last_monday_midnight = last_monday.replace(hour=0, minute=0, second=0, microsecond=0)
    timestamp = int(last_monday_midnight.timestamp())
    return timestamp


@task
def get_block_range() -> BlockRange:
    etherscan_api = "https://api.etherscan.io/api"
    api_key = os.environ["ETHERSCAN_API_KEY"]
    start = requests.get(
            etherscan_api, 
            {
              "module": "block",
              "action": "getblocknobytime",
              "timestamp": get_last_monday_midnight_utc(),
              "closest": "before",
              "apikey": api_key
            }
    ).json().get('result')
    end = requests.get(
            etherscan_api, 
            {
              "module": "block",
              "action": "getblocknobytime",
              "timestamp": int(datetime.now(timezone.utc).timestamp()),
              "closest": "before",
              "apikey": api_key
            }
    ).json().get('result')

    blockrange = BlockRange(block_from=start, block_to=end)
    return blockrange

@task
def fetch_orderbook(blockrange: BlockRange) -> pd.DataFrame:
    orderbook = OrderbookFetcher()
    return orderbook.get_order_rewards(blockrange)


@task
def cast_orderbook_to_dune_string(orderbook: pd.DataFrame) -> str:
    csv_buffer = StringIO()
    orderbook.to_csv(csv_buffer, index=False)
    return csv_buffer.getvalue()


@task
def upload_data_to_dune(data: str, block_start: int, block_end: int):
    table_name = f"order_rewards_{block_start}"
    dune = DuneClient.from_env()
    dune.upload_csv(
            data=data,
            description=f"Order rewards data for blocks {block_start}-{block_end}",
            table_name=table_name,
            is_private=False,
    )
    return table_name


@task
def update_aggregate_query(table_name: str):
    """
    Query example:
    WITH aggregate AS (
        SELECT * FROM dune.cowswapbram.dataset_order_rewards_20921069_20921169
        UNION ALL
        SELECT * FROM dune.cowswapbram.dataset_testtable
    )

    SELECT DISTINCT * FROM aggregate;
    """

    logger = get_run_logger()
    dune = DuneClient.from_env()
    query_id = os.environ['AGGREGATE_QUERY_ID']
    query = dune.get_query(query_id)
    sql_query = query.sql

    if table_name not in sql_query:
        logger.info(f"Table name not found, updating table with {table_name}")
        insertion_point = insertion_point = sql_query.rfind(")")
        updated_sql_query = (
            sql_query[:insertion_point].strip() +
            f"\n    UNION ALL\n    SELECT * FROM {table_name}\n" +
            sql_query[insertion_point:]
        )
        dune.update_query(query_sql=updated_sql_query, query_id=query_id)
    else:
        logger.info(f"Table already in query, not updating query")


@flow(retries=3, retry_delay_seconds=60, log_prints=True)
def order_rewards():
    logger = get_run_logger()
    blockrange = get_block_range()
    orderbook = fetch_orderbook(blockrange)
    data = cast_orderbook_to_dune_string(orderbook)
    table_name = upload_data_to_dune(data, blockrange.block_from, blockrange.block_to)
    update_aggregate_query(table_name)


if __name__ == "__main__":
    github_repository_block = GitHubRepository.load("dune-sync")
    deployment = order_rewards.deploy(
        flow=order_rewards,
        name="dune-sync-prod-order-rewards",
        schedule=(CronSchedule(cron="0 */3 * * *")), # Once every 3 hours
        storage=github_block,
        tags=["solver", "dune-sync"],
        description="Run the dune sync order_rewards query",
        version="0.0.1",
    )
    deployment.apply()
