import os
import sys
import logging
import pandas as pd

deployments_path = os.path.abspath("/deployments")
if deployments_path not in sys.path:
    sys.path.insert(0, deployments_path)

from typing import Any
from dotenv import load_dotenv
from sqlalchemy import create_engine, text
from prefect import flow, task, get_run_logger
from prefect.logging import get_logger

from src.models.block_range import BlockRange
from src.fetch.orderbook import OrderbookFetcher
from src.sync.order_rewards import sync_order_rewards
from src.models.order_rewards_schema import OrderRewards


@task
def get_block_range() -> BlockRange:
    start = 20913994
    end = 20921169
    blockrange = BlockRange(block_from=start, block_to=end)
    return blockrange

@task
def fetch_orderbook(blockrange: BlockRange) -> pd.DataFrame:
    orderbook = OrderbookFetcher()
    return orderbook.get_order_rewards(blockrange)

@task
def cast_orderbook_to_dune_records(orderbook: pd.DataFrame) -> list[dict[str, Any]]:
    return OrderRewards.from_pdf_to_dune_records(orderbook)

@task
def upload_data_to_dune():
    pass

@task
def update_aggregate_query():
    pass


@flow(retries=3, retry_delay_seconds=60)
def order_rewards():
    blockrange = get_block_range()
    orderbook = fetch_orderbook(blockrange)
    dune_records = cast_orderbook_to_dune_records(orderbook)


if __name__ == "__main__":
    order_rewards.serve(
        name="dune-sync-prod-order-rewards",
        cron="*/10 * * * *", # Every 10 minutes
        tags=["solver", "dune-sync"],
        description="Run the dune sync order_rewards query",
        version="0.0.1",
        )
