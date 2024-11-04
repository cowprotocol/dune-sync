"""Prefect Deployment for Order Rewards Data"""
# pylint: disable=import-error
from prefect import flow  # type: ignore

from src.deploy_prefect.tasks import (
    get_block_range,
    fetch_orderbook,
    cast_orderbook_to_dune_string,
    upload_data_to_dune,
    update_aggregate_query,
)
from src.deploy_prefect.models import ENV, CHAIN, Config


@flow(retries=3, retry_delay_seconds=60, log_prints=True)  # type: ignore[misc]
def dev_order_rewards() -> None:
    """Defines a flow for updating the order_rewards table"""
    config = Config(CHAIN.MAINNET, ENV.DEV)

    blockrange = get_block_range()
    orderbook = fetch_orderbook(blockrange)
    data = cast_orderbook_to_dune_string(orderbook)
    table_name = upload_data_to_dune(data, blockrange.block_from, blockrange.block_to)
    update_aggregate_query(table_name, config)


@flow(retries=3, retry_delay_seconds=60, log_prints=True)  # type: ignore[misc]
def prod_order_rewards() -> None:
    """Defines a flow for updating the order_rewards table"""
    config = Config(CHAIN.MAINNET, ENV.PROD)
    blockrange = get_block_range()
    orderbook = fetch_orderbook(blockrange)
    data = cast_orderbook_to_dune_string(orderbook)
    table_name = upload_data_to_dune(data, blockrange.block_from, blockrange.block_to)
    update_aggregate_query(table_name, config)
