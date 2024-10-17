"""Code for Local Testing of Order Rewards Deployment"""

import os
# pylint: disable=import-error
from prefect import flow # type: ignore
from dotenv import load_dotenv
from src.deploy_prefect.deployment import (
    get_block_range,
    fetch_orderbook,
    cast_orderbook_to_dune_string,
    upload_data_to_dune,
)

load_dotenv()


@flow()
def order_rewards() -> None:
    """Local flow for testing the order rewards deployment"""
    blockrange = get_block_range()
    orderbook = fetch_orderbook(blockrange)
    data = cast_orderbook_to_dune_string(orderbook)
    upload_data_to_dune(data, blockrange.block_from, blockrange.block_to)


if __name__ == "__main__":
    # Not ideal, but this script is for local testing
    os.environ["PREFECT_SERVER_API_HOST"] = "0.0.0.0"
    os.environ["PREFECT_SERVER__TELEMETRY__ENABLED"] = "false"
    os.environ["PREFECT_API_URL"] = "http://localhost:4200/api"
    os.environ["PREFECT_LOGGING_LEVEL"] = "INFO"

    order_rewards.serve(
        name="dune-sync-prod-order-rewards",
        cron="*/30 * * * *",  # Every 30 minutes
        tags=["solver", "dune-sync"],
        description="Run the dune sync order_rewards query",
        version="0.0.1",
    )
