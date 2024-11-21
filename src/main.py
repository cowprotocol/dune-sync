"""Main Entry point for app_hash sync"""
import argparse
import asyncio
import os
from dataclasses import dataclass
from pathlib import Path

from dotenv import load_dotenv
from dune_client.client import DuneClient
from web3 import Web3

from src.fetch.orderbook import OrderbookFetcher
from src.logger import set_log
from src.models.tables import SyncTable
from src.post.aws import AWSClient
from src.sync import sync_app_data
from src.sync import sync_price_feed
from src.sync.config import SyncConfig, AppDataSyncConfig, PriceFeedSyncConfig
from src.sync.order_rewards import sync_order_rewards, sync_batch_rewards
from src.sync.batch_data import sync_batch_data

log = set_log(__name__)


@dataclass
class ScriptArgs:
    """Runtime arguments' parser/initializer"""

    dry_run: bool
    sync_table: SyncTable

    def __init__(self) -> None:
        parser = argparse.ArgumentParser("Dune Community Sources Sync")
        parser.add_argument(
            "--sync-table",
            type=SyncTable,
            required=True,
            choices=list(SyncTable),
        )
        parser.add_argument(
            "--dry-run",
            type=bool,
            help="Flag indicating whether script should not post files to AWS or not",
            default=False,
        )
        arguments, _ = parser.parse_known_args()
        self.sync_table: SyncTable = arguments.sync_table
        self.dry_run: bool = arguments.dry_run


def main() -> None:
    """
    Main function
    """
    load_dotenv()
    args = ScriptArgs()
    dune = DuneClient(
        api_key=os.environ["DUNE_API_KEY"],
        request_timeout=float(os.environ.get("DUNE_API_REQUEST_TIMEOUT", 10)),
    )
    orderbook = OrderbookFetcher()
    web3 = Web3(Web3.HTTPProvider(os.environ.get("NODE_URL")))

    if args.sync_table == SyncTable.APP_DATA:
        table = os.environ["APP_DATA_TARGET_TABLE"]
        assert table, "APP_DATA sync needs a APP_DATA_TARGET_TABLE env"
        asyncio.run(
            sync_app_data(
                orderbook,
                dune=dune,
                config=AppDataSyncConfig(table),
                dry_run=args.dry_run,
            )
        )
    elif args.sync_table == SyncTable.PRICE_FEED:
        table = os.environ["PRICE_FEED_TARGET_TABLE"]
        assert table, "PRICE FEED sync needs a PRICE_FEED_TARGET_TABLE env"
        asyncio.run(
            sync_price_feed(
                orderbook,
                dune=dune,
                config=PriceFeedSyncConfig(table),
                dry_run=args.dry_run,
            )
        )
    elif args.sync_table == SyncTable.ORDER_REWARDS:
        aws = AWSClient.new_from_environment()
        volume_path = Path(os.environ["VOLUME_PATH"])
        sync_order_rewards(
            aws,
            config=SyncConfig(volume_path),
            fetcher=orderbook,
            dry_run=args.dry_run,
        )
    elif args.sync_table == SyncTable.BATCH_REWARDS:
        aws = AWSClient.new_from_environment()
        volume_path = Path(os.environ["VOLUME_PATH"])
        sync_batch_rewards(
            aws,
            config=SyncConfig(volume_path),
            fetcher=orderbook,
            dry_run=args.dry_run,
        )
    elif args.sync_table == SyncTable.BATCH_DATA:
        table = os.environ["BATCH_DATA_TARGET_TABLE"]
        assert table, "BATCH DATA sync needs a BATCH_DATA_TARGET_TABLE env"
        asyncio.run(
            sync_batch_data(
                web3,
                orderbook,
                dune=dune,
                config=PriceFeedSyncConfig(table),
                dry_run=args.dry_run,
            )
        )
    else:
        log.error(f"unsupported sync_table '{args.sync_table}'")


if __name__ == "__main__":
    main()
