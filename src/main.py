"""Main Entry point for app_hash sync"""
import argparse
import asyncio
import os
from dataclasses import dataclass
from pathlib import Path

from dotenv import load_dotenv

from src.fetch.dune import DuneFetcher
from src.fetch.orderbook import OrderbookFetcher
from src.logger import set_log
from src.models.tables import SyncTable
from src.post.aws import AWSClient
from src.sync import sync_app_data
from src.sync.config import SyncConfig, AppDataSyncConfig
from src.sync.order_rewards import sync_order_rewards, sync_batch_rewards

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


if __name__ == "__main__":
    load_dotenv()
    volume_path = Path(os.environ["VOLUME_PATH"])
    args = ScriptArgs()
    aws = AWSClient.new_from_environment()

    if args.sync_table == SyncTable.APP_DATA:
        asyncio.run(
            sync_app_data(
                aws,
                dune=DuneFetcher(os.environ["DUNE_API_KEY"]),
                config=AppDataSyncConfig(
                    volume_path=volume_path,
                    missing_files_name="missing_app_hashes.json",
                    max_retries=int(os.environ.get("APP_DATA_MAX_RETRIES", 3)),
                    give_up_threshold=int(
                        os.environ.get("APP_DATA_GIVE_UP_THRESHOLD", 100)
                    ),
                ),
                ipfs_access_key=os.environ["IPFS_ACCESS_KEY"],
                dry_run=args.dry_run,
                chain="mainnet",
            )
        )
        asyncio.run(
            sync_app_data(
                aws,
                dune=DuneFetcher(os.environ["DUNE_API_KEY"]),
                config=AppDataSyncConfig(
                    volume_path=volume_path,
                    missing_files_name="missing_app_hashes_gnosis.json",
                    max_retries=int(os.environ.get("APP_DATA_MAX_RETRIES", 3)),
                    give_up_threshold=int(
                        os.environ.get("APP_DATA_GIVE_UP_THRESHOLD", 100)
                    ),
                ),
                ipfs_access_key=os.environ["IPFS_ACCESS_KEY"],
                dry_run=args.dry_run,
                chain="gnosis",
            )
        )
    elif args.sync_table == SyncTable.ORDER_REWARDS:
        sync_order_rewards(
            aws,
            config=SyncConfig(volume_path),
            fetcher=OrderbookFetcher(),
            dry_run=args.dry_run,
        )
    elif args.sync_table == SyncTable.BATCH_REWARDS:
        sync_batch_rewards(
            aws,
            config=SyncConfig(volume_path),
            fetcher=OrderbookFetcher(),
            dry_run=args.dry_run,
        )
    else:
        log.error(f"unsupported sync_table '{args.sync_table}'")
