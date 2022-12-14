"""Main Entry point for app_hash sync"""
import argparse
import asyncio
import logging.config
import os
import shutil
from dataclasses import dataclass
from pathlib import Path

from dotenv import load_dotenv

from src.fetch.dune import DuneFetcher
from src.fetch.orderbook import OrderbookFetcher
from src.post.aws import AWSClient
from src.sync import sync_app_data
from src.sync.config import SyncConfig, AppDataSyncConfig
from src.sync.order_rewards import sync_order_rewards
from src.models.tables import SyncTable

log = logging.getLogger(__name__)
logging.basicConfig(format="%(asctime)s %(levelname)s %(name)s %(message)s")
log.setLevel(logging.DEBUG)


@dataclass
class ScriptArgs:
    """Runtime arguments' parser/initializer"""

    dry_run: bool
    hard_reset: bool
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
        parser.add_argument(
            "--hard-reset",
            type=bool,
            help="Flag indicating whether the existing sync table data should "
            "be deleted from the bucket and rebuilt from scratch",
            default=False,
        )
        arguments, _ = parser.parse_known_args()
        self.sync_table: SyncTable = arguments.sync_table
        self.dry_run: bool = arguments.dry_run
        self.hard_reset: bool = arguments.hard_reset


if __name__ == "__main__":
    load_dotenv()
    volume_path = Path(os.environ["VOLUME_PATH"])
    args = ScriptArgs()
    aws = AWSClient.new_from_environment()
    if args.hard_reset and not args.dry_run:
        # Drop Data from AWS bucket
        aws.delete_all(args.sync_table)
        # drop backup data from volume path
        shutil.rmtree(volume_path / str(args.sync_table))

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
                dry_run=args.dry_run,
            )
        )
    elif args.sync_table == SyncTable.ORDER_REWARDS:
        sync_order_rewards(
            aws,
            fetcher=OrderbookFetcher(),
            config=SyncConfig(volume_path),
            dry_run=args.dry_run,
        )
    else:
        log.error(f"unsupported sync_table '{args.sync_table}'")
