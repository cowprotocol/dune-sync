"""Main Entry point for app_hash sync"""
import argparse
import asyncio
import logging.config
import os
from dataclasses import dataclass
from pathlib import Path

from dotenv import load_dotenv

from src.fetch.dune import DuneFetcher
from src.fetch.orderbook import OrderbookFetcher
from src.sync import sync_app_data
from src.sync.config import SyncConfig, AWSData
from src.sync.order_rewards import sync_order_rewards
from src.sync.tables import SyncTable

log = logging.getLogger(__name__)
logging.basicConfig(format="%(asctime)s %(levelname)s %(name)s %(message)s")
log.setLevel(logging.DEBUG)


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
    aws = AWSData.empty() if args.dry_run else AWSData.new_from_environment()

    if args.sync_table == SyncTable.APP_DATA:
        asyncio.run(
            sync_app_data(
                dune=DuneFetcher(os.environ["DUNE_API_KEY"]),
                config=SyncConfig(
                    aws,
                    volume_path,
                    table_name=str(args.sync_table),
                ),
                missing_file_name="missing_app_hashes.json",
                dry_run=args.dry_run,
            )
        )
    elif args.sync_table == SyncTable.ORDER_REWARDS:
        sync_order_rewards(
            fetcher=OrderbookFetcher(),
            config=SyncConfig(
                aws,
                volume_path,
                table_name=str(args.sync_table),
            ),
            dry_run=args.dry_run,
        )
    else:
        log.error(f"unsupported sync_table '{args.sync_table}'")
