"""Main Entry point for app_hash sync"""
import argparse
import asyncio
import logging.config
import os
from dataclasses import dataclass
from enum import Enum
from pathlib import Path

from dotenv import load_dotenv

from src.environment import PROJECT_ROOT
from src.fetch.orderbook import OrderbookFetcher
from src.sync import sync_app_data
from src.fetch.dune import DuneFetcher
from src.sync.config import SyncConfig, AWSData
from src.sync.order_rewards import sync_order_rewards

log = logging.getLogger(__name__)
logging.basicConfig(format="%(asctime)s %(levelname)s %(name)s %(message)s")
log.setLevel(logging.DEBUG)


class SyncTable(Enum):
    """Enum for Deployment Supported Table Sync"""

    APP_DATA = "app_data"
    ORDER_REWARDS = "order_rewards"

    def __str__(self) -> str:
        return str(self.value)


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
        arguments = parser.parse_args()
        self.sync_table: SyncTable = arguments.sync_table
        self.dry_run: bool = arguments.dry_run


if __name__ == "__main__":
    load_dotenv()
    aws = AWSData(
        internal_role=os.environ["AWS_INTERNAL_ROLE"],
        external_role=os.environ["AWS_EXTERNAL_ROLE"],
        external_id=os.environ["AWS_EXTERNAL_ID"],
        bucket=os.environ["AWS_BUCKET"],
    )
    volume_path = PROJECT_ROOT / Path(os.environ.get("VOLUME_PATH", "data"))

    args = ScriptArgs()
    # TODO - pass dry-run into runners!
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
