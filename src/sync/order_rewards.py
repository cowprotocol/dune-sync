"""Main Entry point for app_hash sync"""
import csv
import os.path

from pandas import DataFrame

from src.fetch.orderbook import OrderbookFetcher
from src.logger import set_log
from src.models.block_range import BlockRange
from src.models.tables import SyncTable
from src.post.aws import AWSClient
from src.sync.common import last_sync_block
from src.sync.config import SyncConfig
from src.sync.record_handler import RecordHandler
from src.sync.upload_handler import UploadHandler

log = set_log(__name__)

SYNC_TABLE = SyncTable.ORDER_REWARDS


class OrderbookDataHandler(RecordHandler):  # pylint:disable=too-few-public-methods
    """
    This class is responsible for consuming new dune records and missing values from previous runs
    it attempts to fetch content for them and filters them into "found" and "not found" as necessary
    """

    def __init__(
        self, block_range: BlockRange, config: SyncConfig, order_rewards: DataFrame
    ):
        super().__init__(block_range, SYNC_TABLE, config)
        log.info(f"Handling {len(order_rewards)} new records")
        self.order_rewards = order_rewards

    def num_records(self) -> int:
        return len(self.order_rewards)

    def write_found_content(self) -> None:
        path = self.file_path
        if not os.path.exists(path):
            log.info(f"creating write path {path}")
            os.makedirs(path)

        self.order_rewards.to_json(
            os.path.join(self.file_path, self.content_filename),
            orient="records",
            lines=True,
        )

    def write_sync_data(self) -> None:
        # Only write these if upload was successful.
        config = self.config
        column = config.sync_column
        with open(
            os.path.join(self.file_path, config.sync_file), "w", encoding="utf-8"
        ) as sync_file:
            writer = csv.DictWriter(sync_file, fieldnames=[column], lineterminator="\n")
            writer.writeheader()
            writer.writerows([{column: str(self.block_range.block_to)}])


def sync_order_rewards(
    aws: AWSClient, fetcher: OrderbookFetcher, config: SyncConfig, dry_run: bool
) -> None:
    """App Data Sync Logic"""

    block_range = BlockRange(
        block_from=last_sync_block(
            # TODO - we should always use this.
            aws,
            table=SYNC_TABLE,
            genesis_block=15719994,  # First Recorded Order Reward block
        ),
        block_to=fetcher.get_latest_block(),
    )
    record_handler = OrderbookDataHandler(
        block_range, config, order_rewards=fetcher.get_orderbook_rewards(block_range)
    )
    UploadHandler(aws, record_handler, table=SYNC_TABLE).write_and_upload_content(
        dry_run
    )
    log.info("order_rewards sync run completed successfully")
