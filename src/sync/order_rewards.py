"""Main Entry point for app_hash sync"""

from dune_client.file.interface import FileIO
from pandas import DataFrame

from src.fetch.orderbook import OrderbookFetcher
from src.logger import set_log
from src.models.block_range import BlockRange
from src.models.order_rewards_schema import OrderRewards
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
        self,
        file_manager: FileIO,
        block_range: BlockRange,
        config: SyncConfig,
        order_rewards: DataFrame,
    ):
        super().__init__(block_range, SYNC_TABLE, config)
        log.info(f"Handling {len(order_rewards)} new records")
        self.file_manager = file_manager
        self.order_rewards = OrderRewards.from_pdf_to_dune_records(order_rewards)

    def num_records(self) -> int:
        return len(self.order_rewards)

    def write_found_content(self) -> None:
        self.file_manager.write_ndjson(
            data=self.order_rewards, name=self.content_filename
        )

    def write_sync_data(self) -> None:
        # Only write these if upload was successful.
        self.file_manager.write_csv(
            data=[{self.config.sync_column: str(self.block_range.block_to)}],
            name=self.config.sync_file,
        )


def sync_order_rewards(
    aws: AWSClient, fetcher: OrderbookFetcher, config: SyncConfig, dry_run: bool
) -> None:
    """App Data Sync Logic"""

    block_range = BlockRange(
        block_from=last_sync_block(
            aws,
            table=SYNC_TABLE,
            genesis_block=15719994,  # First Recorded Order Reward block
        ),
        block_to=fetcher.get_latest_block(),
    )
    record_handler = OrderbookDataHandler(
        file_manager=FileIO(config.volume_path / str(SYNC_TABLE)),
        block_range=block_range,
        config=config,
        order_rewards=fetcher.get_orderbook_rewards(block_range),
    )
    UploadHandler(aws, record_handler, table=SYNC_TABLE).write_and_upload_content(
        dry_run
    )
    log.info("order_rewards sync run completed successfully")
