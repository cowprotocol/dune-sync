"""Main Entry point for app_hash sync"""
from typing import Any

from dune_client.file.interface import FileIO

from src.fetch.orderbook import OrderbookFetcher
from src.logger import set_log
from src.models.batch_rewards_schema import BatchRewards
from src.models.block_range import BlockRange
from src.models.order_rewards_schema import OrderRewards
from src.models.tables import SyncTable
from src.post.aws import AWSClient
from src.sync.common import last_sync_block
from src.sync.config import SyncConfig
from src.sync.record_handler import RecordHandler
from src.sync.upload_handler import UploadHandler

log = set_log(__name__)


class OrderbookDataHandler(
    RecordHandler
):  # pylint:disable=too-few-public-methods,too-many-arguments
    """
    This class is responsible for consuming new dune records and missing values from previous runs
    it attempts to fetch content for them and filters them into "found" and "not found" as necessary
    """

    def __init__(
        self,
        file_manager: FileIO,
        block_range: BlockRange,
        sync_table: SyncTable,
        config: SyncConfig,
        data_list: list[dict[str, Any]],
    ):
        super().__init__(block_range, sync_table, config)
        log.info(f"Handling {len(data_list)} new records")
        self.file_manager = file_manager
        self.data_list = data_list

    def num_records(self) -> int:
        return len(self.data_list)

    def write_found_content(self) -> None:
        self.file_manager.write_ndjson(data=self.data_list, name=self.content_filename)

    def write_sync_data(self) -> None:
        # Only write these if upload was successful.
        self.file_manager.write_csv(
            data=[{self.config.sync_column: str(self.block_range.block_to)}],
            name=self.config.sync_file,
        )


def sync_order_rewards(
    aws: AWSClient, fetcher: OrderbookFetcher, config: SyncConfig, dry_run: bool
) -> None:
    """Order Rewards Data Sync Logic"""
    sync_table = SyncTable.ORDER_REWARDS
    block_range = BlockRange(
        block_from=last_sync_block(
            aws,
            table=sync_table,
            genesis_block=15719994,  # First Recorded Order Reward block
        ),
        block_to=fetcher.get_latest_block(),
    )
    sync_orderbook_data(
        aws,
        block_range,
        config,
        dry_run,
        sync_table=sync_table,
        data_list=OrderRewards.from_pdf_to_dune_records(
            fetcher.get_order_rewards(block_range)
        ),
    )


def sync_batch_rewards(
    aws: AWSClient, fetcher: OrderbookFetcher, config: SyncConfig, dry_run: bool
) -> None:
    """Batch Reward Sync Logic"""
    sync_table = SyncTable.BATCH_REWARDS
    block_range = BlockRange(
        block_from=last_sync_block(
            aws,
            table=sync_table,
            genesis_block=16862919,  # First Recorded Batch Reward block
        ),
        block_to=fetcher.get_latest_block(),
    )
    sync_orderbook_data(
        aws,
        block_range,
        config,
        dry_run,
        sync_table,
        data_list=BatchRewards.from_pdf_to_dune_records(
            fetcher.get_batch_rewards(block_range)
        ),
    )


def sync_orderbook_data(  # pylint:disable=too-many-arguments
    aws: AWSClient,
    block_range: BlockRange,
    config: SyncConfig,
    dry_run: bool,
    sync_table: SyncTable,
    data_list: list[dict[str, Any]],
) -> None:
    """Generic Orderbook Sync Logic"""
    record_handler = OrderbookDataHandler(
        file_manager=FileIO(config.volume_path / str(sync_table)),
        block_range=block_range,
        config=config,
        data_list=data_list,
        sync_table=sync_table,
    )
    UploadHandler(aws, record_handler, table=sync_table).write_and_upload_content(
        dry_run
    )
    log.info(f"{sync_table} sync run completed successfully")
