"""Main Entry point for app_hash sync"""
import csv
import logging.config
import os.path

from dune_client.file.interface import FileIO
from pandas import DataFrame

from src.fetch.orderbook import OrderbookFetcher
from src.models.block_range import BlockRange
from src.sync.common import last_sync_block, aws_login_and_upload
from src.sync.config import SyncConfig

log = logging.getLogger(__name__)
logging.basicConfig(format="%(asctime)s %(levelname)s %(name)s %(message)s")
log.setLevel(logging.DEBUG)


class RecordHandler:  # pylint:disable=too-few-public-methods
    """
    This class is responsible for consuming new dune records and missing values from previous runs
    it attempts to fetch content for them and filters them into "found" and "not found" as necessary
    """

    def __init__(
        self,
        block_range: BlockRange,
        config: SyncConfig,
        order_rewards: DataFrame,
    ):
        self.config = config
        self.block_range = block_range

        self.file_path = config.volume_path / config.table_name
        self.content_filename = f"cow_{block_range.block_to}.json"
        log.info(f"Handling {len(order_rewards)} new records")
        self.order_rewards = order_rewards

    def _write_found_content(self) -> None:
        self.order_rewards.to_json(
            os.path.join(self.file_path, self.content_filename),
            orient="records",
            lines=True,
        )

    def _write_sync_data(self) -> None:
        # Only write these if upload was successful.
        config = self.config
        column = config.sync_column
        with open(
            os.path.join(self.file_path, config.sync_file), "w", encoding="utf-8"
        ) as sync_file:
            writer = csv.DictWriter(sync_file, fieldnames=[column], lineterminator="\n")
            writer.writeheader()
            writer.writerows([{column: str(self.block_range.block_to)}])

    def write_and_upload_content(self, dry_run: bool) -> None:
        """
        - Writes self.order_rewards to persistent volume,
        - attempts to upload to AWS and
        - records last sync block on volume.
        When dryrun flag is enabled, does not upload to IPFS.
        """
        self._write_found_content()

        if len(self.order_rewards) > 0 and not dry_run:
            aws_login_and_upload(
                config=self.config,
                path=self.file_path,
                filename=self.content_filename,
            )
            log.info(
                f"Order Rewards Sync for block range {self.block_range} complete: "
                f"synced {len(self.order_rewards)} records"
            )
        else:
            log.info(
                f"No new Order Rewards for block range {self.block_range}: no sync necessary"
            )

        self._write_sync_data()


def sync_order_rewards(
    fetcher: OrderbookFetcher, config: SyncConfig, dry_run: bool
) -> None:
    """App Data Sync Logic"""
    # TODO - assert legit configuration before proceeding!
    table_name = config.table_name

    block_range = BlockRange(
        block_from=last_sync_block(
            file_manager=FileIO(config.volume_path / table_name),
            last_block_file=config.sync_file,
            column=config.sync_column,
            genesis_block=15719994,  # First Recorded Order Reward block
        ),
        block_to=fetcher.get_latest_block(),
    )
    record_handler = RecordHandler(
        block_range, config, order_rewards=fetcher.get_orderbook_rewards(block_range)
    )

    record_handler.write_and_upload_content(dry_run)
    log.info("order_rewards sync run completed successfully")
