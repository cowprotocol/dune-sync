"""Main Entry point for token_imbalance sync"""
from dune_client.file.interface import FileIO

from src.fetch.postgres import PostgresFetcher
from src.logger import set_log
from src.models.block_range import BlockRange
from src.models.tables import SyncTable
from src.models.token_imbalance_schema import TokenImbalance
from src.post.aws import AWSClient
from src.sync.common import last_sync_block
from src.sync.config import SyncConfig
from src.sync.orderbook_data import OrderbookDataHandler
from src.sync.upload_handler import UploadHandler

log = set_log(__name__)


def sync_internal_imbalance(
    aws: AWSClient, fetcher: PostgresFetcher, config: SyncConfig, dry_run: bool
) -> None:
    """Token Imbalance Sync Logic"""
    sync_table = SyncTable.INTERNAL_IMBALANCE
    block_range = BlockRange(
        block_from=last_sync_block(
            aws,
            table=sync_table,
            # The first block for which solver competitions
            # are available in production orderbook:
            # select * from solver_competitions where id = 1;
            genesis_block=15173540,
        ),
        block_to=fetcher.get_latest_block(),
    )
    # TODO - Gap Detection (find missing txHashes and ensure they are accounted for!)
    record_handler = OrderbookDataHandler(
        file_manager=FileIO(config.volume_path / str(sync_table)),
        block_range=block_range,
        config=config,
        data_list=TokenImbalance.from_pdf_to_dune_records(
            fetcher.get_internal_imbalances(block_range)
        ),
        sync_table=sync_table,
    )
    UploadHandler(aws, record_handler, table=sync_table).write_and_upload_content(
        dry_run
    )
    log.info(f"{sync_table} sync run completed successfully")
