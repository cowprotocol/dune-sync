"""Main Entry point for price feed sync"""
from dune_client.client import DuneClient
import web3

from src.fetch.orderbook import OrderbookFetcher
from src.logger import set_log
from src.sync.config import BatchDataSyncConfig
from src.sync.common import compute_block_and_month_range
from src.models.block_range import BlockRange


log = set_log(__name__)


async def sync_batch_data(
    node: web3,
    orderbook: OrderbookFetcher,
    dune: DuneClient,
    config: BatchDataSyncConfig,
    dry_run: bool,
) -> None:
    """Batch data Sync Logic"""
    block_range_list, months_list = compute_block_and_month_range(node)
    for i in range(len(block_range_list)):
        start_block = block_range_list[i][0]
        end_block = block_range_list[i][1]
        table_name = "test_batch_rewards_ethereum_" + months_list[i]
        block_range = BlockRange(block_from=start_block, block_to=end_block)
        log.info(
            f"About to process block range ({start_block}, {end_block}) for month {months_list[i]}"
        )
        batch_data = orderbook.get_batch_data(block_range)
        log.info("SQL query successfully executed. About to initiate upload to Dune.")
        if not dry_run:
            dune.upload_csv(
                data=batch_data.to_csv(index=False),
                table_name=table_name,  # config.table,
                description=config.description,
                is_private=False,
            )
        log.info(
            f"batch data sync run completed successfully for month {months_list[i]}"
        )
