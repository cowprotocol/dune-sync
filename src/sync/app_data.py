"""Main Entry point for app_hash sync"""

from dune_client.client import DuneClient

from src.fetch.orderbook import OrderbookFetcher
from src.logger import set_log
from src.sync.config import AppDataSyncConfig

log = set_log(__name__)


async def sync_app_data(
    orderbook: OrderbookFetcher,
    dune: DuneClient,
    config: AppDataSyncConfig,
    dry_run: bool,
) -> None:
    """App Data Sync Logic"""
    hashes = orderbook.get_app_hashes()
    if not dry_run:
        dune.upload_csv(
            data=hashes.to_csv(index=False),
            table_name=config.table,
            description=config.description,
            is_private=False,
        )
    log.info("app_data sync run completed successfully")
