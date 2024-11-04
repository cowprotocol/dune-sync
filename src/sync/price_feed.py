"""Main Entry point for price feed sync"""

from dune_client.client import DuneClient

from src.fetch.orderbook import OrderbookFetcher
from src.logger import set_log
from src.sync.config import PriceFeedSyncConfig

log = set_log(__name__)


async def sync_price_feed(
    orderbook: OrderbookFetcher,
    dune: DuneClient,
    config: PriceFeedSyncConfig,
    dry_run: bool,
) -> None:
    """Price Feed Sync Logic"""
    prices = orderbook.get_price_feed()
    if not dry_run:
        dune.upload_csv(
            data=prices.to_csv(index=False),
            table_name=config.table,
            description=config.description,
            is_private=False,
        )
    log.info("price feed sync run completed successfully")
