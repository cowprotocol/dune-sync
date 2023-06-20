"""Shared methods between both sync scripts."""

from src.logger import set_log
from src.models.tables import SyncTable
from src.post.aws import AWSClient

log = set_log(__name__)


def last_sync_block(aws: AWSClient, table: SyncTable, genesis_block: int = 0) -> int:
    """Attempts to get last sync block from AWS Bucket files, otherwise uses genesis"""
    try:
        block_from = aws.last_sync_block(table)
    except FileNotFoundError:
        log.warning(
            f"last sync could not be evaluated from AWS, using genesis block {genesis_block}"
        )
        block_from = genesis_block

    return block_from
