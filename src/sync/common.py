"""Shared methods between both sync scripts."""

from dune_client.file.interface import FileIO

from src.logger import set_log

log = set_log(__name__)


def last_sync_block(
    file_manager: FileIO, last_block_file: str, column: str, genesis_block: int = 0
) -> int:
    """Attempts to get last sync block from file, otherwise uses genesis"""
    try:
        block_from = int(file_manager.load_singleton(last_block_file, "csv")[column])
    except FileNotFoundError:
        log.warning(
            f"last sync file {last_block_file} not found, using genesis block {genesis_block}"
        )
        block_from = genesis_block
    except KeyError as err:
        message = (
            f"last sync file {last_block_file} does not contain column header {column}, "
            f"exiting to avoid duplication"
        )
        log.error(message)
        raise RuntimeError(message) from err

    return block_from
