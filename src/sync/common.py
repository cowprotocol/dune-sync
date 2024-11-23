"""Shared methods between both sync scripts."""
from datetime import datetime, timezone
from web3 import Web3
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


def node_suffix(network: str) -> str:
    """
    Converts network internal name to name used for nodes and dune tables
    """
    if network == "mainnet":
        return "MAINNET"
    if network == "xdai":
        return "GNOSIS"
    if network == "arbitrum-one":
        return "ARBITRUM"
    return ""


def find_block_with_timestamp(node: Web3, time_stamp: float) -> int:
    """
    This implements binary search and returns the smallest block number
    whose timestamp is at least as large as the time_stamp argument passed in the function
    """
    end_block_number = node.eth.get_block("finalized").number
    start_block_number = 1
    close_in_seconds = 30

    while True:
        mid_block_number = (start_block_number + end_block_number) // 2
        block = node.eth.get_block(mid_block_number)
        block_time = block.timestamp
        difference_in_seconds = int((time_stamp - block_time))

        if abs(difference_in_seconds) < close_in_seconds:
            break

        if difference_in_seconds < 0:
            end_block_number = mid_block_number - 1
        else:
            start_block_number = mid_block_number + 1

    ## we now brute-force to ensure we have found the right block
    for b in range(mid_block_number - 200, mid_block_number + 200):
        block = node.eth.get_block(b)
        block_time_stamp = block.timestamp
        if block_time_stamp >= time_stamp:
            return int(block.number)
    # fallback in case correct block number hasn't been found
    # in that case, we will include some more blocks than necessary
    return mid_block_number + 200


def compute_block_and_month_range(node: Web3):  # pylint: disable=too-many-locals
    """
    This determines the block range and the relevant months
    for which we will compute and upload data on Dune.
    """
    # We first compute the relevant block range
    # Here, we assume that the job runs at least once every 24h
    # Because of that, if it is the first day of month, we also
    # compute the previous month's table just to be on the safe side

    latest_finalized_block = node.eth.get_block("finalized")

    current_month_end_block = latest_finalized_block.number
    current_month_end_timestamp = latest_finalized_block.timestamp

    current_month_end_datetime = datetime.fromtimestamp(
        current_month_end_timestamp, tz=timezone.utc
    )
    current_month_start_datetime = datetime(
        current_month_end_datetime.year, current_month_end_datetime.month, 1, 00, 00
    )
    current_month_start_timestamp = current_month_start_datetime.replace(
        tzinfo=timezone.utc
    ).timestamp()

    current_month_start_block = find_block_with_timestamp(
        node, current_month_start_timestamp
    )

    current_month = (
        f"{current_month_end_datetime.year}_{current_month_end_datetime.month}"
    )
    months_list = [current_month]
    block_range = [(current_month_start_block, current_month_end_block)]
    if current_month_end_datetime.day == 1:
        if current_month_end_datetime.month == 1:
            previous_month = f"{current_month_end_datetime.year - 1}_12"
            previous_month_start_datetime = datetime(
                current_month_end_datetime.year - 1, 12, 1, 00, 00
            )
        else:
            previous_month = f"""{current_month_end_datetime.year}_
                {current_month_end_datetime.month - 1}
            """
            previous_month_start_datetime = datetime(
                current_month_end_datetime.year,
                current_month_end_datetime.month - 1,
                1,
                00,
                00,
            )
        months_list.append(previous_month)
        previous_month_start_timestamp = previous_month_start_datetime.replace(
            tzinfo=timezone.utc
        ).timestamp()
        previous_month_start_block = find_block_with_timestamp(
            node, previous_month_start_timestamp
        )
        previous_month_end_block = current_month_start_block
        block_range.append((previous_month_start_block, previous_month_end_block))

    return block_range, months_list
