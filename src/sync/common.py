"""Shared methods between both sync scripts."""
from datetime import datetime, timezone
import time
from dateutil import tz
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


def find_block_with_timestamp(node, time_stamp):
    """
    This implements binary search and returns the smallest block number
    whose timestamp is at least as large as the time_stamp argument passed in the function
    """
    block_found = False
    end_block_number = node.eth.get_block("finalized").number
    start_block_number = 1
    close_in_seconds = 30

    while not block_found:
        mid_block_number = (start_block_number + end_block_number) // 2
        block = node.eth.get_block(mid_block_number)
        block_time = block.timestamp
        difference_in_seconds = int((time_stamp - block_time))

        if abs(difference_in_seconds) < close_in_seconds:
            block_found = True
            continue

        if difference_in_seconds < 0:
            end_block_number = mid_block_number - 1
        else:
            start_block_number = mid_block_number + 1

    ## we now brute-force to ensure we have found the right block
    for b in range(mid_block_number - 100, mid_block_number + 100):
        block = node.eth.get_block(b)
        block_time_stamp = block.timestamp
        if block_time_stamp >= time_stamp:
            return block.number


def compute_block_range_from_timestamps(node, start_timestamp, end_timestamp):
    start_block = find_block_with_timestamp(node, start_timestamp)
    end_block = find_block_with_timestamp(node, end_timestamp)
    if node.eth.get_block(end_block).timestamp > end_timestamp:
        end_block = end_block - 1
    return start_block, end_block


def compute_block_and_month_range(node: Web3):
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
            previous_month = f"{current_month_end_datetime.year}_{current_month_end_datetime.month - 1}"
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
