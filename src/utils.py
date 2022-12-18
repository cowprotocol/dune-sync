"""Basic reusable utility functions"""
import os
import re
from datetime import datetime, timezone

from src.environment import QUERY_PATH


def open_query(filename: str) -> str:
    """Opens `filename` and returns as string"""
    with open(query_file(filename), "r", encoding="utf-8") as file:
        return file.read()


def query_file(filename: str) -> str:
    """Returns proper path for filename in QUERY_PATH"""
    return os.path.join(QUERY_PATH, filename)


def valid_address(address: str) -> bool:
    """Validates Ethereum addresses"""
    match_result = re.match(
        pattern=r"^(0x)?[0-9a-f]{40}$", string=address, flags=re.IGNORECASE
    )
    return match_result is not None


def utc_now() -> datetime:
    """Returns current UTC time"""
    return datetime.now(tz=timezone.utc)
