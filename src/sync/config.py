"""Configuration details for sync jobs"""
from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path


@dataclass
class SyncConfig:
    """
    This data class contains all the credentials and volume paths
    required to sync with both a persistent volume and Dune's S3 Buckets.
    """

    volume_path: Path
    # File System
    sync_file: str = "sync_block.csv"
    sync_column: str = "last_synced_block"


@dataclass
class AppDataSyncConfig:
    """Configuration for app data sync."""

    # The name of the table to upload to
    table: str = "app_data_test"
    # Description of the table (for creation)
    description: str = (
        "Table containing known CoW Protocol appData hashes and their pre-images"
    )


@dataclass
class PriceFeedSyncConfig:
    """Configuration for price feed sync."""

    # The name of the table to upload to
    table: str = "price_feed_test"
    # Description of the table (for creation)
    description: str = (
        "Table containing prices and timestamps from multiple price feeds"
    )
