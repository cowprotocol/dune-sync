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
class AppDataSyncConfig(SyncConfig):
    """Additional data field for app data sync."""

    missing_files_name: str = "missing_app_hashes.json"
