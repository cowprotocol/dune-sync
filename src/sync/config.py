"""Configuration details for sync jobs"""
from dataclasses import dataclass
from pathlib import Path


@dataclass
class AWSData:
    """Data Class containing AWS specific details"""

    internal_role: str
    external_role: str
    external_id: str
    bucket: str


@dataclass
class SyncConfig:
    """
    This data class contains all the credentials and volume paths
    required to sync with both a persistent volume and Dune's S3 Buckets.
    """

    aws: AWSData
    volume_path: Path
    table_name: str
    # File System
    sync_file: str = "sync_block.csv"
    sync_column: str = "last_synced_block"


@dataclass
class AppDataSyncConfig(SyncConfig):
    """Additional data field for app data sync."""

    missing_files_name: str = "missing_app_hashes.json"
