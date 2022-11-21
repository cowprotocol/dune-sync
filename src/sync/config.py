"""Configuration details for sync jobs"""
from dataclasses import dataclass
from pathlib import Path


@dataclass
class AppDataSyncConfig:
    """
    This data class contains all the credentials and volume paths
    required to sync with both a persistent volume and Dune's S3 Buckets.
    """

    aws_role: str
    aws_bucket: str
    volume_path: Path
    table_name: str = "app_data"
    # File System
    missing_files_name: str = "missing_app_hashes.json"
    sync_file: str = "sync_block.csv"
    sync_column: str = "last_synced_block"
