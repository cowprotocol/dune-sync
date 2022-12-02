"""Configuration details for sync jobs"""
from __future__ import annotations
import os
from dataclasses import dataclass
from pathlib import Path

from dotenv import load_dotenv


@dataclass
class AWSData:
    """Data Class containing AWS specific details"""

    internal_role: str
    external_role: str
    external_id: str
    bucket: str

    @classmethod
    def new_from_environment(cls) -> AWSData:
        """Class construct from environment variables"""
        load_dotenv()
        return cls(
            internal_role=os.environ["AWS_INTERNAL_ROLE"],
            external_role=os.environ["AWS_EXTERNAL_ROLE"],
            external_id=os.environ["AWS_EXTERNAL_ID"],
            bucket=os.environ["AWS_BUCKET"],
        )

    @classmethod
    def empty(cls) -> AWSData:
        """Empty class constructor"""
        return cls(
            internal_role="",
            external_role="",
            external_id="",
            bucket="",
        )


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
