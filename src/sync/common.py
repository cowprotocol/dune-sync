"""Shared methods between both sync scripts."""
import logging
import os
import sys
from pathlib import Path

from dune_client.file.interface import FileIO
from s3transfer import S3UploadFailedError

from src.post.aws import AWSClient
from src.sync.config import SyncConfig

log = logging.getLogger(__name__)


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


def aws_login_and_upload(config: SyncConfig, path: Path | str, filename: str) -> bool:
    """Creates AWS client session and attempts to upload file"""
    aws_client = AWSClient(
        internal_role=config.aws.internal_role,
        external_role=config.aws.external_role,
        external_id=config.aws.external_id,
    )
    try:
        return aws_client.upload_file(
            filename=os.path.join(path, filename),
            bucket=config.aws.bucket,
            object_key=f"{config.table_name}/{filename}",
        )
    except S3UploadFailedError as err:
        logging.error(err)
        sys.exit(1)
