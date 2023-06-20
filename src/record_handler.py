"""
Abstraction for New Content Handling
provides a framework for writing new content to disk and posting to AWS
"""
import sys
from typing import Any

from s3transfer import S3UploadFailedError
from src.aws import AWSClient

from src.logger import set_log

log = set_log(__name__)


def last_sync_block(aws: AWSClient, table: str, genesis_block: int = 0) -> int:
    """Attempts to get last sync block from AWS Bucket files, otherwise uses genesis"""
    try:
        block_from = aws.last_sync_block(table)
    except FileNotFoundError:
        log.warning(
            f"last sync could not be evaluated from AWS, using genesis block {genesis_block}"
        )
        block_from = genesis_block

    return block_from


class RecordHandler:

    """
    This class is responsible for consuming new dune records and missing values from previous runs
    it attempts to fetch content for them and filters them into "found" and "not found" as necessary
    """

    def __init__(
        self,
        file_index: int,
        file_prefix: str,
        table: str,
        data_set: list[dict[str, Any]],
    ):
        self.file_index = file_index
        self.file_prefix = file_prefix
        self.table = table
        self.data_set = data_set

    def num_records(self) -> int:
        """Returns number of records to handle"""
        return len(self.data_set)

    @property
    def content_filename(self) -> str:
        """returns filename"""
        return f"{self.file_prefix}_{self.file_index}.json"

    @property
    def object_key(self) -> str:
        """returns object key"""
        return f"{self.table}/{self.content_filename}"

    def _aws_login_and_upload(
        self, aws: AWSClient, data_set: list[dict[str, Any]]
    ) -> bool:
        """Creates AWS client session and attempts to upload file"""
        try:
            return aws.put_object(
                data_set,
                object_key=self.object_key,
            )
        except S3UploadFailedError as err:
            log.error(err)
            sys.exit(1)

    def write_and_upload_content(self, aws: AWSClient, dry_run: bool) -> None:
        """
        - Writes record handlers content to persistent volume,
        - attempts to upload to AWS and
        - records last sync block on volume.
        When dryrun flag is enabled, does not upload to IPFS.
        """
        count = self.num_records()
        if count > 0:
            log.info(
                f"posting {count} new {self.table} records for file index {self.file_index}"
            )
            if dry_run:
                log.info("DRY-RUN-ENABLED: new records not posted to AWS.")
            else:
                try:
                    aws.put_object(
                        data_set=self.data_set,
                        object_key=self.object_key,
                    )
                    log.info(
                        f"{self.table} sync for file index {self.file_index} complete: "
                        f"synced {count} records"
                    )
                    return
                except S3UploadFailedError as err:
                    log.error(err)
                    sys.exit(1)

        else:
            log.info(
                f"No new {self.table} for file index {self.file_index}: no sync necessary"
            )
