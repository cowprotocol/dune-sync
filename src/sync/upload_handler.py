"""Upload handler responsible for local file updates and aws uploads"""
import os
import sys

from s3transfer import S3UploadFailedError

from src.logger import set_log
from src.models.tables import SyncTable
from src.post.aws import AWSClient
from src.sync.record_handler import RecordHandler

log = set_log(__name__)


class UploadHandler:  # pylint: disable=too-few-public-methods
    """
    Given an instance of Record handler, ensures that
     - files are written locally,
     - uploaded to AWS and
     - sync block is recorded
    in the appropriate order.
    """

    def __init__(self, aws: AWSClient, record_handler: RecordHandler, table: SyncTable):
        self.aws = aws
        self.record_handler = record_handler
        self.table = str(table)

    def _aws_login_and_upload(self) -> bool:
        """Creates AWS client session and attempts to upload file"""
        path, filename = (
            self.record_handler.file_path,
            self.record_handler.content_filename,
        )
        try:
            return self.aws.upload_file(
                filename=os.path.join(path, filename),
                object_key=f"{self.table}/{filename}",
            )
        except S3UploadFailedError as err:
            log.error(err)
            sys.exit(1)

    def write_and_upload_content(self, dry_run: bool) -> None:
        """
        - Writes record handlers content to persistent volume,
        - attempts to upload to AWS and
        - records last sync block on volume.
        When dryrun flag is enabled, does not upload to IPFS.
        """
        record_handler = self.record_handler
        num_records, block_range, name = (
            record_handler.num_records(),
            record_handler.block_range,
            record_handler.name,
        )

        record_handler.write_found_content()
        if num_records > 0:
            log.info(
                f"attempting to post {num_records} new {name} records for block range {block_range}"
            )
            if dry_run:
                log.info(
                    "DRY-RUN-ENABLED: New records written to volume, but not posted to AWS."
                )
            else:
                self._aws_login_and_upload()
                log.info(
                    f"{name} sync for block range {block_range} complete: "
                    f"synced {num_records} records"
                )
        else:
            log.info(f"No new {name} for block range {block_range}: no sync necessary")

        record_handler.write_sync_data()
