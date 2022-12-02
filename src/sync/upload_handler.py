"""Upload handler responsible for local file updates and aws uploads"""
import os
import sys

from s3transfer import S3UploadFailedError

from src.logger import set_log
from src.post.aws import AWSClient
from src.sync.record_handler import RecordHandler

log = set_log(__name__)


class UploadHandler:
    """
    Given an instance of Record handler, ensures that
     - files are written locally,
     - uploaded to AWS and
     - sync block is recorded
    in the appropriate order.
    """

    def __init__(self, record_handler: RecordHandler):
        self.record_handler = record_handler

    def get_aws_client(self) -> AWSClient:
        """
        Returns AWS client according to the configuration of self.record_handler
        """
        aws_config = self.record_handler.config.aws
        return AWSClient(
            internal_role=aws_config.internal_role,
            external_role=aws_config.external_role,
            external_id=aws_config.external_id,
        )

    def _aws_login_and_upload(self) -> bool:
        """Creates AWS client session and attempts to upload file"""
        path, filename, table_name = (
            self.record_handler.file_path,
            self.record_handler.content_filename,
            self.record_handler.config.table_name,
        )
        try:
            return self.get_aws_client().upload_file(
                filename=os.path.join(path, filename),
                bucket=self.record_handler.config.aws.bucket,
                object_key=f"{table_name}/{filename}",
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
