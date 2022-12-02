"""
Abstraction for New Content Handling
provides a framework for writing new content to disk and posting to AWS
"""
from abc import ABC, abstractmethod

from src.logger import set_log
from src.models.block_range import BlockRange
from src.sync.common import aws_login_and_upload
from src.sync.config import SyncConfig

log = set_log(__name__)


class RecordHandler(ABC):  # pylint: disable=too-few-public-methods

    """
    This class is responsible for consuming new dune records and missing values from previous runs
    it attempts to fetch content for them and filters them into "found" and "not found" as necessary
    """

    def __init__(
        self,
        block_range: BlockRange,
        config: SyncConfig,
    ):
        self.config = config
        self.block_range = block_range

        self.name = config.table_name
        self.file_path = config.volume_path / self.name
        self.content_filename = f"cow_{block_range.block_to}.json"

    @abstractmethod
    def _num_records(self) -> int:
        """Returns number of records to handle"""

    @abstractmethod
    def _write_found_content(self) -> None:
        """Writes content to disk"""

    @abstractmethod
    def _write_sync_data(self) -> None:
        """Records last synced content file"""

    def write_and_upload_content(self, dry_run: bool) -> None:
        """
        - Writes self.order_rewards to persistent volume,
        - attempts to upload to AWS and
        - records last sync block on volume.
        When dryrun flag is enabled, does not upload to IPFS.
        """
        self._write_found_content()
        if dry_run:
            log.info(
                "DRY-RUN: New records written to volume, but not posted to AWS. "
                "Not updating last sync block."
            )
            return

        if self._num_records() > 0:
            aws_login_and_upload(
                config=self.config,
                path=self.file_path,
                filename=self.content_filename,
            )
            log.info(
                f"{self.name} sync for block range {self.block_range} complete: "
                f"synced {self._num_records()} records"
            )
        else:
            log.info(
                f"No new {self.name} for block range {self.block_range}: no sync necessary"
            )

        self._write_sync_data()
