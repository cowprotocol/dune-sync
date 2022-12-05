"""Main Entry point for app_hash sync"""
import json

from dune_client.file.interface import FileIO
from dune_client.types import DuneRecord

from src.fetch.dune import DuneFetcher
from src.fetch.ipfs import Cid
from src.logger import set_log
from src.models.block_range import BlockRange
from src.models.tables import SyncTable
from src.post.aws import AWSClient
from src.sync.common import last_sync_block
from src.sync.config import SyncConfig, AppDataSyncConfig
from src.sync.record_handler import RecordHandler
from src.sync.upload_handler import UploadHandler

log = set_log(__name__)


SYNC_TABLE = SyncTable.APP_DATA


class AppDataHandler(RecordHandler):  # pylint:disable=too-many-instance-attributes
    """
    This class is responsible for consuming new dune records and missing values from previous runs
    it attempts to fetch content for them and filters them into "found" and "not found" as necessary
    """

    def __init__(  # pylint:disable=too-many-arguments
        self,
        file_manager: FileIO,
        new_rows: list[DuneRecord],
        block_range: BlockRange,
        config: SyncConfig,
        missing_file_name: str,
    ):
        super().__init__(block_range, SYNC_TABLE, config)
        self.file_manager = file_manager

        self._found: list[dict[str, str]] = []
        self._not_found: list[dict[str, str]] = []

        self.new_rows = new_rows
        self.missing_file_name = missing_file_name
        try:
            self.missing_values = self.file_manager.load_ndjson(missing_file_name)
        except FileNotFoundError:
            self.missing_values = []

    def num_records(self) -> int:
        assert len(self.new_rows) == 0, (
            "this function call is not allowed until self.new_rows have been processed! "
            "call fetch_content_and_filter first"
        )
        return len(self._found)

    def _handle_new_records(self, max_retries: int) -> None:
        # Drain the dune_results into "found" and "not found" categories
        while self.new_rows:
            row = self.new_rows.pop()
            app_hash = row["app_hash"]
            cid = Cid(app_hash)
            app_data = cid.get_content(max_retries)

            # Here it would be nice if python we more like rust!
            if app_data is not None:
                # Row is modified and added found items
                log.debug(f"Found content for {app_hash} at CID {cid}")
                row["content"] = app_data
                self._found.append(row)
            else:
                # Unmodified row added to not_found items
                log.debug(
                    f"No content found for {app_hash} at CID {cid} after {max_retries} retries"
                )
                # Dune Records are string dicts.... :(
                row["attempts"] = str(max_retries)
                self._not_found.append(row)

    def _handle_missing_records(self, max_retries: int, give_up_threshold: int) -> None:
        while self.missing_values:
            row = self.missing_values.pop()
            app_hash = row["app_hash"]
            cid = Cid(app_hash)
            app_data = cid.get_content(max_retries)
            attempts = int(row["attempts"]) + max_retries

            if app_data is not None:
                log.debug(
                    f"Found previously missing content hash {row['app_hash']} at CID {cid}"
                )
                self._found.append(
                    {
                        "app_hash": app_hash,
                        "first_seen_block": row["first_seen_block"],
                        "content": app_data,
                    }
                )
            elif app_data is None and attempts > give_up_threshold:
                log.debug(
                    f"No content found after {attempts} attempts for {app_hash} assuming NULL."
                )
                self._found.append(
                    {
                        "app_hash": app_hash,
                        "first_seen_block": row["first_seen_block"],
                        "content": json.dumps({}),
                    }
                )
            else:
                log.debug(
                    f"Still no content found for {app_hash} at CID {cid} after {attempts} attempts"
                )
                row.update({"attempts": str(attempts)})
                self._not_found.append(row)

    def write_found_content(self) -> None:
        assert len(self.new_rows) == 0, "Must call _handle_new_records first!"
        self.file_manager.write_ndjson(data=self._found, name=self.content_filename)

    def write_sync_data(self) -> None:
        # Only write these if upload was successful.
        self.file_manager.write_csv(
            data=[{self.config.sync_column: str(self.block_range.block_to)}],
            name=self.config.sync_file,
        )
        # When not_found is empty, we want to overwrite the file (hence skip_empty=False)
        # This happens when number of attempts exceeds GIVE_UP_THRESHOLD
        self.file_manager.write_ndjson(
            self._not_found, self.missing_file_name, skip_empty=False
        )

    def fetch_content_and_filter(
        self, max_retries: int, give_up_threshold: int
    ) -> tuple[list[DuneRecord], list[DuneRecord]]:
        """
        Run loop fetching app_data for hashes,
        separates into (found and not found), returning the pair.
        """
        self._handle_new_records(max_retries)
        log.info(
            f"Attempting to recover missing {len(self.missing_values)} records from previous run"
        )
        self._handle_missing_records(max_retries, give_up_threshold)
        return self._found, self._not_found


async def sync_app_data(
    aws: AWSClient,
    dune: DuneFetcher,
    config: AppDataSyncConfig,
    dry_run: bool,
) -> None:
    """App Data Sync Logic"""
    block_range = BlockRange(
        block_from=last_sync_block(
            aws,
            table=SYNC_TABLE,
            genesis_block=12153262,  # First App Hash Block
        ),
        block_to=await dune.latest_app_hash_block(),
    )

    data_handler = AppDataHandler(
        file_manager=FileIO(config.volume_path / str(SYNC_TABLE)),
        new_rows=await dune.get_app_hashes(block_range),
        block_range=block_range,
        config=config,
        missing_file_name=config.missing_files_name,
    )
    data_handler.fetch_content_and_filter(
        max_retries=config.max_retries, give_up_threshold=config.give_up_threshold
    )
    UploadHandler(aws, data_handler, table=SYNC_TABLE).write_and_upload_content(dry_run)
    log.info("app_data sync run completed successfully")
