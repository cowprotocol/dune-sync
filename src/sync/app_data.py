"""Main Entry point for app_hash sync"""
import json
import logging.config
import os.path
import sys

from boto3.exceptions import S3UploadFailedError
from dune_client.file.interface import FileIO
from dune_client.types import DuneRecord

from src.fetch.dune import DuneFetcher
from src.fetch.ipfs import Cid
from src.models.block_range import BlockRange
from src.post.aws import upload_file, get_s3_client
from src.sync.config import AppDataSyncConfig

log = logging.getLogger(__name__)
logging.basicConfig(format="%(asctime)s %(levelname)s %(name)s %(message)s")
log.setLevel(logging.DEBUG)


MAX_RETRIES = 3
GIVE_UP_THRESHOLD = 10


class RecordHandler:
    """
    This class is responsible for consuming new dune records and missing values from previous runs
    it attempts to fetch content for them and filters them into "found" and "not found" as necessary
    """

    def __init__(
        self,
        file_manager: FileIO,
        new_rows: list[DuneRecord],
        block_range: BlockRange,
        config: AppDataSyncConfig,
    ):
        self.file_manager = file_manager
        self.config = config
        self.block_range = block_range

        self._found: list[dict[str, str]] = []
        self._not_found: list[dict[str, str]] = []

        self.new_rows = new_rows
        try:
            self.missing_values = self.file_manager.load_ndjson(
                config.missing_files_name
            )
        except FileNotFoundError:
            self.missing_values = []

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

    def _handle_missing_records(self, max_retries: int) -> None:
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
            elif app_data is None and attempts > GIVE_UP_THRESHOLD:
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

    def _write_found_content(self, content_filename: str) -> None:
        assert len(self.new_rows) == 0, "Must call _handle_new_records first!"
        self.file_manager.write_ndjson(data=self._found, name=content_filename)

    def _write_sync_data(self) -> None:
        # Only write these if upload was successful.
        self.file_manager.write_csv(
            data=[{self.config.sync_column: str(self.block_range.block_to)}],
            name=self.config.sync_file,
        )
        # When not_found is empty, we want to overwrite the file (hence skip_empty=False)
        # This happens when number of attempts exceeds GIVE_UP_THRESHOLD
        self.file_manager.write_ndjson(
            self._not_found, self.config.missing_files_name, skip_empty=False
        )

    def fetch_content_and_filter(
        self, max_retries: int
    ) -> tuple[list[DuneRecord], list[DuneRecord]]:
        """
        Run loop fetching app_data for hashes,
        separates into (found and not found), returning the pair.
        """
        self._handle_new_records(max_retries)
        log.info(
            f"Attempting to recover missing {len(self.missing_values)} records from previous run"
        )
        self._handle_missing_records(max_retries)
        return self._found, self._not_found

    def write_and_upload_content(self) -> None:
        """
        - Writes self.found to persistent volume,
        - attempts to upload to AWS and
        - records last sync block on volume.
        """
        content_filename = f"cow_{self.block_range.block_to}.json"
        self._write_found_content(content_filename)

        if len(self._found) > 0:
            config = self.config
            try:
                upload_file(
                    client=get_s3_client(profile=config.aws_role),
                    filename=os.path.join(self.file_manager.path, content_filename),
                    bucket=config.aws_bucket,
                    object_key=f"{config.table_name}/{content_filename}",
                )
                log.info(
                    f"App Data Sync for block range {self.block_range} complete: "
                    f"synced {len(self._found)} records with {len(self._not_found)} missing"
                )
            except S3UploadFailedError as err:
                logging.error(err)
                sys.exit(1)
        else:
            log.info(
                f"No new App Data for block range {self.block_range}: no sync necessary"
            )

        self._write_sync_data()


async def get_block_range(
    file_manager: FileIO, dune: DuneFetcher, last_block_file: str, column: str
) -> BlockRange:
    """
    Constructs a block range object
    block_from is fetched from the last sync block (via file_manager)
    block_to is fetched from Dune via query results.
    """

    block_from = 12153262  # Genesis block
    try:
        block_from = int(file_manager.load_singleton(last_block_file, "csv")[column])
    except FileNotFoundError:
        log.warning(
            f"last sync file {last_block_file} not found, using genesis block {block_from}"
        )
    except KeyError as err:
        message = (
            f"last sync file {last_block_file} does not contain column header {column}, "
            f"exiting to avoid duplication"
        )
        log.error(message)
        raise RuntimeError(message) from err

    return BlockRange(
        # TODO - could be replaced by Dune Query on the app_data table (once available).
        #  https://github.com/cowprotocol/dune-bridge/issues/42
        block_from,
        block_to=await dune.latest_app_hash_block(),
    )


async def sync_app_data(dune: DuneFetcher, config: AppDataSyncConfig) -> None:
    """App Data Sync Logic"""
    # TODO - assert legit configuration before proceeding!
    table_name = config.table_name
    file_manager = FileIO(config.volume_path / table_name)
    block_range = await get_block_range(
        file_manager,
        dune,
        last_block_file=config.sync_file,
        column=config.sync_column,
    )

    data_handler = RecordHandler(
        file_manager,
        new_rows=await dune.get_app_hashes(block_range),
        block_range=block_range,
        config=config,
    )
    data_handler.fetch_content_and_filter(MAX_RETRIES)
    data_handler.write_and_upload_content()
    log.info("app_data sync run completed successfully")
