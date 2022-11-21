"""Main Entry point for app_hash sync"""
import json
import logging.config
import os.path

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


class RecordHandler:  # pylint:disable=too-few-public-methods
    """
    This class is responsible for consuming new dune records and missing values from previous runs
    it attempts to fetch content for them and filters them into "found" and "not found" as necessary
    """

    def __init__(
        self,
        new_rows: list[DuneRecord],
        missing_values: list[DuneRecord],
        block_range: BlockRange,
        config: AppDataSyncConfig,
    ):
        self.config = config
        self.block_range = block_range

        self.found: list[dict[str, str]] = []
        self.not_found: list[dict[str, str]] = []

        self.new_rows = new_rows
        self.missing_values = missing_values

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
                self.found.append(row)
            else:
                # Unmodified row added to not_found items
                log.debug(
                    f"No content found for {app_hash} at CID {cid} after {max_retries} retries"
                )
                # Dune Records are string dicts.... :(
                row["attempts"] = str(max_retries)
                self.not_found.append(row)

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
                self.found.append(
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
                self.found.append(
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
                self.not_found.append(row)

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
        return self.found, self.not_found


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
            f"block range file {last_block_file} not found, using genesis block {block_from}"
        )
    except KeyError as err:
        message = (
            f"block range file {last_block_file} does not contain column header {column}, "
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


def get_missing_data(file_manager: FileIO, missing_fname: str) -> list[DuneRecord]:
    """
    Loads missing records from file (aka previous run) if there are any.
    Otherwise, assumes there are none.
    """
    try:
        return file_manager.load_ndjson(missing_fname)
    except FileNotFoundError:
        return []


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
        new_rows=await dune.get_app_hashes(block_range),
        missing_values=get_missing_data(
            file_manager, missing_fname=config.missing_files_name
        ),
        block_range=block_range,
        config=config,
    )
    found, not_found = data_handler.fetch_content_and_filter(MAX_RETRIES)

    content_filename = f"cow_{block_range.block_to}.json"
    file_manager.write_ndjson(data=found, name=content_filename)

    if len(found) > 0:
        success = upload_file(
            s3_client=get_s3_client(profile=config.aws_role),
            file_name=os.path.join(file_manager.path, content_filename),
            bucket=config.aws_bucket,
            object_key=f"{table_name}/{content_filename}",
        )
        if success:
            # Only write these if upload was successful.
            file_manager.write_csv(
                data=[{config.sync_column: str(block_range.block_to)}],
                name=config.sync_file,
            )
            # When not_found is empty, we want to overwrite the file (hence skip_empty=False)
            # This happens when number of attempts exceeds GIVE_UP_THRESHOLD
            file_manager.write_ndjson(
                not_found, config.missing_files_name, skip_empty=False
            )
            log.info(
                f"App Data Sync for block range {BlockRange} complete: "
                f"synced {len(found)} records with {len(not_found)} missing"
            )
    else:
        log.info(f"No new App Data for block range {BlockRange}: no sync necessary")
