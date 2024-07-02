"""Main Entry point for app_hash sync"""

from dune_client.file.interface import FileIO
from dune_client.types import DuneRecord

from src.fetch.dune import DuneFetcher
from src.fetch.ipfs import Cid
from src.logger import set_log
from src.models.app_data_content import FoundContent, NotFoundContent
from src.models.block_range import BlockRange
from src.models.tables import SyncTable
from src.post.aws import AWSClient
from src.sync.common import last_sync_block
from src.sync.config import SyncConfig, AppDataSyncConfig
from src.sync.record_handler import RecordHandler
from src.sync.upload_handler import UploadHandler

log = set_log(__name__)


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
        ipfs_access_key: str,
        missing_file_name: str,
        sync_table: SyncTable,
    ):
        super().__init__(block_range, sync_table, config)
        self.file_manager = file_manager
        self.ipfs_access_key = ipfs_access_key

        self._found: list[FoundContent] = []
        self._not_found: list[NotFoundContent] = []

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

    async def _handle_new_records(self, max_retries: int) -> None:
        # Drain the dune_results into "found" and "not found" categories
        self._found, self._not_found = await Cid.fetch_many(
            self.new_rows, self.ipfs_access_key, max_retries
        )

    async def _handle_missing_records(
        self, max_retries: int, give_up_threshold: int
    ) -> None:
        found, not_found = await Cid.fetch_many(
            self.missing_values, self.ipfs_access_key, max_retries
        )
        while found:
            self._found.append(found.pop())
        while not_found:
            row = not_found.pop()
            app_hash, attempts = row.app_hash, row.attempts
            if attempts > give_up_threshold:
                log.debug(
                    f"No content found after {attempts} attempts for {app_hash} assuming NULL."
                )
                self._found.append(
                    FoundContent(
                        app_hash=app_hash,
                        first_seen_block=row.first_seen_block,
                        content={},
                    )
                )
            else:
                self._not_found.append(row)

    def write_found_content(self) -> None:
        assert len(self.new_rows) == 0, "Must call _handle_new_records first!"
        self.file_manager.write_ndjson(
            data=[x.as_dune_record() for x in self._found], name=self.content_filename
        )
        # When not_found is empty, we want to overwrite the file (hence skip_empty=False)
        # This happens when number of attempts exceeds GIVE_UP_THRESHOLD
        self.file_manager.write_ndjson(
            data=[x.as_dune_record() for x in self._not_found],
            name=self.missing_file_name,
            skip_empty=False,
        )

    def write_sync_data(self) -> None:
        # Only write these if upload was successful.
        self.file_manager.write_csv(
            data=[{self.config.sync_column: str(self.block_range.block_to)}],
            name=self.config.sync_file,
        )

    async def fetch_content_and_filter(
        self, max_retries: int, give_up_threshold: int
    ) -> None:
        """
        Run loop fetching app_data for hashes,
        separates into (found and not found), returning the pair.
        """
        await self._handle_new_records(max_retries)
        log.info(
            f"Attempting to recover missing {len(self.missing_values)} records from previous run"
        )
        await self._handle_missing_records(max_retries, give_up_threshold)


async def sync_app_data(  # pylint: disable=too-many-arguments
    aws: AWSClient,
    dune: DuneFetcher,
    config: AppDataSyncConfig,
    ipfs_access_key: str,
    dry_run: bool,
    chain: str,
) -> None:
    """App Data Sync Logic"""

    sync_table = SyncTable.APP_DATA_GNOSIS
    genesis_bl = 15310317
    if chain == "mainnet":
        genesis_bl = 12153262
        sync_table = SyncTable.APP_DATA

    block_range = BlockRange(
        block_from=last_sync_block(
            aws,
            table=sync_table,
            genesis_block=genesis_bl,  # First App Hash Block
        ),
        block_to=await dune.latest_app_hash_block(chain),
    )

    data_handler = AppDataHandler(
        file_manager=FileIO(config.volume_path / str(sync_table)),
        new_rows=await dune.get_app_hashes(block_range, chain),
        block_range=block_range,
        config=config,
        ipfs_access_key=ipfs_access_key,
        missing_file_name=config.missing_files_name,
        sync_table=sync_table,
    )
    await data_handler.fetch_content_and_filter(
        max_retries=config.max_retries, give_up_threshold=config.give_up_threshold
    )
    UploadHandler(aws, data_handler, table=sync_table).write_and_upload_content(dry_run)
    log.info("app_data sync run completed successfully")
