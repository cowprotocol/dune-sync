"""
Abstraction for New Content Handling
provides a framework for writing new content to disk and posting to AWS
"""
from abc import ABC, abstractmethod

from src.logger import set_log
from src.models.block_range import BlockRange
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
    def num_records(self) -> int:
        """Returns number of records to handle"""

    @abstractmethod
    def write_found_content(self) -> None:
        """Writes content to disk"""

    @abstractmethod
    def write_sync_data(self) -> None:
        """Records last synced content file"""
