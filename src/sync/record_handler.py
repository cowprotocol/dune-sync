"""
Abstraction for New Content Handling
provides a framework for writing new content to disk and posting to AWS
"""
from abc import ABC, abstractmethod

from src.logger import set_log
from src.models.block_range import BlockRange

log = set_log(__name__)


class RecordHandler(ABC):  # pylint: disable=too-few-public-methods

    """
    This class is responsible for consuming new dune records and missing values from previous runs
    it attempts to fetch content for them and filters them into "found" and "not found" as necessary
    """

    def __init__(
        self,
        block_range: BlockRange,
        table: str,
    ):
        self.block_range = block_range
        self.name = table
        self.content_filename = f"cow_{block_range.block_to}.json"

    @abstractmethod
    def num_records(self) -> int:
        """Returns number of records to handle"""
