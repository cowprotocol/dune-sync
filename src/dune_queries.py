"""
Localized account of all Queries related to this project's main functionality
"""
from __future__ import annotations

from copy import copy
from dataclasses import dataclass

from dune_client.query import Query
from dune_client.types import QueryParameter


@dataclass
class QueryData:
    """Stores name and a version of the query for each query."""

    name: str
    query: Query

    def __init__(self, name: str, query_id: int, filename: str) -> None:
        self.name = name
        self.filepath = filename
        self.query = Query(query_id, name)

    def with_params(self, params: list[QueryParameter]) -> Query:
        """
        Copies the query and adds parameters to it, returning the copy.
        """
        # We currently default to the V1 Queries, soon to switch them out.
        query_copy = copy(self.query)
        query_copy.params = params
        return query_copy


QUERIES = {
    "APP_HASHES": QueryData(
        query_id=1610025, name="Unique App Hashes", filename="app_hashes.sql"
    ),
    "LATEST_APP_HASH_BLOCK": QueryData(
        query_id=1615490,
        name="Latest Possible App Hash Block",
        filename="app_hash_latest_block.sql",
    ),
}
