"""IPFS CID (de)serialization"""
from __future__ import annotations

import asyncio
from typing import Any, Optional

import aiohttp
import requests
from aiohttp import ClientSession
from multiformats_cid.cid import from_bytes

from src.logger import set_log
from src.models.app_data_content import FoundContent, NotFoundContent

log = set_log(__name__)


class Cid:
    """Holds logic for constructing and converting various representations of a Delegation ID"""

    def __init__(self, hex_str: str) -> None:
        """Builds Object (bytes as base representation) from hex string."""
        stripped_hex = hex_str.replace("0x", "")
        # Anatomy of a CID: https://proto.school/anatomy-of-a-cid/04
        prefix = bytearray([1, 112, 18, 32])
        self.bytes = bytes(prefix + bytes.fromhex(stripped_hex))

    @property
    def hex(self) -> str:
        """Returns hex representation"""
        without_prefix = self.bytes[4:]
        return "0x" + without_prefix.hex()

    def __str__(self) -> str:
        """Returns string representation"""
        return str(from_bytes(self.bytes))

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, Cid):
            return False
        return self.bytes == other.bytes

    def url(self) -> str:
        """IPFS URL where content can be recovered"""
        return f"https://ipfs.cow.fi/ipfs/{self}"

    def get_content(self, access_token: str, max_retries: int = 3) -> Optional[Any]:
        """
        Attempts to fetch content at cid with a timeout of 1 second.
        Trys `max_retries` times and otherwise returns None`
        """
        attempts = 0
        while attempts < max_retries:
            try:
                response = requests.get(
                    self.url(),
                    timeout=1,
                    headers={"x-pinata-gateway-token": access_token},
                )
                return response.json()
            except requests.exceptions.ReadTimeout:
                attempts += 1
            except requests.exceptions.JSONDecodeError as err:
                attempts += 1
                log.warning(f"unexpected error {err} retrying...")
        return None

    @classmethod
    async def fetch_many(  # pylint: disable=too-many-locals
        cls, missing_rows: list[dict[str, str]], access_token: str, max_retries: int = 3
    ) -> tuple[list[FoundContent], list[NotFoundContent]]:
        """Async AppData Fetching"""
        found, not_found = [], []
        async with aiohttp.ClientSession(
            headers={"x-pinata-gateway-token": access_token}
        ) as session:
            while missing_rows:
                row = missing_rows.pop()
                app_hash = row["app_hash"]

                previous_attempts = int(row.get("attempts", 0))
                cid = cls(app_hash)

                first_seen_block = int(row["first_seen_block"])
                result = await cid.fetch_content(
                    max_retries, previous_attempts, session, first_seen_block
                )
                if isinstance(result, FoundContent):
                    found.append(result)
                else:
                    assert isinstance(result, NotFoundContent)
                    not_found.append(result)

            return found, not_found

    async def fetch_content(
        self,
        max_retries: int,
        previous_attempts: int,
        session: ClientSession,
        first_seen_block: int,
    ) -> FoundContent | NotFoundContent:
        """Asynchronous content fetching"""
        attempts = 0
        while attempts < max_retries:
            try:
                async with session.get(self.url(), timeout=1) as response:
                    content = await response.json()
                    if previous_attempts:
                        log.debug(
                            f"Found previously missing content hash {self.hex} at CID {self}"
                        )
                    else:
                        log.debug(
                            f"Found content for {self.hex} at CID {self} ({attempts + 1} trys)"
                        )
                    return FoundContent(
                        app_hash=self.hex,
                        first_seen_block=first_seen_block,
                        content=content,
                    )
            except asyncio.TimeoutError:
                attempts += 1
            except aiohttp.ContentTypeError as err:
                log.warning(f"failed to parse response {response} with {err}")
                attempts += 1

        #  Content Not Found.
        total_attempts = previous_attempts + max_retries
        base_message = f"no content found for {self.hex} at CID {self} after"
        if previous_attempts:
            log.debug(f"still {base_message} {total_attempts} attempts")
        else:
            log.debug(f"{base_message} {max_retries} retries")

        return NotFoundContent(
            app_hash=self.hex,
            first_seen_block=first_seen_block,
            attempts=total_attempts,
        )
