"""IPFS CID (de)serialization"""
from __future__ import annotations

import asyncio
from typing import Any, Optional

import aiohttp
import requests
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
        return f"https://gnosis.mypinata.cloud/ipfs/{self}"

    def get_content(self, max_retries: int = 3) -> Optional[Any]:
        """
        Attempts to fetch content at cid with a timeout of 1 second.
        Trys `max_retries` times and otherwise returns None`
        """
        attempts = 0
        while attempts < max_retries:
            try:
                response = requests.get(self.url(), timeout=1)
                return response.json()
            except requests.exceptions.ReadTimeout:
                attempts += 1
        return None

    @classmethod
    async def fetch_many(
        cls, missing_rows: list[dict[str, str]], max_retries: int = 3
    ) -> tuple[list[FoundContent], list[NotFoundContent]]:
        """Async AppData Fetching"""
        found, not_found = [], []
        async with aiohttp.ClientSession() as session:
            while missing_rows:
                row = missing_rows.pop()
                app_hash = row["app_hash"]
                cid = cls(app_hash)
                attempts = 0
                previous_attempts = int(row.get("attempts", 0))
                content = None
                while attempts < max_retries:
                    try:
                        async with session.get(cid.url(), timeout=1) as response:
                            content = await response.json()
                            if previous_attempts:
                                log.debug(
                                    f"Found previously missing content hash {app_hash} at CID {cid}"
                                )
                            else:
                                log.debug(
                                    f"Found content for {app_hash} at CID {cid} ({attempts + 1} trys)"
                                )
                            found.append(
                                FoundContent(
                                    app_hash=app_hash,
                                    first_seen_block=int(row["first_seen_block"]),
                                    content=content,
                                )
                            )
                            break
                    except asyncio.TimeoutError:
                        attempts += 1

                if not content:
                    total_attempts = previous_attempts + max_retries
                    base_message = f"no content found for {app_hash} at CID {cid} after"
                    if previous_attempts:
                        log.debug(f"still {base_message} {total_attempts} attempts")
                    else:
                        log.debug(f"{base_message} {max_retries} retries")

                    not_found.append(
                        NotFoundContent(
                            app_hash=app_hash,
                            first_seen_block=int(row["first_seen_block"]),
                            attempts=total_attempts,
                        )
                    )
            return found, not_found
