"""IPFS CID (de)serialization"""
from __future__ import annotations

from typing import Any, Optional

import requests
from multiformats_cid.cid import from_bytes  # type: ignore


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
