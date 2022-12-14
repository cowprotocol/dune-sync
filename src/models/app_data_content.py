"""Models for Found and Not Found App Data Content. Also, responsible for type conversion"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Any


@dataclass
class FoundContent:
    """Representation of AppData with Content"""

    app_hash: str
    first_seen_block: int
    content: dict[str, Any]

    @classmethod
    def from_dict(cls, row: dict[str, Any]) -> FoundContent:
        """Constructor from dictionary"""
        return cls(
            app_hash=row["app_hash"],
            first_seen_block=int(row["first_seen_block"]),
            content=row["content"],
        )

    def as_dune_record(self) -> dict[str, Any]:
        """Converts to DuneRecord type"""
        return {
            "app_hash": self.app_hash,
            "first_seen_block": self.first_seen_block,
            "content": self.content,
        }


@dataclass
class NotFoundContent:
    """
    Representation of AppData with unknown content.
    Records also number of attempts made to recover the content"""

    app_hash: str
    first_seen_block: int
    attempts: int

    @classmethod
    def from_dict(cls, row: dict[str, Any]) -> NotFoundContent:
        """Constructor from dictionary"""
        return cls(
            app_hash=row["app_hash"],
            first_seen_block=int(row["first_seen_block"]),
            attempts=int(row["attempts"]),
        )

    def as_dune_record(self) -> dict[str, Any]:
        """Converts to DuneRecord type"""
        return {
            "app_hash": self.app_hash,
            "first_seen_block": self.first_seen_block,
            "attempts": self.attempts,
        }
