from __future__ import annotations
from dataclasses import dataclass
from typing import Any


@dataclass
class SimulationLog:
    address: str
    topics: list[str]
    data: str

    @classmethod
    def from_dict(cls, log_data: dict[str, Any]) -> SimulationLog:
        return cls(
            address=log_data["address"],
            topics=log_data["topics"],
            data=log_data["data"],
        )


@dataclass
class SimulationData:
    tx_hash: str
    block_number: int
    logs: list[SimulationLog]

    @classmethod
    def from_dict(cls, simulation: dict[str, Any]) -> SimulationData:
        transaction = simulation["transaction"]
        return cls(
            logs=[
                SimulationLog.from_dict(log)
                for log in transaction["transaction_info"]["logs"]
            ],
            block_number=transaction["block_number"],
            tx_hash=transaction["hash"],
        )