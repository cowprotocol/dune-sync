"""
Elementary structures parsing results of Tenderly Transaction Simulations
"""
from __future__ import annotations
from dataclasses import dataclass
from typing import Any


@dataclass
class SimulationLog:
    """
    Represents the meaningful components of a tenderly transaction-simulation log.
    """

    # The contract address emitting the event
    address: str
    # The indexed topics from the event log
    topics: list[str]
    # The additional (non-indexed event data)
    data: str

    @classmethod
    def from_dict(cls, log_data: dict[str, Any]) -> SimulationLog:
        """Constructor of class instance from dict/json (i.e. from a simulation API response)"""
        return cls(
            address=log_data["address"],
            topics=log_data["topics"],
            data=log_data["data"],
        )


@dataclass
class SimulationData:
    """
    Represents the relevant (to us) data returned from a tenderly transaction-simulation.
    """

    # Transaction hash that would have been assigned if this were an actually mined tx.
    # TODO - this is not necessary at all for our purposes.
    tx_hash: str
    # Block on which the simulation was made.
    block_number: int
    # Event Logs emitted within the transaction's simulation.
    logs: list[SimulationLog]

    @classmethod
    def from_dict(cls, simulation: dict[str, Any]) -> SimulationData:
        """Constructor of class instance from dict/json (i.e. from a simulation API response)"""
        transaction = simulation["transaction"]
        return cls(
            logs=[
                SimulationLog.from_dict(log["raw"])
                for log in transaction["transaction_info"]["logs"] or []
            ],
            block_number=transaction["block_number"],
            tx_hash=transaction["hash"],
        )
