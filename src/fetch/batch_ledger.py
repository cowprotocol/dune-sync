"""
Combining fetchers (Tenderly Simulation & Direct EVM Reads)
to construct batch-wise token transfer Ledger for Solver Slippage
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Optional

import requests

from src.environment import SETTLEMENT_CONTRACT_ADDRESS
from src.fetch.evm.events import TradeEvent, TransferEvent, TransferType
from src.models.tenderly import SimulationData
from src.post import tenderly


@dataclass
class InteractionData:
    """
    Relevant components of a SolverCompetition record
    fetched from CoW Protocol Orderbook API
    """

    # Block at which settlement was simulated.
    simulation_block: int
    # Solver who submitted the solution
    solver_address: str
    # Full Call Data provided by the solver
    call_data: str
    # Reduced Call Data,after internal interactions have been removed.
    # This should be equivalent to what actually appears on chain.
    uninternalized_call_data: str


@dataclass
class SettlementTransfer:
    """
    Enriched Transfer details intrinsic to the settlement contract
    with associated labels used to identify slippage.
    """

    token: str
    amount: int
    incoming: bool
    transfer_type: TransferType

    @classmethod
    def from_events(
        cls, trades: list[TradeEvent], transfers: list[TransferEvent]
    ) -> list[SettlementTransfer]:
        """
        Multi-Constructor, for building record sets based on Ethereum Event Data.
            - trades: Events used to distinguish between AMM or USER type.
            - transfers: Events (token, src, dst, wad) are transformed by examination of src, dst
        """
        traders = {t.owner for t in trades}
        return [
            cls(
                token=transfer.token,
                amount=transfer.wad,
                incoming=SETTLEMENT_CONTRACT_ADDRESS == transfer.dst,
                transfer_type=TransferType.from_referenced_transfer(
                    transfer, users=traders
                ),
            )
            for transfer in transfers
        ]


def get_competition_data(tx_hash: str) -> InteractionData:
    """
    Fetches solver_competition for `tx_hash` from orderbook via SQL query.
    Parses only the relevant fields from the results:
    Example: 0x26f01695c0983ea19915053f0eb62af633431d039beef94bd3f5b37ed9521627

    Orderbook API:
    https://api.cow.fi/mainnet/api/v1/solver_competition/by_tx_hash/{tx_hash}
    """
    print(f"Fetching competition data for transaction {tx_hash}")
    data = requests.get(
        url=f"https://api.cow.fi/mainnet/api/v1/solver_competition/by_tx_hash/{tx_hash}",
        timeout=2,
    ).json()
    # The Orderbook, stores all solution submissions sorted by the objective criteria.
    # The winning solution is the last entry of the `solutions` array.
    winning_solution = data["solutions"][-1]
    return InteractionData(
        simulation_block=data["competitionSimulationBlock"],
        uninternalized_call_data=winning_solution["uninternalizedCallData"],
        call_data=winning_solution["callData"],
        solver_address=winning_solution["solver"],
    )


def simulate_settlement(
    sender: str, call_data: str, block_number: Optional[int]
) -> SimulationData:
    """
    Given `call_data`, uses Tenderly API to simulate a settlement call on `block_number`.
    Returning sufficiently relevant parts of the simulation to build Settlement Transfers
    """
    simulation_dict = tenderly.simulate_transaction(
        contract_address=SETTLEMENT_CONTRACT_ADDRESS,
        sender=sender,
        call_data=call_data,
        block_number=block_number,
    )
    # Test simulation here: http://jsonblob.com/1063126730742710272
    # print("Simulation Success", json.dumps(simulation_dict))

    return SimulationData.from_dict(simulation_dict)
