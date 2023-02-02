"""
Combining fetchers (Tenderly Simulation & Direct EVM Reads)
to construct batch-wise token transfer Ledger for Solver Slippage
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Optional

import requests


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
    # Reduced Call Data,after internal interactions have been removed.
    # This should be equivalent to what actually appears on chain.
    call_data: str
    # Full Call Data provided by the solver
    uninternalized_call_data: Optional[str]


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
        uninternalized_call_data=winning_solution.get("uninternalizedCallData", None),
        call_data=winning_solution["callData"],
        solver_address=winning_solution["solver"],
    )
