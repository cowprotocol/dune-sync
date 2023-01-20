"""
Elementary components and interfaces for performing Tenderly Transaction Simulations
"""
import os
from typing import Optional, Any

import requests
from dotenv import load_dotenv

from src.environment import SETTLEMENT_CONTRACT_ADDRESS
from src.models.tenderly import SimulationData

load_dotenv()
TENDERLY_USER = os.environ["TENDERLY_USER"]
TENDERLY_PROJECT = os.environ["TENDERLY_PROJECT"]
BASE_URL = "https://api.tenderly.co/api"
SIMULATE_URL = (
    f"{BASE_URL}/v1/account/{TENDERLY_USER}/project/{TENDERLY_PROJECT}/simulate"
)


def simulate_transaction(
    contract_address: str, sender: str, call_data: str, block_number: Optional[int]
) -> Any:
    """
    Makes a Tenderly Transaction Simulation request
    via their API on `block_number` with transaction data:
        {
            "to": contract_address,
            "from": sender,
            "data": call_data
            "value": 0,
        }
    Returns "untyped" simulation results.
    """
    response = requests.post(
        url=SIMULATE_URL,
        json={
            # // standard TX fields
            "network_id": "1",
            "from": sender,
            "to": contract_address,
            "input": call_data,
            "block_number": block_number,
            "gas": 5000000,
            "gas_price": "0",
            "value": 0,
            # // simulation config (tenderly specific)
            "save_if_fails": False,
            "save": False,
            "simulation_type": "quick",
        },
        headers={
            "X-Access-Key": os.environ["TENDERLY_ACCESS_KEY"],
        },
        timeout=3,
    )
    return response.json()


def simulate_settlement(
    sender: str, call_data: str, block_number: Optional[int]
) -> SimulationData:
    """
    Given `call_data`, uses Tenderly API to simulate a settlement call on `block_number`.
    Returning sufficiently relevant parts of the simulation to build Settlement Transfers
    """
    simulation_dict = simulate_transaction(
        contract_address=SETTLEMENT_CONTRACT_ADDRESS,
        sender=sender,
        call_data=call_data,
        block_number=block_number,
    )
    # Test simulation here: http://jsonblob.com/1063126730742710272
    # print("Simulation Success", json.dumps(simulation_dict))

    return SimulationData.from_dict(simulation_dict)
