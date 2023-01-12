import os
from typing import Optional, Any

import requests
from dotenv import load_dotenv

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
    )
    return response.json()
