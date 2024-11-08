"""Dataclasses for the prefect deployments"""
import os
from enum import Enum
from dataclasses import dataclass, field

from dotenv import load_dotenv

load_dotenv()


class ENV(Enum):
    """
    Enum ENV class to change environment variables for DEV And PROD
    """

    DEV = "DEV"
    PR = "PR"
    PROD = "PROD"

    def is_dev(self) -> bool:
        """Check if the environment is DEV."""
        return self == ENV.DEV

    def is_pr(self) -> bool:
        """Check if the environment is PR."""
        return self == ENV.PR

    def is_prod(self) -> bool:
        """Check if the environment is PROD."""
        return self == ENV.PROD


class CHAIN(Enum):
    """
    Enum CHAIN class to change environment variables different chains
    """

    ARBITRUM = "ARBITRUM"
    GNOSIS = "GNOSIS"
    MAINNET = "MAINNET"

    def is_arbitrum(self) -> bool:
        """Check if the chain is Arbitrum"""
        return self == CHAIN.ARBITRUM

    def is_gnosis(self) -> bool:
        """Check if the chain is Gnosis"""
        return self == CHAIN.GNOSIS

    def is_mainnet(self) -> bool:
        """Check if the chain is mainnet"""
        return self == CHAIN.MAINNET


@dataclass(frozen=True)
class Config:
    """
    Config dataclass to setup config based on chain&env combination.
    """

    _chain: CHAIN
    _env: ENV

    _dune_query_id: str = field(init=False)
    _etherscan_api_key: str = field(init=False)

    def __post_init__(self) -> None:
        etherscan_api_value = os.environ["ETHERSCAN_API_KEY"]

        if self._env.is_dev():
            dune_query_id_value = os.environ["AGGREGATE_QUERY_DEV_ID"]
        elif self._env.is_pr():
            dune_query_id_value = os.environ["AGGREGATE_QUERY_PR_ID"]
        elif self._env.is_prod():
            dune_query_id_value = os.environ["AGGREGATE_QUERY_ID"]
        else:
            raise ValueError("ENV is neither DEV, PR, nor PROD")

        object.__setattr__(self, "_etherscan_api_key", etherscan_api_value)
        object.__setattr__(self, "_dune_query_id", dune_query_id_value)

    @property
    def etherscan_api_key(self) -> str:
        """Etherscan API Key Getter"""
        return self._etherscan_api_key

    @property
    def dune_query_id(self) -> str:
        """Dune Aggregate Query Getter"""
        return self._dune_query_id

    @property
    def env(self) -> ENV:
        """ENV Getter"""
        return self._env

    @property
    def chain(self) -> CHAIN:
        """Chain Getter"""
        return self._chain
