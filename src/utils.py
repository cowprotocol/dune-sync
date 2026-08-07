"""Basic reusable utility functions"""
import os

from src.environment import QUERY_PATH


def open_query(filename: str) -> str:
    """Opens `filename` and returns as string"""
    with open(query_file(filename), "r", encoding="utf-8") as file:
        return file.read()


def query_file(filename: str) -> str:
    """Returns proper path for filename in QUERY_PATH"""
    return os.path.join(QUERY_PATH, filename)


def node_suffix(network: str) -> str:
    """
    Converts network internal name to name used for nodes and dune tables
    """
    if network == "mainnet":
        return "ETHEREUM"
    if network == "xdai":
        return "GNOSIS"
    if network == "arbitrum-one":
        return "ARBITRUM"
    return ""
