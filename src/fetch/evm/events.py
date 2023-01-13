"""
Event data classes, and methods for fetching and parsing from EVM.
"""
from __future__ import annotations

import os
import warnings
from dataclasses import dataclass
from enum import Enum

from dotenv import load_dotenv
from eth_typing import HexStr
from web3 import Web3, HTTPProvider
from web3.types import TxReceipt, EventData

from src.abis.load import load_contract_abi
from src.environment import SETTLEMENT_CONTRACT_ADDRESS
from src.models.tenderly import SimulationData

load_dotenv()
SETTLEMENT_CONTRACT = Web3().eth.contract(
    address=SETTLEMENT_CONTRACT_ADDRESS, abi=load_contract_abi("gpv2_settlement")
)
NETWORK_STRING = os.environ.get("NETWORK", "mainnet")
NODE_URL = f"https://{NETWORK_STRING}.infura.io/v3/{os.environ['INFURA_KEY']}"
WEB3 = Web3(HTTPProvider(NODE_URL))
ERC20_CONTRACT = Web3().eth.contract(abi=load_contract_abi("erc20"))

# Transfer topics are uniquely identified by this "method signature topic".
# May need to more thoroughly investigate if there are any obscure tokens
# which do not satisfy this classification.
# According to Dune this holds for a time period.
# https://dune.com/queries/1886446
TRANSFER_TOPIC = "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef"


def address_from_topic(topic: str) -> str:
    """
    Utility method to strip empty bytes from an event
    topic which is known to be a (20-byte) EVM address.
    E.g. 0x0000000000000000000000009008d19f58aabd9ed0d60971565aa8510560ab41
    """
    assert topic[:-40] == "0x000000000000000000000000"
    return "0x" + topic[1][-40:]


def integer_from_hex_data(hex_data: str) -> int:
    """
    Utility method to strip empty bytes from hex string
    which is known to represent an integer.
    E.g.
    """
    print(hex_data)
    hex_data.replace("0x", "")
    return int(hex_data, 16)


def get_tx_receipt(tx_hash: HexStr) -> TxReceipt:
    """
    Fetches Transaction Receipt for `tx_hash` from EVM via eth_rpc call.
    """
    receipt = WEB3.eth.get_transaction_receipt(tx_hash)
    return receipt


class TransferType(Enum):
    """
    Enum variants represent all possible transfer types within a Batch Settlement
    """

    USER_IN = "USER_IN"
    USER_OUT = "USER_OUT"
    AMM_IN = "AMM_IN"
    AMM_OUT = "AMM_OUT"

    @classmethod
    def from_referenced_transfer(
        cls,
        transfer: TransferEvent,
        users: set[str],
        ref_account: str = SETTLEMENT_CONTRACT_ADDRESS,
    ) -> TransferType:
        """
        Compartmentalizes the logic of transfer type classification.
        For a given transfer and reference account
        """
        assert ref_account in {transfer.src, transfer.dst}
        if ref_account == transfer.src:
            # Outgoing from ref_account
            return cls.USER_OUT if transfer.dst in users else cls.AMM_OUT
        # Incoming to ref_account
        return cls.USER_IN if transfer.src in users else cls.AMM_IN


@dataclass
class EventMeta:
    """Common data among all Ethereum Events"""

    # Block at which the event was emitted (i.e. same at the transaction block number)
    block_number: int
    # index of event emission (within the transaction)
    index: int
    # Transaction has where the event was emitted.
    tx_hash: str

    @classmethod
    def from_event(cls, event: EventData) -> EventMeta:
        """
        Constructor of instance from a web3-py EventData instance
        Essentially trimming away the fat from a "bulky" web3 object.
        """
        return cls(
            block_number=event.get("blockNumber"),
            index=event.get("logIndex"),
            tx_hash=event.get("transactionHash").hex(),
        )


@dataclass
class TransferEvent:
    """
    Relevant Event Data emitted from ERC20 Token Transfers.
    """

    meta: EventMeta
    token: str
    src: str
    dst: str
    wad: int

    @classmethod
    def from_tx_receipt(cls, receipt: TxReceipt) -> list[TransferEvent]:
        """
        Multi-Constructor of instances from an Ethereum Transaction Receipt.
        Multiple transfers can occur within a single transaction and this constructor
        parses the receipt logs, returning all the transfers.
        """
        warnings.filterwarnings(action="ignore", category=UserWarning)
        transfer_events = ERC20_CONTRACT.events.Transfer().process_receipt(receipt)
        return [
            cls(
                token=event.address,
                wad=event.args.wad,
                src=event.args.src,
                dst=event.args.dst,
                meta=EventMeta.from_event(event),
            )
            for event in transfer_events
            # Only transfers to or from settlement contract are relevant for slippage.
            # if SETTLEMENT_CONTRACT in [event.args.dst.lower(), event.args.src.lower()]
        ]

    @classmethod
    def from_tenderly_simulation(
        cls, simulation: SimulationData
    ) -> list[TransferEvent]:
        """
        Multi-Constructor for instances of class parsed from a TenderlySimulation
        # TODO - should this go into the SimulationData class as (get_transfer_events)?
        """
        transfers = []
        for index, log in enumerate(simulation.logs):
            if log.topics[0] == TRANSFER_TOPIC:
                # found a transfer!
                transfers.append(
                    TransferEvent(
                        token=log.address,
                        src=address_from_topic(log.topics[1]),
                        dst=address_from_topic(log.topics[2]),
                        wad=integer_from_hex_data(log.data),
                        meta=EventMeta(
                            block_number=simulation.block_number,
                            tx_hash=simulation.tx_hash,
                            index=index,
                        ),
                    )
                )
        return transfers


@dataclass
class TradeEvent:  # pylint: disable=too-many-instance-attributes
    """
    Relevant Event Data emitted GPv2Settlement Trade events
    """

    buy_token: str
    sell_token: str
    buy_amount: int
    sell_amount: int
    fee_amount: int
    order_uid: str
    owner: str
    meta: EventMeta

    @classmethod
    def from_tx_receipt(cls, receipt: TxReceipt) -> list[TradeEvent]:
        """
        Multi-Constructor of instances from an Ethereum Transaction Receipt.
        Multiple Trades can occur within a single transaction.
        This constructor, parses the receipt logs, returning all the transfers.
        """
        warnings.filterwarnings(action="ignore", category=UserWarning)
        trade_events = SETTLEMENT_CONTRACT.events.Trade().process_receipt(receipt)
        return [
            cls(
                buy_token=event.args.buyToken,
                sell_token=event.args.sellToken,
                buy_amount=event.args.buyAmount,
                sell_amount=event.args.sellAmount,
                fee_amount=event.args.feeAmount,
                order_uid=event.args.orderUid.hex(),
                owner=event.args.owner,
                meta=EventMeta.from_event(event),
            )
            for event in trade_events
        ]
