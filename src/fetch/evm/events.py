from __future__ import annotations

import os
from dataclasses import dataclass
from enum import Enum
from typing import Any

from dotenv import load_dotenv
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
TRANSFER_TOPIC = "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef"


def get_tx_receipt(tx_hash: str) -> TxReceipt:
    receipt = WEB3.eth.getTransactionReceipt(tx_hash)
    return receipt


class TransferType(Enum):
    USER_IN = "USER_IN"
    USER_OUT = "USER_OUT"
    AMM_IN = "AMM_IN"
    AMM_OUT = "AMM_OUT"


@dataclass
class EventMeta:
    block_number: int
    index: int
    tx_hash: str

    @classmethod
    def from_event(cls, event: EventData) -> EventMeta:
        return cls(
            block_number=event.get("blockNumber"),
            index=event.get("logIndex"),
            tx_hash=event.get("transactionHash").hex(),
        )


@dataclass
class TransferEvent:
    meta: EventMeta
    token: str
    src: str
    dst: str
    wad: int

    @classmethod
    def from_tx_receipt(cls, receipt: TxReceipt) -> list[TransferEvent]:
        transfer_events = ERC20_CONTRACT.events.Transfer().processReceipt(receipt)
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
    def from_tenderly_simulation(cls, simulation: SimulationData):
        transfers = []
        for index, log in enumerate(simulation.logs):
            if log.topics[0] == TRANSFER_TOPIC:
                # found a transfer!
                # 0x0000000000000000000000009008d19f58aabd9ed0d60971565aa8510560ab41
                transfers.append(
                    TransferEvent(
                        token=log.address,
                        src="0x" + log.topics[1][-40:],
                        dst="0x" + log.topics[2][-40:],
                        wad=int(log.data[2:], 16),
                        meta=EventMeta(
                            block_number=simulation.block_number,
                            tx_hash=simulation.tx_hash,
                            index=index,
                        ),
                    )
                )

    def transfer_type(self, user_list: set[str]) -> TransferType:
        assert SETTLEMENT_CONTRACT_ADDRESS in {self.src, self.dst}
        if SETTLEMENT_CONTRACT_ADDRESS == self.src:
            # Outgoing Transfer
            return (
                TransferType.USER_OUT if self.dst in user_list else TransferType.AMM_OUT
            )

        return TransferType.USER_IN if self.src in user_list else TransferType.AMM_IN


@dataclass
class TradeEvent:
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
        trade_events = SETTLEMENT_CONTRACT.events.Trade().processReceipt(receipt)
        print(trade_events[0].args)
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
            # Only transfers to or from settlement contract are relevant for slippage.
            # if SETTLEMENT_CONTRACT in [event.args.dst.lower(), event.args.src.lower()]
        ]
