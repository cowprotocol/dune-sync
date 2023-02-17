"""
Combining fetchers (Tenderly Simulation & Direct EVM Reads)
to construct batch-wise token transfer Ledger for Solver Slippage
"""
from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass

from src.environment import SETTLEMENT_CONTRACT_ADDRESS
from src.fetch.evm.events import TradeEvent, TransferEvent, TransferType


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
        traders = {t.owner.lower() for t in trades}
        ref_address = str(SETTLEMENT_CONTRACT_ADDRESS).lower()
        relevant_transfers = []
        for transfer in transfers:
            if transfer.involves_address(ref_address):
                relevant_transfers.append(
                    cls(
                        token=transfer.token,
                        amount=transfer.wad,
                        incoming=ref_address == transfer.dst.lower(),
                        transfer_type=TransferType.from_referenced_transfer(
                            transfer, users=traders, ref_account=ref_address
                        ),
                    )
                )
            else:
                print(f"Found irrelevant transfer {transfer}")
        return relevant_transfers


@dataclass
class TokenImbalance:
    """
    Enriched Transfer details intrinsic to the settlement contract
    with associated labels used to identify slippage.
    """

    token: str
    amount: int

    @classmethod
    def from_settlement_transfers(
        cls, transfers: list[SettlementTransfer]
    ) -> list[TokenImbalance]:
        """Aggregates list into token-wise sum."""
        imbalance_dict: dict[str, int] = defaultdict(int)
        for transfer in transfers:
            amount = transfer.amount if transfer.incoming else -transfer.amount
            imbalance_dict[transfer.token] += amount
        return [
            cls(token=key, amount=value)
            for key, value in imbalance_dict.items()
            if value != 0
        ]
