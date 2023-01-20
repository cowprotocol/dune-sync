"""
Combining fetchers (Tenderly Simulation & Direct EVM Reads)
to construct batch-wise token transfer Ledger for Solver Slippage
"""
from __future__ import annotations

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
