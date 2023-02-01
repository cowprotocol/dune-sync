"""Full Computation of InternalImbalances"""
from dataclasses import asdict

from dune_client.file.interface import FileIO
from eth_typing import HexStr
from web3.types import TxReceipt

from src.environment import SETTLEMENT_CONTRACT_ADDRESS
from src.fetch.evm.events import TransferEvent, get_tx_receipt, TradeEvent
from src.fetch.solver_competition import get_competition_data
from src.models.settlement import SettlementTransfer, TokenImbalance
from src.post.tenderly import simulate_settlement


def internal_transfers(
    tx_hash: str, file_manager: FileIO, save_simulation: bool = False
) -> list[TokenImbalance]:
    """
    Transforms Ethereum Transaction hash into Internalized Imbalances (i.e. invisible transfers)
    """
    tx_receipt: TxReceipt = get_tx_receipt(HexStr(tx_hash))
    solver_address = tx_receipt.get("from")

    trades = TradeEvent.from_tx_receipt(tx_receipt)

    interaction_data = get_competition_data(tx_hash)

    full_simulation = simulate_settlement(
        block_number=interaction_data.simulation_block,
        call_data=interaction_data.uninternalized_call_data,
        sender=solver_address,
        save_simulation=save_simulation,
    )

    full_transfers = SettlementTransfer.from_events(
        trades, transfers=TransferEvent.from_tenderly_simulation(full_simulation)
    )

    # # May not need this AT ALL.
    # optimized_simulation = simulate_settlement(
    #     block_number=interaction_data.simulation_block,
    #     call_data=interaction_data.call_data,
    #     sender=solver_address,
    #     save_simulation=save_simulation,
    # )

    # reduced_transfers = SettlementTransfer.from_events(
    #     trades, transfers=TransferEvent.from_tenderly_simulation(optimized_simulation)
    # )
    # if not reduced_transfers:
    #     print("Simulation failed using actual transfers")
    #     reduced_transfers = SettlementTransfer.from_events(
    #         trades,
    #         transfers=[
    #             t
    #             for t in TransferEvent.from_tx_receipt(tx_receipt)
    #             if t.involves_address(SETTLEMENT_CONTRACT_ADDRESS)
    #         ],
    #     )
    reduced_transfers = SettlementTransfer.from_events(
        trades,
        transfers=[
            t
            for t in TransferEvent.from_tx_receipt(tx_receipt)
            if t.involves_address(SETTLEMENT_CONTRACT_ADDRESS)
        ],
    )

    if not full_transfers:
        raise BrokenPipeError("Can't run program without Full Simulation!")

    file_manager.write_csv(
        data=[asdict(x) for x in full_transfers], name=f"{tx_hash[:5]}-full.csv"
    )
    file_manager.write_csv(
        data=[asdict(x) for x in reduced_transfers], name=f"{tx_hash[:5]}-reduced.csv"
    )

    # The following code mutates values.
    cheeky_diff = list(full_transfers)
    for rec in reduced_transfers:
        rec.incoming = not rec.incoming
        cheeky_diff.append(rec)

    internalized_imbalance = TokenImbalance.from_settlement_transfers(cheeky_diff)
    file_manager.write_csv(
        [asdict(ti) for ti in internalized_imbalance if ti.amount != 0],
        name=f"{tx_hash[:5]}-agg_diff.csv",
    )

    return internalized_imbalance
