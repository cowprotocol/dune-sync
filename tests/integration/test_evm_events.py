import unittest

import web3.exceptions
from eth_typing import HexStr
from hexbytes import HexBytes
from web3 import Web3
from web3.types import TxReceipt, EventData

from src.fetch.evm.events import get_tx_receipt, TransferEvent, TradeEvent, EventMeta
from src.models.tenderly import SimulationData, SimulationLog


class TestEvmEventFetching(unittest.TestCase):
    def setUp(self) -> None:
        self.evm_address = Web3.to_checksum_address("0x" + "1" * 40)
        self.tx_hash = HexStr(
            "0x26f01695c0983ea19915053f0eb62af633431d039beef94bd3f5b37ed9521627"
        )

    def test_get_tx_receipt_success(self):
        receipt: TxReceipt = get_tx_receipt(self.tx_hash)
        self.assertEqual(
            receipt["transactionHash"],
            HexBytes(self.tx_hash),
        )

    def test_get_tx_receipt_fails(self):
        with self.assertRaises(web3.exceptions.TransactionNotFound):
            get_tx_receipt(HexStr("0x1"))

    def test_event_meta_from_event(self):
        block_number = 1
        index = 1
        tx_hash = HexBytes("0x1")
        event = EventData(
            blockNumber=block_number,
            logIndex=index,
            blockHash=HexBytes("0x2"),
            transactionHash=tx_hash,
            address=self.evm_address,
            args={},
            event="",
            transactionIndex=0,
        )
        event_meta = EventMeta.from_event(event)
        self.assertEqual(event_meta, EventMeta(block_number, index, tx_hash.hex()))

    def test_transfer_events_from_receipt(self):
        transfers = TransferEvent.from_tx_receipt(get_tx_receipt(self.tx_hash))
        self.assertEqual(
            transfers,
            [
                TransferEvent(
                    meta=EventMeta(
                        block_number=16300368,
                        index=251,
                        tx_hash="0x26f01695c0983ea19915053f0eb62af633431d039beef94bd3f5b37ed9521627",
                    ),
                    token="0x6B175474E89094C44Da98b954EedeAC495271d0F",
                    src="0xE2b424053b9ebFCEdF89ECB8Bf2972974E98700C",
                    dst="0x9008D19f58AAbD9eD0D60971565AA8510560ab41",
                    wad=2000000000000000000000,
                ),
                TransferEvent(
                    meta=EventMeta(
                        block_number=16300368,
                        index=252,
                        tx_hash="0x26f01695c0983ea19915053f0eb62af633431d039beef94bd3f5b37ed9521627",
                    ),
                    token="0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2",
                    src="0x9008D19f58AAbD9eD0D60971565AA8510560ab41",
                    dst="0x61eB53ee427aB4E007d78A9134AaCb3101A2DC23",
                    wad=1614063949739215622,
                ),
                TransferEvent(
                    meta=EventMeta(
                        block_number=16300368,
                        index=255,
                        tx_hash="0x26f01695c0983ea19915053f0eb62af633431d039beef94bd3f5b37ed9521627",
                    ),
                    token="0x3432B6A60D23Ca0dFCa7761B7ab56459D9C964D0",
                    src="0x61eB53ee427aB4E007d78A9134AaCb3101A2DC23",
                    dst="0x9008D19f58AAbD9eD0D60971565AA8510560ab41",
                    wad=456148202922231918871,
                ),
                TransferEvent(
                    meta=EventMeta(
                        block_number=16300368,
                        index=261,
                        tx_hash="0x26f01695c0983ea19915053f0eb62af633431d039beef94bd3f5b37ed9521627",
                    ),
                    token="0x3432B6A60D23Ca0dFCa7761B7ab56459D9C964D0",
                    src="0x9008D19f58AAbD9eD0D60971565AA8510560ab41",
                    dst="0xE2b424053b9ebFCEdF89ECB8Bf2972974E98700C",
                    wad=469613864284355328238,
                ),
            ],
        )

    def test_trade_events_from_receipt(self):
        trades = TradeEvent.from_tx_receipt(get_tx_receipt(self.tx_hash))
        self.assertEqual(
            trades,
            [
                TradeEvent(
                    buy_token="0x3432B6A60D23Ca0dFCa7761B7ab56459D9C964D0",
                    sell_token="0x6B175474E89094C44Da98b954EedeAC495271d0F",
                    buy_amount=469613864284355328238,
                    sell_amount=2000000000000000000000,
                    fee_amount=6939976408813727744,
                    order_uid="bf293f652b46fe85a15838d7ff736add1b6098ed1c143f3902869d325f9e0069e2b424053b9ebfcedf89ecb8bf2972974e98700c63af639e",
                    owner="0xE2b424053b9ebFCEdF89ECB8Bf2972974E98700C",
                    meta=EventMeta(
                        block_number=16300368,
                        index=250,
                        tx_hash="0x26f01695c0983ea19915053f0eb62af633431d039beef94bd3f5b37ed9521627",
                    ),
                )
            ],
        )

    def test_from_tenderly_simulation(self):
        sim_data = SimulationData(
            tx_hash="0x29518cacd96089f992cec0b803a1afba6857ef61098d9609d9b70174f80c3fba",
            block_number=16300366,
            logs=[
                SimulationLog(
                    address="0x9008d19f58aabd9ed0d60971565aa8510560ab41",
                    topics=[
                        "0xa07a543ab8a018198e99ca0184c93fe9050a79400a0a723441f84de1d972cc17",
                        "0x000000000000000000000000e2b424053b9ebfcedf89ecb8bf2972974e98700c",
                    ],
                    data="0x",
                ),
                SimulationLog(
                    address="0x6b175474e89094c44da98b954eedeac495271d0f",
                    topics=[
                        "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef",
                        "0x000000000000000000000000e2b424053b9ebfcedf89ecb8bf2972974e98700c",
                        "0x0000000000000000000000009008d19f58aabd9ed0d60971565aa8510560ab41",
                    ],
                    data="0x00000000000000000000000000000000000000000000006c6b935b8bbd400000",
                ),
            ],
        )
        transfers = TransferEvent.from_tenderly_simulation(sim_data)
        self.assertEqual(
            transfers,
            [
                TransferEvent(
                    meta=EventMeta(
                        block_number=16300366,
                        index=1,
                        tx_hash="0x29518cacd96089f992cec0b803a1afba6857ef61098d9609d9b70174f80c3fba",
                    ),
                    token="0x6b175474e89094c44da98b954eedeac495271d0f",
                    src="0xe2b424053b9ebfcedf89ecb8bf2972974e98700c",
                    dst="0x9008d19f58aabd9ed0d60971565aa8510560ab41",
                    wad=2000000000000000000000,
                )
            ],
        )
