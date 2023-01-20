import unittest

from src.fetch.evm.events import (
    TransferEvent,
    TradeEvent,
    TransferType,
    EventMeta,
)
from src.models.settlement import SettlementTransfer


class TestSettlementTransfer(unittest.TestCase):
    def setUp(self) -> None:
        self.tx_hash = (
            "0x26f01695c0983ea19915053f0eb62af633431d039beef94bd3f5b37ed9521627"
        )

    def test_get_settlement_transfers(self):

        settlement_transfers = SettlementTransfer.from_events(
            trades=[
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
            transfers=[
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
        self.assertEqual(
            settlement_transfers,
            [
                SettlementTransfer(
                    token="0x6B175474E89094C44Da98b954EedeAC495271d0F",
                    amount=2000000000000000000000,
                    incoming=True,
                    transfer_type=TransferType.USER_IN,
                ),
                SettlementTransfer(
                    token="0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2",
                    amount=1614063949739215622,
                    incoming=False,
                    transfer_type=TransferType.AMM_OUT,
                ),
                SettlementTransfer(
                    token="0x3432B6A60D23Ca0dFCa7761B7ab56459D9C964D0",
                    amount=456148202922231918871,
                    incoming=True,
                    transfer_type=TransferType.AMM_IN,
                ),
                SettlementTransfer(
                    token="0x3432B6A60D23Ca0dFCa7761B7ab56459D9C964D0",
                    amount=469613864284355328238,
                    incoming=False,
                    transfer_type=TransferType.USER_OUT,
                ),
            ],
        )
