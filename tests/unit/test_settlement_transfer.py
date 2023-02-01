import unittest

from src.fetch.evm.events import (
    TransferEvent,
    TradeEvent,
    TransferType,
    EventMeta,
)
from src.models.settlement import SettlementTransfer
from src.models.tenderly import SimulationData, SimulationLog


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

    def test_from_events(self):
        trades = [
            TradeEvent(
                buy_token="0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48",
                sell_token="0xaDB2437e6F65682B85F814fBc12FeC0508A7B1D0",
                buy_amount=4475150642,
                sell_amount=10000000000000000000,
                fee_amount=90532473378739360,
                order_uid="8fc9b18fd8033d923c2129547df8e170967bb1a11f4a21d15d27fcaf769ca6e66d04b3f82a0b42a09f355fea3bccce3c819ad5b563c20502",
                owner="0x6d04B3f82a0B42A09F355fEA3bCCCE3C819Ad5B5",
                meta=EventMeta(
                    block_number=16401609,
                    index=10,
                    tx_hash="0xca0bbc3551a4e44c31a9fbd29f872f921548d33400e28debb07ffdc5c2d82370",
                ),
            ),
        ]

        transfers = [
            TransferEvent(
                meta=EventMeta(
                    block_number=16401607,
                    index=31,
                    tx_hash="0xe5835723740f0906e7ab01efd8d6831ce536b7618b81e9746f982c2a5fb5fe6d",
                ),
                token="0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48",
                src="0x9008d19f58aabd9ed0d60971565aa8510560ab41",
                dst="0x6d04b3f82a0b42a09f355fea3bccce3c819ad5b5",
                wad=4475150642,
            ),
        ]

        self.assertEqual(
            SettlementTransfer.from_events(trades, transfers),
            [
                SettlementTransfer(
                    token="0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48",
                    amount=4475150642,
                    incoming=False,
                    transfer_type=TransferType.USER_OUT,
                )
            ],
        )

    def test_settlement_from(self):
        logs = [
            {
                "name": "",
                "anonymous": False,
                "inputs": None,
                "raw": {
                    "address": "0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2",
                    "topics": [
                        "0xe1fffcc4923d04b559f4d29a8bfc6cda04eb5b0d3c460751c2402c5c5cc9109c",
                        "0x00000000000000000000000040a50cf069e992aa4536211b23f286ef88752187",
                    ],
                    "data": "0x00000000000000000000000000000000000000000000000029a2241af62c0000",
                },
            },
            {
                "name": "",
                "anonymous": False,
                "inputs": None,
                "raw": {
                    "address": "0x9008d19f58aabd9ed0d60971565aa8510560ab41",
                    "topics": [
                        "0xed99827efb37016f2275f98c4bcf71c7551c75d59e9b450f79fa32e60be672c2",
                        "0x00000000000000000000000040a50cf069e992aa4536211b23f286ef88752187",
                    ],
                    "data": "0x00000000000000000000000000000000000000000000000000000000000000004c84c1c800000000000000000000000000000000000000000000000000000000",
                },
            },
            {
                "name": "",
                "anonymous": False,
                "inputs": None,
                "raw": {
                    "address": "0x9008d19f58aabd9ed0d60971565aa8510560ab41",
                    "topics": [
                        "0xa07a543ab8a018198e99ca0184c93fe9050a79400a0a723441f84de1d972cc17",
                        "0x00000000000000000000000040a50cf069e992aa4536211b23f286ef88752187",
                    ],
                    "data": "0x000000000000000000000000c02aaa39b223fe8d0a0e5c4f27ead9083c756cc20000000000000000000000006b175474e89094c44da98b954eedeac495271d0f00000000000000000000000000000000000000000000000014d1120d7b16000000000000000000000000000000000000000000000000007ac331741c3e81b263000000000000000000000000000000000000000000000000005ac91011bfd9f800000000000000000000000000000000000000000000000000000000000000c00000000000000000000000000000000000000000000000000000000000000038e674e87f4f5dfd20cc710365c9711e0d56c3b0532fc37d4832adbe45096bd82640a50cf069e992aa4536211b23f286ef88752187ffffffff0000000000000000",
                },
            },
            {
                "name": "",
                "anonymous": False,
                "inputs": None,
                "raw": {
                    "address": "0x9008d19f58aabd9ed0d60971565aa8510560ab41",
                    "topics": [
                        "0xa07a543ab8a018198e99ca0184c93fe9050a79400a0a723441f84de1d972cc17",
                        "0x0000000000000000000000006d04b3f82a0b42a09f355fea3bccce3c819ad5b5",
                    ],
                    "data": "0x000000000000000000000000adb2437e6f65682b85f814fbc12fec0508a7b1d0000000000000000000000000a0b86991c6218b36c1d19d4a2e9eb0ce3606eb480000000000000000000000000000000000000000000000008ac7230489e80000000000000000000000000000000000000000000000000000000000010abd61320000000000000000000000000000000000000000000000000141a2ce0c6094a000000000000000000000000000000000000000000000000000000000000000c000000000000000000000000000000000000000000000000000000000000000388fc9b18fd8033d923c2129547df8e170967bb1a11f4a21d15d27fcaf769ca6e66d04b3f82a0b42a09f355fea3bccce3c819ad5b563c205020000000000000000",
                },
            },
            {
                "name": "",
                "anonymous": False,
                "inputs": None,
                "raw": {
                    "address": "0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2",
                    "topics": [
                        "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef",
                        "0x00000000000000000000000040a50cf069e992aa4536211b23f286ef88752187",
                        "0x0000000000000000000000009008d19f58aabd9ed0d60971565aa8510560ab41",
                    ],
                    "data": "0x00000000000000000000000000000000000000000000000014d1120d7b160000",
                },
            },
            {
                "name": "",
                "anonymous": False,
                "inputs": None,
                "raw": {
                    "address": "0xadb2437e6f65682b85f814fbc12fec0508a7b1d0",
                    "topics": [
                        "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef",
                        "0x0000000000000000000000006d04b3f82a0b42a09f355fea3bccce3c819ad5b5",
                        "0x0000000000000000000000009008d19f58aabd9ed0d60971565aa8510560ab41",
                    ],
                    "data": "0x0000000000000000000000000000000000000000000000008ac7230489e80000",
                },
            },
            {
                "name": "",
                "anonymous": False,
                "inputs": None,
                "raw": {
                    "address": "0xadb2437e6f65682b85f814fbc12fec0508a7b1d0",
                    "topics": [
                        "0x8c5be1e5ebec7d5bd14f71427d1e84f3dd0314c0f7b2291e5b200ac8c7c3b925",
                        "0x0000000000000000000000006d04b3f82a0b42a09f355fea3bccce3c819ad5b5",
                        "0x000000000000000000000000c92e8bdf79f0507f65a392b0ab4667716bfe0110",
                    ],
                    "data": "0xffffffffffffffffffffffffffffffffffffffffffffffe6cd534a826c469e6d",
                },
            },
            {
                "name": "",
                "anonymous": False,
                "inputs": None,
                "raw": {
                    "address": "0xadb2437e6f65682b85f814fbc12fec0508a7b1d0",
                    "topics": [
                        "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef",
                        "0x0000000000000000000000009008d19f58aabd9ed0d60971565aa8510560ab41",
                        "0x000000000000000000000000c70bb2736e218861dca818d1e9f7a1930fe61e5b",
                    ],
                    "data": "0x000000000000000000000000000000000000000000000000898580367d876556",
                },
            },
            {
                "name": "",
                "anonymous": False,
                "inputs": None,
                "raw": {
                    "address": "0xadb2437e6f65682b85f814fbc12fec0508a7b1d0",
                    "topics": [
                        "0x8c5be1e5ebec7d5bd14f71427d1e84f3dd0314c0f7b2291e5b200ac8c7c3b925",
                        "0x0000000000000000000000009008d19f58aabd9ed0d60971565aa8510560ab41",
                        "0x0000000000000000000000007a250d5630b4cf539739df2c5dacb4c659f2488d",
                    ],
                    "data": "0xffffffffffffffffffffffffffffffffffffffffffffffeceaae1a4c679b1f5b",
                },
            },
            {
                "name": "",
                "anonymous": False,
                "inputs": None,
                "raw": {
                    "address": "0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2",
                    "topics": [
                        "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef",
                        "0x000000000000000000000000c70bb2736e218861dca818d1e9f7a1930fe61e5b",
                        "0x0000000000000000000000009008d19f58aabd9ed0d60971565aa8510560ab41",
                    ],
                    "data": "0x0000000000000000000000000000000000000000000000002818b312c5b23fce",
                },
            },
            {
                "name": "",
                "anonymous": False,
                "inputs": None,
                "raw": {
                    "address": "0xc70bb2736e218861dca818d1e9f7a1930fe61e5b",
                    "topics": [
                        "0x1c411e9a96e071241c2f21f7726b17ae89e3cab4c78be50e062b03a9fffbbad1"
                    ],
                    "data": "0x0000000000000000000000000000000000000000000000590f958197799fb3d4000000000000000000000000000000000000000000000019e359eae47c9aae33",
                },
            },
            {
                "name": "",
                "anonymous": False,
                "inputs": None,
                "raw": {
                    "address": "0xc70bb2736e218861dca818d1e9f7a1930fe61e5b",
                    "topics": [
                        "0xd78ad95fa46c994b6551d0da85fc275fe613ce37657fb8d5e3d130840159d822",
                        "0x0000000000000000000000007a250d5630b4cf539739df2c5dacb4c659f2488d",
                        "0x0000000000000000000000009008d19f58aabd9ed0d60971565aa8510560ab41",
                    ],
                    "data": "0x000000000000000000000000000000000000000000000000898580367d876556000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000002818b312c5b23fce",
                },
            },
            {
                "name": "",
                "anonymous": False,
                "inputs": None,
                "raw": {
                    "address": "0x9008d19f58aabd9ed0d60971565aa8510560ab41",
                    "topics": [
                        "0xed99827efb37016f2275f98c4bcf71c7551c75d59e9b450f79fa32e60be672c2",
                        "0x0000000000000000000000007a250d5630b4cf539739df2c5dacb4c659f2488d",
                    ],
                    "data": "0x00000000000000000000000000000000000000000000000000000000000000008803dbee00000000000000000000000000000000000000000000000000000000",
                },
            },
            {
                "name": "",
                "anonymous": False,
                "inputs": None,
                "raw": {
                    "address": "0xba12222222228d8ba445958a75a0704d566bf2c8",
                    "topics": [
                        "0x2170c741c41531aec20e7c107c24eecfdd15e69c9bb0a8dd37b1840b9e0b207b",
                        "0x265b6d1a6c12873a423c177eba6dd2470f40a3b50001000000000000000003fd",
                        "0x000000000000000000000000c02aaa39b223fe8d0a0e5c4f27ead9083c756cc2",
                        "0x000000000000000000000000a0b86991c6218b36c1d19d4a2e9eb0ce3606eb48",
                    ],
                    "data": "0x00000000000000000000000000000000000000000000000001f291c5ddb829ad000000000000000000000000000000000000000000000000000000000d059811",
                },
            },
            {
                "name": "",
                "anonymous": False,
                "inputs": None,
                "raw": {
                    "address": "0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2",
                    "topics": [
                        "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef",
                        "0x0000000000000000000000009008d19f58aabd9ed0d60971565aa8510560ab41",
                        "0x000000000000000000000000ba12222222228d8ba445958a75a0704d566bf2c8",
                    ],
                    "data": "0x00000000000000000000000000000000000000000000000001f291c5ddb829ad",
                },
            },
            {
                "name": "",
                "anonymous": False,
                "inputs": None,
                "raw": {
                    "address": "0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48",
                    "topics": [
                        "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef",
                        "0x000000000000000000000000ba12222222228d8ba445958a75a0704d566bf2c8",
                        "0x0000000000000000000000009008d19f58aabd9ed0d60971565aa8510560ab41",
                    ],
                    "data": "0x000000000000000000000000000000000000000000000000000000000d059811",
                },
            },
            {
                "name": "",
                "anonymous": False,
                "inputs": None,
                "raw": {
                    "address": "0x9008d19f58aabd9ed0d60971565aa8510560ab41",
                    "topics": [
                        "0xed99827efb37016f2275f98c4bcf71c7551c75d59e9b450f79fa32e60be672c2",
                        "0x000000000000000000000000ba12222222228d8ba445958a75a0704d566bf2c8",
                    ],
                    "data": "0x000000000000000000000000000000000000000000000000000000000000000052bbbe2900000000000000000000000000000000000000000000000000000000",
                },
            },
            {
                "name": "",
                "anonymous": False,
                "inputs": None,
                "raw": {
                    "address": "0x6b175474e89094c44da98b954eedeac495271d0f",
                    "topics": [
                        "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef",
                        "0x00000000000000000000000060594a405d53811d3bc4766596efd80fd545a270",
                        "0x0000000000000000000000009008d19f58aabd9ed0d60971565aa8510560ab41",
                    ],
                    "data": "0x00000000000000000000000000000000000000000000001856e41db56c42a445",
                },
            },
            {
                "name": "",
                "anonymous": False,
                "inputs": None,
                "raw": {
                    "address": "0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2",
                    "topics": [
                        "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef",
                        "0x0000000000000000000000009008d19f58aabd9ed0d60971565aa8510560ab41",
                        "0x00000000000000000000000060594a405d53811d3bc4766596efd80fd545a270",
                    ],
                    "data": "0x000000000000000000000000000000000000000000000000040a49411ddfb69d",
                },
            },
            {
                "name": "",
                "anonymous": False,
                "inputs": None,
                "raw": {
                    "address": "0x60594a405d53811d3bc4766596efd80fd545a270",
                    "topics": [
                        "0xc42079f94a6350d7e6235f29174924f928cc2ac818eb64fed8004e115fbcca67",
                        "0x000000000000000000000000e592427a0aece92de3edee1f18e0157c05861564",
                        "0x0000000000000000000000009008d19f58aabd9ed0d60971565aa8510560ab41",
                    ],
                    "data": "0xffffffffffffffffffffffffffffffffffffffffffffffe7a91be24a93bd5bbb000000000000000000000000000000000000000000000000040a49411ddfb69d00000000000000000000000000000000000000000684769dd8ae059c7b7ae75e000000000000000000000000000000000000000000001a3748aba0bc03ef4fddfffffffffffffffffffffffffffffffffffffffffffffffffffffffffffee136",
                },
            },
            {
                "name": "",
                "anonymous": False,
                "inputs": None,
                "raw": {
                    "address": "0x9008d19f58aabd9ed0d60971565aa8510560ab41",
                    "topics": [
                        "0xed99827efb37016f2275f98c4bcf71c7551c75d59e9b450f79fa32e60be672c2",
                        "0x000000000000000000000000e592427a0aece92de3edee1f18e0157c05861564",
                    ],
                    "data": "0x0000000000000000000000000000000000000000000000000000000000000000414bf38900000000000000000000000000000000000000000000000000000000",
                },
            },
            {
                "name": "",
                "anonymous": False,
                "inputs": None,
                "raw": {
                    "address": "0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48",
                    "topics": [
                        "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef",
                        "0x00000000000000000000000088e6a0c2ddd26feeb64f039a2c41296fcb3f5640",
                        "0x0000000000000000000000009008d19f58aabd9ed0d60971565aa8510560ab41",
                    ],
                    "data": "0x00000000000000000000000000000000000000000000000000000000fdb7c878",
                },
            },
            {
                "name": "",
                "anonymous": False,
                "inputs": None,
                "raw": {
                    "address": "0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2",
                    "topics": [
                        "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef",
                        "0x0000000000000000000000009008d19f58aabd9ed0d60971565aa8510560ab41",
                        "0x00000000000000000000000088e6a0c2ddd26feeb64f039a2c41296fcb3f5640",
                    ],
                    "data": "0x000000000000000000000000000000000000000000000000263d5b852b883958",
                },
            },
            {
                "name": "",
                "anonymous": False,
                "inputs": None,
                "raw": {
                    "address": "0x88e6a0c2ddd26feeb64f039a2c41296fcb3f5640",
                    "topics": [
                        "0xc42079f94a6350d7e6235f29174924f928cc2ac818eb64fed8004e115fbcca67",
                        "0x000000000000000000000000e592427a0aece92de3edee1f18e0157c05861564",
                        "0x0000000000000000000000009008d19f58aabd9ed0d60971565aa8510560ab41",
                    ],
                    "data": "0xffffffffffffffffffffffffffffffffffffffffffffffffffffffff02483788000000000000000000000000000000000000000000000000263d5b852b883958000000000000000000000000000000000000635c584f9558463257784ca142a6000000000000000000000000000000000000000000000000b3306b8de5c793780000000000000000000000000000000000000000000000000000000000031888",
                },
            },
            {
                "name": "",
                "anonymous": False,
                "inputs": None,
                "raw": {
                    "address": "0x9008d19f58aabd9ed0d60971565aa8510560ab41",
                    "topics": [
                        "0xed99827efb37016f2275f98c4bcf71c7551c75d59e9b450f79fa32e60be672c2",
                        "0x000000000000000000000000e592427a0aece92de3edee1f18e0157c05861564",
                    ],
                    "data": "0x0000000000000000000000000000000000000000000000000000000000000000414bf38900000000000000000000000000000000000000000000000000000000",
                },
            },
            {
                "name": "",
                "anonymous": False,
                "inputs": None,
                "raw": {
                    "address": "0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2",
                    "topics": [
                        "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef",
                        "0x0000000000000000000000009008d19f58aabd9ed0d60971565aa8510560ab41",
                        "0x000000000000000000000000c3d03e4f041fd4cd388c549ee2a29a9e5075882f",
                    ],
                    "data": "0x0000000000000000000000000000000000000000000000001054c5a1837f4fff",
                },
            },
            {
                "name": "",
                "anonymous": False,
                "inputs": None,
                "raw": {
                    "address": "0x6b175474e89094c44da98b954eedeac495271d0f",
                    "topics": [
                        "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef",
                        "0x000000000000000000000000c3d03e4f041fd4cd388c549ee2a29a9e5075882f",
                        "0x0000000000000000000000009008d19f58aabd9ed0d60971565aa8510560ab41",
                    ],
                    "data": "0x0000000000000000000000000000000000000000000000626c4d4ecf55e0f384",
                },
            },
            {
                "name": "",
                "anonymous": False,
                "inputs": None,
                "raw": {
                    "address": "0xc3d03e4f041fd4cd388c549ee2a29a9e5075882f",
                    "topics": [
                        "0x1c411e9a96e071241c2f21f7726b17ae89e3cab4c78be50e062b03a9fffbbad1"
                    ],
                    "data": "0x000000000000000000000000000000000000000000030c6db920c29e0da598b70000000000000000000000000000000000000000000000812b96942b3b59481d",
                },
            },
            {
                "name": "",
                "anonymous": False,
                "inputs": None,
                "raw": {
                    "address": "0xc3d03e4f041fd4cd388c549ee2a29a9e5075882f",
                    "topics": [
                        "0xd78ad95fa46c994b6551d0da85fc275fe613ce37657fb8d5e3d130840159d822",
                        "0x000000000000000000000000d9e1ce17f2641f24ae83637ab66a2cca9c378b9f",
                        "0x0000000000000000000000009008d19f58aabd9ed0d60971565aa8510560ab41",
                    ],
                    "data": "0x00000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000001054c5a1837f4fff0000000000000000000000000000000000000000000000626c4d4ecf55e0f3840000000000000000000000000000000000000000000000000000000000000000",
                },
            },
            {
                "name": "",
                "anonymous": False,
                "inputs": None,
                "raw": {
                    "address": "0x9008d19f58aabd9ed0d60971565aa8510560ab41",
                    "topics": [
                        "0xed99827efb37016f2275f98c4bcf71c7551c75d59e9b450f79fa32e60be672c2",
                        "0x000000000000000000000000d9e1ce17f2641f24ae83637ab66a2cca9c378b9f",
                    ],
                    "data": "0x00000000000000000000000000000000000000000000000000000000000000008803dbee00000000000000000000000000000000000000000000000000000000",
                },
            },
            {
                "name": "",
                "anonymous": False,
                "inputs": None,
                "raw": {
                    "address": "0x6b175474e89094c44da98b954eedeac495271d0f",
                    "topics": [
                        "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef",
                        "0x0000000000000000000000009008d19f58aabd9ed0d60971565aa8510560ab41",
                        "0x0000000000000000000000005dcc9dc962f391517240718d1bebbbe78d24fac7",
                    ],
                    "data": "0x00000000000000000000000000000000000000000000007ac331741c3e81b263",
                },
            },
            {
                "name": "",
                "anonymous": False,
                "inputs": None,
                "raw": {
                    "address": "0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48",
                    "topics": [
                        "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef",
                        "0x0000000000000000000000009008d19f58aabd9ed0d60971565aa8510560ab41",
                        "0x0000000000000000000000006d04b3f82a0b42a09f355fea3bccce3c819ad5b5",
                    ],
                    "data": "0x000000000000000000000000000000000000000000000000000000010abd6132",
                },
            },
            {
                "name": "",
                "anonymous": False,
                "inputs": None,
                "raw": {
                    "address": "0x9008d19f58aabd9ed0d60971565aa8510560ab41",
                    "topics": [
                        "0x40338ce1a7c49204f0099533b1e9a7ee0a3d261f84974ab7af36105b8c4e9db4",
                        "0x00000000000000000000000097ec0a17432d71a3234ef7173c6b48a2c0940896",
                    ],
                    "data": "0x",
                },
            },
        ]
        simulation = SimulationData(
            block_number=0,
            tx_hash="0x1",
            logs=[SimulationLog.from_dict(log["raw"]) for log in logs],
        )

        transfers = TransferEvent.from_tenderly_simulation(simulation)
        self.assertEqual(len(transfers), 14)
        dummy_meta = EventMeta(block_number=0, index=0, tx_hash="0x")
        settlement_transfers = SettlementTransfer.from_events(
            trades=[
                TradeEvent(
                    owner="0x6d04b3f82a0b42a09f355fea3bccce3c819ad5b5",
                    sell_token="0xadb2437e6f65682b85f814fbc12fec0508a7b1d0",
                    buy_token="0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48",
                    sell_amount=10000000000000000000,
                    buy_amount=4475150642,
                    fee_amount=90532473378739360,
                    order_uid="0x8fc9b18fd8033d923c2129547df8e170967bb1a11f4a21d15d27fcaf769ca6e66d04b3f82a0b42a09f355fea3bccce3c819ad5b563c20502",
                    meta=dummy_meta,
                ),
                TradeEvent(
                    owner="0x40a50cf069e992aa4536211b23f286ef88752187",
                    sell_token="0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2",
                    buy_token="0x6b175474e89094c44da98b954eedeac495271d0f",
                    sell_amount=1500000000000000000,
                    buy_amount=2264567927768476660323,
                    fee_amount=25553818758404600,
                    order_uid="0xe674e87f4f5dfd20cc710365c9711e0d56c3b0532fc37d4832adbe45096bd82640a50cf069e992aa4536211b23f286ef88752187ffffffff",
                    meta=dummy_meta,
                ),
            ],
            transfers=transfers,
        )

        self.assertLessEqual(len(settlement_transfers), len(transfers))
        print(len(settlement_transfers))
