import unittest

from dune_client.file.interface import FileIO

from src.environment import PROJECT_ROOT
from src.event_pipe import internal_transfers
from src.models.settlement import TokenImbalance


class TestFullPipeline(unittest.TestCase):
    def setUp(self) -> None:
        self.file_manager = FileIO(path=f"{PROJECT_ROOT}/out")

    def test_event_pipeline_0xca0(self):
        tx_hash = "0xca0bbc3551a4e44c31a9fbd29f872f921548d33400e28debb07ffdc5c2d82370"
        # Saved Simulations
        # Reduced
        # https://dashboard.tenderly.co/bh2smith/project/simulator/dec3ad28-a055-4153-b5a7-c7cb08f1cfec/logs
        # Full
        # https://dashboard.tenderly.co/bh2smith/project/simulator/5ae918be-8df0-4789-a872-e64514705614/logs
        results = internal_transfers(tx_hash, self.file_manager)

        # Check that internalized imbalance "matches" (negatively)
        # https://dune.com/queries/663231?TxHash={tx_hash}
        self.assertEqual(
            results,
            [
                TokenImbalance(
                    token="0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2",
                    amount=-1608243187495153737,
                ),
                TokenImbalance(
                    token="0xadb2437e6f65682b85f814fbc12fec0508a7b1d0", amount=0
                ),
                TokenImbalance(
                    token="0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48", amount=218470417
                ),
                TokenImbalance(
                    token="0x6b175474e89094c44da98b954eedeac495271d0f",
                    amount=2264567919421268662217,
                ),
            ],
        )

    def test_event_pipeline_0x82c(self):
        """
        This TxData doesn't simulate successfully:
            http://jsonblob.com/1070286702807105536

        See slack: https://cowservices.slack.com/archives/C0375NV72SC/p1675248855853289
        """
        tx_hash = "0x82c20f4583fb2a49a1db506ef2a1777a3efc99d90d100f7d2da9ca718de395f2"
        # Saved Simulations
        # Reduced (FAILING)
        # https://dashboard.tenderly.co/bh2smith/project/simulator/fcac6bd3-c519-406e-89c0-710b24989c1b
        # Full
        # https://dashboard.tenderly.co/bh2smith/project/simulator/592786f2-c9a7-4a11-a362-7404c81ae54f
        results = internal_transfers(tx_hash, self.file_manager)

        # Check that internalized imbalance "matches" (negatively)
        # https://dune.com/queries/663231?TxHash=0x82c20f4583fb2a49a1db506ef2a1777a3efc99d90d100f7d2da9ca718de395f2
        self.assertEqual(
            results,
            [
                # Expected
                TokenImbalance(
                    token="0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2",
                    amount=-4160765679312911865,
                ),
                # Expected
                TokenImbalance(
                    token="0xdef1ca1fb7fbcdc777520aa7f396b4e015f497ab",
                    amount=3364304704062661651393,
                ),
                # Expected
                TokenImbalance(
                    token="0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48",
                    amount=6556036868,
                ),
            ],
        )

    def test_event_pipeline_0xc6a(self):
        tx_hash = "0xc6a48f8c08dad2742fa225246da2becec44d87c54e5dadb516d34c1cffc3f2d5"
        results = internal_transfers(tx_hash, self.file_manager)
        # Simulations
        # Reduced Simulation
        # https://dashboard.tenderly.co/gp-v2/solver-slippage/simulator/5e82c551-e737-4ae5-a3bc-0c2efbfd69b1
        # Full Simulation
        # https://dashboard.tenderly.co/gp-v2/solver-slippage/simulator/25e1ebb7-d80c-4691-af2e-1d43a1276329

        # Check that internalized imbalance "matches" (negatively)
        # https://dune.com/queries/663231?TxHash=0xc6a48f8c08dad2742fa225246da2becec44d87c54e5dadb516d34c1cffc3f2d5
        self.assertEqual(
            results,
            [
                # Expected.
                TokenImbalance(
                    token="0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48",
                    amount=-3763821350,
                ),
                # This is expected but did not appear in the on chain data because
                # this token was not at all involved.
                # (must have been a multi-hop token that was completely avoided)
                TokenImbalance(
                    token="0x7d1afa7b718fb893db30a3abc0cfc608aacfebb0",
                    amount=42476141134110,
                ),
                # Expected.
                TokenImbalance(
                    token="0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2",
                    amount=-622410208819002243,
                ),
                # Expected.
                TokenImbalance(
                    token="0x2260fac5e5542a773aa44fbcfedf7c193bc2c599", amount=0
                ),
                # Expected.
                TokenImbalance(
                    token="0x5a98fcbea516cf06857215779fd812ca3bef1b32",
                    amount=2232582627542468223215,
                ),
            ],
        )


if __name__ == "__main__":
    unittest.main()
