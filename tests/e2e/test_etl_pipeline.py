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
        # https://dune.com/queries/663231?TxHash=0xca0bbc3551a4e44c31a9fbd29f872f921548d33400e28debb07ffdc5c2d82370
        self.assertEqual(
            results,
            [
                TokenImbalance(
                    token="0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2",
                    amount=-1608243187495153737,
                ),
                TokenImbalance(
                    token="0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48", amount=231318775
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
                    token="0x5a98fcbea516cf06857215779fd812ca3bef1b32",
                    amount=2232582627542468223215,
                ),
            ],
        )

    def test_event_pipeline_0x3b2e(self):
        tx_hash = "0x3b2e9675b6d71a34e9b7f4abb4c9e80922be311076fcbb345d7da9d91a05e048"
        results = internal_transfers(tx_hash, self.file_manager)
        self.assertEqual(
            results,
            [],
        )

    def test_event_pipeline_0x7a00(self):
        tx_hash = "0x7a007eb8ad25f5f1f1f36459998ae758b0e699ca69cc7b4c38354d42092651bf"
        results = internal_transfers(tx_hash, self.file_manager)
        self.assertEqual(
            results,
            [
                TokenImbalance(
                    token="0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2",
                    amount=-95100807345736279,
                ),
                TokenImbalance(
                    token="0x64aa3364f17a4d01c6f1751fd97c2bd3d7e7f1d5",
                    amount=14916332565,
                ),
            ],
        )

    def test_event_pipeline_0x0e98(self):
        tx_hash = "0x0e9877bff7c9f9fb8516afc857d5bc986f8116bbf6972899c3eb65af4445901e"
        results = internal_transfers(tx_hash, self.file_manager)
        self.assertEqual(
            results,
            [
                TokenImbalance(
                    token="0x6810e776880c02933d47db1b9fc05908e5386b96",
                    amount=-1694865144280746549,
                ),
                TokenImbalance(
                    token="0x00a8b738e453ffd858a7edf03bccfe20412f0eb0",
                    amount=-1945523048541962118749,
                ),
                TokenImbalance(
                    token="0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2",
                    amount=99902973634572547,
                ),
            ],
        )

    def test_event_pipeline_0x42669(self):
        # Unusual slippage from prod-Laertes
        # https://dune.com/queries/1688044?MinAbsoluteSlippageTolerance=100&RelativeSlippageTolerance=1.0&SignificantSlippageValue=2000&TxHash=0x&StartTime=2023-02-15+00%3A00%3A00&EndTime=2023-02-16+00%3A00%3A00
        tx_hash = "0x426690f4385bf943dffc12c5e2adbfd793acc1d16b3a8f5fddcd9e3f94a5a20b"
        results = internal_transfers(tx_hash, self.file_manager)
        self.assertEqual(
            results,
            [
                TokenImbalance(
                    token="0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48",
                    amount=5677312578,
                ),
                TokenImbalance(
                    token="0x0f2d719407fdbeff09d87557abb7232601fd9f29",
                    amount=-4480160974861274910720,
                ),
                TokenImbalance(
                    token="0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2",
                    amount=1054375308649770183,
                ),
            ],
        )


if __name__ == "__main__":
    unittest.main()
