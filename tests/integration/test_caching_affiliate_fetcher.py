import os
import os
import unittest

from dune_client.client import DuneClient

from src.fetch.affiliate_data import (
    CachingAffiliateFetcher,
)


class TestCachingAffiliateFetcher(unittest.TestCase):
    """This test requires valid API Key"""

    def setUp(self) -> None:
        self.dune = DuneClient(os.environ["DUNE_API_KEY"])

    def test_affiliate_fetching(self):
        fetcher = CachingAffiliateFetcher(self.dune, execution_id=None)
        known_trader_and_referrer = "0x0d5dc686d0a2abbfdafdfb4d0533e886517d4e83"
        affiliate_data = fetcher.get_affiliate_data(known_trader_and_referrer)
        # As of January 17, 2023
        self.assertGreater(affiliate_data.totalTrades, 241)
        self.assertGreater(affiliate_data.totalReferrals, 7)
        self.assertGreater(affiliate_data.tradeVolumeUsd, 75687577)
        self.assertGreater(affiliate_data.referralVolumeUsd, 75788291)

    def test_affiliate_fetching_deterministic(self):
        fetcher = CachingAffiliateFetcher(
            self.dune, execution_id="01GQ0YASEDSE1W7MENGTT1Q3AJ"
        )
        known_trader_and_referrer = "0x0d5dc686d0a2abbfdafdfb4d0533e886517d4e83"
        affiliate_data = fetcher.get_affiliate_data(known_trader_and_referrer)

        self.assertEqual(affiliate_data.totalTrades, 242)
        self.assertEqual(affiliate_data.totalReferrals, 8)
        self.assertAlmostEqual(affiliate_data.tradeVolumeUsd, 75687577.26913233)
        self.assertAlmostEqual(affiliate_data.referralVolumeUsd, 75788291.13699351)
