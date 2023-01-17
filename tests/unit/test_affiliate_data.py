import datetime
import time
import unittest
from unittest.mock import MagicMock

from dune_client.client import DuneClient
from dune_client.models import (
    ResultsResponse,
    ExecutionState,
    ExecutionResult,
    TimeData,
    ResultMetadata,
)

from src.fetch.affiliate_data import (
    AffiliateData,
    AffiliateMemory,
    CachingAffiliateFetcher,
)


class TestCachingAffiliateFetcher(unittest.TestCase):
    """This test Mocks the Dune Client"""

    def setUp(self) -> None:
        self.dummy_times = TimeData(
            submitted_at=datetime.datetime(1970, 1, 1),
            cancelled_at=None,
            expires_at=None,
            execution_ended_at=None,
            execution_started_at=None,
        )
        self.dummy_metadata = ResultMetadata(
            column_names=[],
            datapoint_count=1,
            execution_time_millis=1,
            pending_time_millis=None,
            result_set_bytes=1,
            total_row_count=1,
        )
        self.empty_refresh_response = ResultsResponse(
            execution_id="1",
            query_id=1,
            state=ExecutionState.COMPLETED,
            result=ExecutionResult(
                rows=[],
                metadata=self.dummy_metadata,
            ),
            times=self.dummy_times,
        )
        self.trader = "0x1"
        self.get_result_response = ResultsResponse(
            execution_id="3",
            query_id=1,
            state=ExecutionState.COMPLETED,
            result=ExecutionResult(
                rows=[
                    {
                        "trader": self.trader,
                        "total_trades": "4",
                        "total_referrals": "5",
                        "trade_volume_usd": "156.7",
                        "referral_volume_usd": "18.9",
                    }
                ],
                metadata=self.dummy_metadata,
            ),
            times=self.dummy_times,
        )

    def test_cache_expired(self):
        mock_dune = DuneClient("api_key")
        mock_dune.refresh = MagicMock(return_value=self.empty_refresh_response)
        seconds = 3
        affiliate_fetcher = CachingAffiliateFetcher(
            dune=mock_dune, cache_validity=seconds / 60.0, execution_id=None
        )
        self.assertEqual(affiliate_fetcher.cache_expired(), False)
        time.sleep(seconds)
        self.assertEqual(affiliate_fetcher.cache_expired(), True)

    def test_get_affiliate_data_refresh(self):
        mock_dune = DuneClient("api_key")
        affiliate_result = {
            "trader": self.trader,
            "total_trades": "3",
            "total_referrals": "4",
            "trade_volume_usd": "56.7",
            "referral_volume_usd": "8.9",
        }
        mock_dune.refresh = MagicMock(
            return_value=ResultsResponse(
                execution_id="2",
                query_id=1,
                state=ExecutionState.COMPLETED,
                result=ExecutionResult(
                    rows=[affiliate_result],
                    metadata=self.dummy_metadata,
                ),
                times=self.dummy_times,
            )
        )
        affiliate_fetcher = CachingAffiliateFetcher(dune=mock_dune, execution_id=None)
        self.assertEqual(
            affiliate_fetcher.get_affiliate_data(self.trader),
            AffiliateData.from_dict(
                affiliate_result, last_update=affiliate_fetcher.memory.last_update
            ),
        )

        random_address = "0x123"
        self.assertEqual(
            affiliate_fetcher.get_affiliate_data(random_address),
            AffiliateData.default(),
        )

    def test_get_affiliate_data_from_execution_id(self):
        mock_dune = DuneClient("api_key")
        affiliate_result = {
            "trader": self.trader,
            "total_trades": "4",
            "total_referrals": "5",
            "trade_volume_usd": "156.7",
            "referral_volume_usd": "18.9",
        }
        mock_dune.get_result = MagicMock(
            return_value=ResultsResponse(
                execution_id="3",
                query_id=1,
                state=ExecutionState.COMPLETED,
                result=ExecutionResult(
                    rows=[affiliate_result],
                    metadata=self.dummy_metadata,
                ),
                times=self.dummy_times,
            )
        )
        affiliate_fetcher = CachingAffiliateFetcher(dune=mock_dune, execution_id="3")
        self.assertEqual(
            affiliate_fetcher.get_affiliate_data(self.trader),
            AffiliateData.from_dict(
                affiliate_result, last_update=affiliate_fetcher.memory.last_update
            ),
        )

        random_address = "0x123"
        self.assertEqual(
            affiliate_fetcher.get_affiliate_data(random_address),
            AffiliateData.default(),
        )

    def test_update_memory(self):
        mock_dune = DuneClient("api_key")
        affiliate_result_after = {
            "trader": self.trader,
            "total_trades": "4",
            "total_referrals": "5",
            "trade_volume_usd": "156.7",
            "referral_volume_usd": "18.9",
        }
        mock_dune.get_result = MagicMock(
            return_value=ResultsResponse(
                execution_id="3",
                query_id=1,
                state=ExecutionState.COMPLETED,
                result=ExecutionResult(
                    rows=[],
                    metadata=self.dummy_metadata,
                ),
                times=self.dummy_times,
            )
        )
        mock_dune.refresh = MagicMock(
            return_value=ResultsResponse(
                execution_id="4",
                query_id=1,
                state=ExecutionState.COMPLETED,
                result=ExecutionResult(
                    rows=[affiliate_result_after],
                    metadata=self.dummy_metadata,
                ),
                times=self.dummy_times,
            )
        )
        # Initialize with results from execution id (i.e. mocked get_result)
        seconds = 3
        affiliate_fetcher = CachingAffiliateFetcher(
            dune=mock_dune, execution_id="3", cache_validity=3 / 60.0
        )
        # Initially no results for this trader.
        self.assertEqual(
            affiliate_fetcher.get_affiliate_data(self.trader),
            AffiliateData.default(),
        )
        time.sleep(3)
        self.assertEqual(
            affiliate_fetcher.get_affiliate_data(self.trader),
            AffiliateData(
                totalTrades=4,
                tradeVolumeUsd=156.7,
                totalReferrals=5,
                referralVolumeUsd=18.9,
                lastUpdated=affiliate_fetcher.memory.last_update,
            ),
        )


class TestAffiliateData(unittest.TestCase):
    def test_from_dict(self):
        update = datetime.datetime(1970, 1, 1)
        self.assertEqual(
            AffiliateData.from_dict(
                {
                    "total_trades": "1",
                    "total_referrals": "2",
                    "trade_volume_usd": "123.4",
                    "referral_volume_usd": "567.8",
                },
                last_update=update,
            ),
            AffiliateData(
                totalTrades=1,
                tradeVolumeUsd=123.4,
                totalReferrals=2,
                referralVolumeUsd=567.8,
                lastUpdated=update,
            ),
        )

    def test_default(self):
        self.assertEqual(
            AffiliateData.default(),
            AffiliateData(
                totalTrades=0,
                tradeVolumeUsd=0,
                totalReferrals=0,
                referralVolumeUsd=0,
                lastUpdated=None,
            ),
        )


class TestAffiliateMemory(unittest.TestCase):
    def test_constructor(self):
        last_update = datetime.datetime(1970, 1, 1)
        last_execution_id = "ExecutionID"
        data = {
            "0x1": AffiliateData(
                totalTrades=1,
                tradeVolumeUsd=123.4,
                totalReferrals=2,
                referralVolumeUsd=567.8,
                lastUpdated=last_update,
            )
        }
        memory = AffiliateMemory(data, last_update, last_execution_id)
        # This test is kinda meaningless,
        # but would fail if someone changed the constructor in an unexpected way.
        self.assertEqual(memory.data, data)
        self.assertEqual(memory.last_update, last_update)
        self.assertEqual(memory.last_execution_id, last_execution_id)


if __name__ == "__main__":
    unittest.main()
