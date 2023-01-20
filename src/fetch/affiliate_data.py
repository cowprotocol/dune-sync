"""Caching Dune affiliate data fetcher"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Optional

from dune_client.client import DuneClient
from dune_client.query import Query
from dune_client.types import DuneRecord

from src.logger import set_log
from src.utils import utc_now, valid_address

AFFILIATE_QUERY_ID = 1757231
log = set_log(__name__)


@dataclass
class AffiliateData:  # pylint: disable=invalid-name
    """Represents the affiliate API return type"""

    totalTrades: int
    tradeVolumeUsd: float
    totalReferrals: int
    referralVolumeUsd: float
    lastUpdated: Optional[datetime]

    @classmethod
    def from_dict(cls, data_dict: DuneRecord, last_update: datetime) -> AffiliateData:
        """Constructor of data class from DuneRecord or any dict with compatible types"""
        return cls(
            totalTrades=int(data_dict["total_trades"]),
            totalReferrals=int(data_dict["total_referrals"]),
            tradeVolumeUsd=float(data_dict["trade_volume_usd"]),
            referralVolumeUsd=float(data_dict["referral_volume_usd"]),
            lastUpdated=last_update,
        )

    @classmethod
    def default(cls) -> AffiliateData:
        """Default return type for NotFound affiliates"""
        return cls(
            totalTrades=0,
            totalReferrals=0,
            tradeVolumeUsd=0.0,
            referralVolumeUsd=0.0,
            lastUpdated=None,
        )


@dataclass
class AffiliateMemory:
    """Indexed affiliate data with knowledge of data's age"""

    data: dict[str, AffiliateData]
    last_update: datetime
    last_execution_id: str


class CachingAffiliateFetcher:
    """
    Class containing, DuneClient, FileIO and a logger for convenient Dune Fetching.
    """

    def __init__(
        self, dune: DuneClient, execution_id: Optional[str], cache_validity: float = 30
    ) -> None:
        """
        Class constructor.
        Builds DuneClient from `api_key`.
        - `cache_validity` is in minutes.
        - `execution_id` can be used to avoid initial fetching if known
        """
        log.info("loading affiliate data cache")
        self.dune = dune
        self.memory: AffiliateMemory = self._fetch_affiliates(execution_id)
        self.cache_validity = timedelta(minutes=cache_validity)
        log.info("affiliate data cache loaded")

    def _fetch_affiliates(self, execution_id: Optional[str] = None) -> AffiliateMemory:
        """Fetches Affiliate data from Dune and updates cache"""
        if execution_id:
            results = self.dune.get_result(execution_id)
        else:
            # https://dune.com/queries/1757231?d=1
            # Query executes in ~24 seconds.
            results = self.dune.refresh(Query(AFFILIATE_QUERY_ID), ping_frequency=10)
            log.info(f"query execution {results.execution_id} succeeded")

        if not results.times.execution_ended_at:
            # This should not happen for successful query executions.
            log.warning(f"Missing execution time on result (using utc_now): {results}")
            last_update = utc_now()
        else:
            last_update = results.times.execution_ended_at

        return AffiliateMemory(
            last_update=last_update,
            last_execution_id=results.execution_id,
            data={
                row["trader"]: AffiliateData.from_dict(row, last_update=last_update)
                for row in results.get_rows()
            },
        )

    def _update_memory(self) -> None:
        """Internal method to update the cached affiliate data"""
        self.memory = self._fetch_affiliates()

    def cache_expired(self) -> bool:
        """
        Determines if data cache is expired.
        """
        before = self.memory.last_update
        delta = self.cache_validity
        return utc_now() > before + delta

    def get_affiliate_data(self, account: str) -> AffiliateData:
        """
        Returns affiliate data for `account`.
        If the record on hand is considered "too old",
        refreshes memory before return.
        """
        if not valid_address(account):
            message = f"Invalid address: {account}"
            log.warning(message)
            raise ValueError(message)

        if self.cache_expired():
            log.debug("cache expired - refreshing results")
            self._update_memory()

        clean_address = account.lower()
        result = self.memory.data.get(clean_address, AffiliateData.default())
        log.debug(f"Found result for {account}: {result}")
        return result
