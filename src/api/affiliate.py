"""Basic Flask API"""
import os
from typing import Any

from dotenv import load_dotenv
from dune_client.client import DuneClient
from flask import Flask, jsonify

from src.fetch.affiliate_data import CachingAffiliateFetcher
from src.logger import set_log

app = Flask(__name__)
load_dotenv()
cached_fetcher = CachingAffiliateFetcher(
    dune=DuneClient(os.environ["DUNE_API_KEY"]),
    execution_id=os.environ.get("LAST_EXECUTION_ID"),
    cache_validity=int(os.environ["CACHE_VALIDITY"]),
)
log = set_log(__name__)


@app.route("/profile/<string:address>", methods=["GET"])
def get_profile(address: str) -> Any:
    """
    Fetches Affiliate Profile for `address`
    https://api.cow.fi/affiliate/api/v1/profile/<address>
    """
    log.info(f"Fetching affiliate data for {address}")
    affiliate_data = cached_fetcher.get_affiliate_data(account=address)
    log.debug(f"got results {affiliate_data}")
    return jsonify(affiliate_data)


if __name__ == "__main__":
    app.run()
