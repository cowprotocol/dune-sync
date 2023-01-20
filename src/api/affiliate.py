"""Basic Flask API"""
import os
from typing import Any

from dotenv import load_dotenv
from dune_client.client import DuneClient
from flask import Flask, jsonify
from flask.logging import create_logger

from src.fetch.affiliate_data import CachingAffiliateFetcher

app = Flask(__name__)
log = create_logger(app)
load_dotenv()
cached_fetcher = CachingAffiliateFetcher(
    dune=DuneClient(os.environ["DUNE_API_KEY"]),
    execution_id=os.environ.get("LAST_EXECUTION_ID"),
    cache_validity=int(os.environ["CACHE_VALIDITY"]),
)


@app.route("/profile/<string:address>", methods=["GET"])
def get_profile(address: str) -> Any:
    """
    Fetches Affiliate Profile for `address`
    https://api.cow.fi/affiliate/api/v1/profile/<address>
    """
    log.info(f"Fetching affiliate data for {address}")
    try:
        affiliate_data = cached_fetcher.get_affiliate_data(account=address)
    except ValueError as err:
        return str(err), 400

    return jsonify(affiliate_data)


if __name__ == "__main__":
    app.run()
