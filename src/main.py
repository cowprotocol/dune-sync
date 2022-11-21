"""Main Entry point for app_hash sync"""
import asyncio
import logging.config
import os
from pathlib import Path

from dotenv import load_dotenv

from src.sync import sync_app_data
from src.fetch.dune import DuneFetcher
from src.sync.config import AppDataSyncConfig

log = logging.getLogger(__name__)
logging.basicConfig(format="%(asctime)s %(levelname)s %(name)s %(message)s")
log.setLevel(logging.DEBUG)


GIVE_UP_THRESHOLD = 10

PROJECT_ROOT = Path(__file__).parent.parent

if __name__ == "__main__":
    load_dotenv()
    asyncio.run(
        sync_app_data(
            dune=DuneFetcher(os.environ["DUNE_API_KEY"]),
            config=AppDataSyncConfig(
                aws_role=os.environ["AWS_ROLE"],
                aws_bucket=os.environ["AWS_BUCKET"],
                volume_path=PROJECT_ROOT / Path(os.environ.get("VOLUME_PATH", "data")),
            ),
        )
    )
