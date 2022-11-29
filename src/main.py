"""Main Entry point for app_hash sync"""
import asyncio
import logging.config
import os
from pathlib import Path

from dotenv import load_dotenv

from src.environment import PROJECT_ROOT
from src.sync import sync_app_data
from src.fetch.dune import DuneFetcher
from src.sync.config import AppDataSyncConfig, AWSData

log = logging.getLogger(__name__)
logging.basicConfig(format="%(asctime)s %(levelname)s %(name)s %(message)s")
log.setLevel(logging.DEBUG)


GIVE_UP_THRESHOLD = 10


if __name__ == "__main__":
    load_dotenv()
    asyncio.run(
        sync_app_data(
            dune=DuneFetcher(os.environ["DUNE_API_KEY"]),
            config=AppDataSyncConfig(
                aws=AWSData(
                    internal_role=os.environ["AWS_INTERNAL_ROLE"],
                    external_role=os.environ["AWS_EXTERNAL_ROLE"],
                    external_id=os.environ["AWS_EXTERNAL_ID"],
                    bucket=os.environ["AWS_BUCKET"],
                ),
                volume_path=PROJECT_ROOT / Path(os.environ.get("VOLUME_PATH", "data")),
            ),
        )
    )
