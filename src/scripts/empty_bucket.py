"""
Script to empty AWS bucket.
Used for re-deployments involving schema change.
"""
import os
import shutil
from pathlib import Path

from dotenv import load_dotenv

from src.main import ScriptArgs
from src.models.tables import SyncTable
from src.post.aws import AWSClient


def empty_bucket(table: SyncTable, aws: AWSClient, volume_path: Path) -> None:
    """
    Empties the bucket for `table`
    and deletes backup from mounted volume
    """
    # Drop Data from AWS bucket
    aws.delete_all(table)
    # drop backup data from volume path
    shutil.rmtree(volume_path / str(table))


if __name__ == "__main__":
    load_dotenv()
    empty_bucket(
        table=ScriptArgs().sync_table,
        aws=AWSClient.new_from_environment(),
        volume_path=Path(os.environ["VOLUME_PATH"]),
    )
