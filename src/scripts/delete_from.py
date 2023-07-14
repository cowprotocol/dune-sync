"""
Script to empty AWS bucket.
Used for re-deployments involving schema change.
"""
import argparse

from dotenv import load_dotenv

from src.models.tables import SyncTable
from src.post.aws import AWSClient

if __name__ == "__main__":
    load_dotenv()
    parser = argparse.ArgumentParser("Script Arguments")
    parser.add_argument(
        "--from-block",
        type=int,
        required=True,
    )
    parser.add_argument(  # pylint: disable=duplicate-code
        "--sync-table",
        type=SyncTable,
        required=True,
        choices=list(SyncTable),
    )
    args, _ = parser.parse_known_args()
    aws = AWSClient.new_from_environment()
    aws.delete_from(args.sync_table, args.from_block)
