"""Downloads file from AWS to PWD"""
import argparse

from dotenv import load_dotenv

from src.post.aws import AWSClient


def download_file(aws: AWSClient, object_key: str) -> None:
    """Download file (`object_key`) from AWS"""
    aws.download_file(
        filename=object_key.split("/")[1],
        object_key=object_key,
    )


if __name__ == "__main__":
    load_dotenv()
    parser = argparse.ArgumentParser("Download File from Bucket")
    parser.add_argument(
        "filename",
        type=str,
        required=True,
    )
    args, _ = parser.parse_known_args()
    download_file(aws=AWSClient.new_from_environment(), object_key=args.object_key)
