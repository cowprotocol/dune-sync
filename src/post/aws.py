"""Aws S3 Bucket functionality (namely upload_file)"""
import logging

import boto3
from boto3.exceptions import S3UploadFailedError
from boto3.s3.transfer import S3Transfer


def upload_file(
    s3_client: S3Transfer, filename: str, bucket: str, object_key: str
) -> bool:
    """Upload a file to an S3 bucket

    :param s3_client: S3Transfer object with `upload_file` method
    :param filename: File to upload. Should be a full path to file.
    :param bucket: Bucket to upload to
    :param object_key: S3 object key. For our purposes, this would
                       be f"{table_name}/cow_{latest_block_number}.json"
    :return: True if file was uploaded, else False
    """

    try:
        s3_client.upload_file(
            filename,
            bucket,
            key=object_key,
            extra_args={"ACL": "bucket-owner-full-control"},
        )
        logging.info(f"successfully uploaded {filename} to {bucket}")
        return True
    except S3UploadFailedError as err:
        logging.error(err)
        return False


def get_s3_client(profile: str) -> S3Transfer:
    """Constructs a client session for S3 Bucket upload."""
    session = boto3.Session(profile_name=profile)
    return S3Transfer(session.client("s3"))
