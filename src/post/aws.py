"""Aws S3 Bucket functionality (namely upload_file)"""
import logging

import boto3
from boto3.exceptions import S3UploadFailedError
from boto3.s3.transfer import S3Transfer


def upload_file(
    s3_client: S3Transfer, file_name: str, bucket: str, object_key: str
) -> bool:
    """Upload a file to an S3 bucket

    :param s3_client: S3Transfer object with `upload_file` method
    :param file_name: File to upload. Should be a full path to file.
    :param bucket: Bucket to upload to
    :param object_key: S3 object key. For our purposes, this would
                       be f"{table_name}/cow_{latest_block_number}.json"
    :return: True if file was uploaded, else False
    """

    try:
        s3_client.upload_file(
            filename=file_name,
            bucket=bucket,
            key=object_key,
            extra_args={"ACL": "bucket-owner-full-control"},
        )
        logging.info(f"successfully uploaded {file_name} to {bucket} with response")
        return True
    except S3UploadFailedError as err:
        logging.error(err)
        return False


def get_s3_client(profile: str) -> S3Transfer:
    """Constructs a client session for S3 Bucket upload."""
    # This page suggests:
    # https://boto3.amazonaws.com/v1/documentation/api/latest/guide/credentials.html#aws-iam-identity-center
    session = boto3.Session(profile_name=profile)
    return S3Transfer(session.client("s3"))

    # First attempt
    # return boto3.client(
    #     "s3",
    #     aws_access_key_id=os.environ["AWS_ACCESS_KEY"],
    #     aws_secret_access_key=os.environ["AWS_SECRET_KEY"],
    # )

    # This is how flashbots instantiates a connection: here
    # https://github.com/flashbots/mev-inspect-py/blob/d917ae72ded847af9cbdda0e87a1f38f94f4cb55/mev_inspect/s3_export.py#L103-L111
    # return boto3.client(
    #     "s3",
    #     endpoint_url=os.environ[AWS_ENDPOINT],
    #     region_name=os.environ[AWS_REGION],
    #     aws_access_key_id=os.environ.get(AWS_ACCESS_KEY),
    #     aws_secret_access_key=os.environ.get(AWS_SECRET_KEY),
    # )
