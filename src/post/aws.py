"""Aws S3 Bucket functionality (namely upload_file)"""
import logging

import boto3
from boto3.resources.base import ServiceResource
from boto3.s3.transfer import S3Transfer
from botocore.client import BaseClient


def get_assumed_role(role_arn: str) -> ServiceResource:
    """
    Borrowed from AWS documentation
    https://docs.aws.amazon.com/IAM/latest/UserGuide/id_roles_use_switch-role-api.html
    """
    # create an STS client object that represents a live connection to the
    # STS service
    sts_client = boto3.client("sts")

    # Call the assume_role method of the STSConnection object and pass the role
    # ARN and a role session name.
    assumed_role_object = sts_client.assume_role(
        RoleArn=role_arn,
        RoleSessionName="AssumeRoleSession1",
    )

    # From the response that contains the assumed role, get the temporary
    # credentials that can be used to make subsequent API calls
    credentials = assumed_role_object["Credentials"]

    # Use the temporary credentials that AssumeRole returns to make a
    # connection to Amazon S3
    # TODO - The types returned from boto3 are pretty weak. Manual casting is not great!
    s3_resource: ServiceResource = boto3.resource(
        "s3",
        aws_access_key_id=credentials["AccessKeyId"],
        aws_secret_access_key=credentials["SecretAccessKey"],
        aws_session_token=credentials["SessionToken"],
    )
    return s3_resource


def upload_file(
    client: BaseClient | S3Transfer, filename: str, bucket: str, object_key: str
) -> bool:
    """Upload a file to an S3 bucket

    :param client: S3Transfer object with `upload_file` method
    :param filename: File to upload. Should be a full path to file.
    :param bucket: Bucket to upload to
    :param object_key: S3 object key. For our purposes, this would
                       be f"{table_name}/cow_{latest_block_number}.json"
    :return: True if file was uploaded, else False
    """
    # Convert BaseClient to S3Transfer (if necessary)
    s3_client = client if isinstance(client, S3Transfer) else S3Transfer(client)

    s3_client.upload_file(
        filename,
        bucket,
        key=object_key,
        extra_args={"ACL": "bucket-owner-full-control"},
    )
    logging.info(f"successfully uploaded {filename} to {bucket}")
    return True


def get_s3_client(profile: str) -> S3Transfer:
    """Constructs a client session for S3 Bucket upload."""
    session = boto3.Session(profile_name=profile)
    return S3Transfer(session.client("s3"))
