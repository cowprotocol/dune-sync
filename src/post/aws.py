"""Aws S3 Bucket functionality (namely upload_file)"""
import logging

import boto3
from boto3.resources.base import ServiceResource
from boto3.s3.transfer import S3Transfer
from botocore.client import BaseClient


class AWSClient:
    """
    Class managing the roles required to do file operations on our S3 bucket
    """

    def __init__(self, internal_role: str, external_role: str, external_id: str):
        self.internal_role = internal_role
        self.external_role = external_role
        self.external_id = external_id

        self.service_resource = self._assume_role()
        self.s3_client = self._get_s3_client(self.service_resource)

    def _get_s3_client(self, s3_resource: ServiceResource) -> BaseClient:
        """Constructs a client session for S3 Bucket upload."""
        return s3_resource.meta.client

    def _assume_role(self) -> ServiceResource:
        """
        Borrowed from AWS documentation
        https://docs.aws.amazon.com/IAM/latest/UserGuide/id_roles_use_switch-role-api.html
        """
        sts_client = boto3.client("sts")

        internal_assumed_role_object = sts_client.assume_role(
            RoleArn=self.internal_role,
            RoleSessionName="OuterSession",
        )
        credentials = internal_assumed_role_object["Credentials"]
        sts_client = boto3.client(
            "sts",
            aws_access_key_id=credentials["AccessKeyId"],
            aws_secret_access_key=credentials["SecretAccessKey"],
            aws_session_token=credentials["SessionToken"],
        )

        external_assumed_role_object = sts_client.assume_role(
            RoleArn=self.external_role,
            RoleSessionName="InnerSession",
            ExternalId=self.external_id,
        )
        credentials = external_assumed_role_object["Credentials"]

        s3_resource: ServiceResource = boto3.resource(
            "s3",
            aws_access_key_id=credentials["AccessKeyId"],
            aws_secret_access_key=credentials["SecretAccessKey"],
            aws_session_token=credentials["SessionToken"],
        )
        return s3_resource

    def upload_file(self, filename: str, bucket: str, object_key: str) -> bool:
        """Upload a file to an S3 bucket

        :param filename: File to upload. Should be a full path to file.
        :param bucket: Bucket to upload to
        :param object_key: S3 object key. For our purposes, this would
                           be f"{table_name}/cow_{latest_block_number}.json"
        :return: True if file was uploaded, else raises
        """

        S3Transfer(self.s3_client).upload_file(
            filename=filename,
            bucket=bucket,
            key=object_key,
            extra_args={"ACL": "bucket-owner-full-control"},
        )
        logging.info(f"successfully uploaded {filename} to {bucket}")
        return True

    def delete_file(self, bucket: str, object_key: str) -> bool:
        """Delete a file from an S3 bucket

        :param bucket: Bucket to delete from
        :param object_key: S3 object key. For our purposes, this would
                           be f"{table_name}/cow_{latest_block_number}.json"
        :return: True if file was deleted, else raises
        """
        # TODO - types! error: "BaseClient" has no attribute "delete_object"
        self.s3_client.delete_object(  # type: ignore
            Bucket=bucket,
            Key=object_key,
        )
        logging.info(f"successfully deleted {object_key} from {bucket}")
        return True

    def download_file(self, filename: str, bucket: str, object_key: str) -> bool:
        """Download a file from an S3 bucket

        :param filename: File to download. Should be a full path to file.
        :param bucket: Bucket to download from
        :param object_key: S3 object key. For our purposes, this would
                           be f"{table_name}/cow_{latest_block_number}.json"
        :return: True if file was downloaded, else raises
        """
        S3Transfer(self.s3_client).download_file(
            filename=filename,
            bucket=bucket,
            key=object_key,
        )
        logging.info(f"successfully downloaded {filename} from {bucket}")
        return True
