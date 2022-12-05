"""Aws S3 Bucket functionality (namely upload_file)"""
from __future__ import annotations

import os
from collections import defaultdict
from dataclasses import dataclass
from typing import Any, Optional

import boto3
from boto3.resources.base import ServiceResource
from boto3.s3.transfer import S3Transfer
from botocore.client import BaseClient
from dotenv import load_dotenv

from src.logger import set_log
from src.models.tables import SyncTable

log = set_log(__name__)


@dataclass
class BucketFileObject:
    """
    Basic Structure describing a file's location and associated block number in AWS bucket
    Our files are structured as `table_name/cow_XXXXXXX.json`
    where XXXXXX is an increasing sequence of integers.
    More precisely, we use last_sync_block_number to indicate our last collection of records.
    """

    path: str
    name: str
    block: Optional[int]

    @classmethod
    def from_key(cls, object_key: str) -> BucketFileObject:
        """
        Decompose the unique identifier `object_key` into
        more meaningful parts from which it can be reconstructed
        """
        path, name = object_key.split("/")

        try:
            block = int(name.strip("cow_").strip(".json"))
        except ValueError:
            # File structure does not satisfy block indexing!
            block = None
        return cls(
            path,
            name,  # Keep the full reference (for delete)
            block,
        )

    @property
    def object_key(self) -> str:
        """
        Original object key
        used to operate on these elements within the S3 bucket (e.g. delete)
        """
        return "/".join([self.path, self.name])


@dataclass
class BucketStructure:
    """Representation of the bucket directory structure"""

    files: dict[str, list[BucketFileObject]]

    @classmethod
    def from_bucket_collection(cls, bucket_objects: Any) -> BucketStructure:
        """
        Constructor from results of ServiceResource.Buckets
        """
        # Initialize empty lists (incase the directories contain nothing)
        grouped_files = defaultdict(list[BucketFileObject])
        for bucket_obj in bucket_objects:
            object_key = bucket_obj.key
            path, _ = object_key.split("/")
            grouped_files[path].append(BucketFileObject.from_key(object_key))
            if path not in SyncTable.supported_tables():
                # Catches any unrecognized filepath.
                log.warning(f"Found unexpected file {object_key}")

        log.debug(f"loaded bucket filesystem: {grouped_files}")

        return cls(files=grouped_files)

    def get(self, table: SyncTable | str) -> list[BucketFileObject]:
        """
        Returns the list of files under `table`
            - returns empty list if none available.
        """
        table_str = str(table) if isinstance(table, SyncTable) else table
        return self.files.get(table_str, [])


class AWSClient:
    """
    Class managing the roles required to do file operations on our S3 bucket
    """

    def __init__(
        self, internal_role: str, external_role: str, external_id: str, bucket: str
    ):
        self.internal_role = internal_role
        self.external_role = external_role
        self.external_id = external_id
        self.bucket = bucket

        self.service_resource = self._assume_role()
        self.s3_client = self._get_s3_client(self.service_resource)

    @classmethod
    def new_from_environment(cls) -> AWSClient:
        """Constructs an instance of AWSClient from environment variables"""
        load_dotenv()
        return cls(
            internal_role=os.environ["AWS_INTERNAL_ROLE"],
            external_role=os.environ["AWS_EXTERNAL_ROLE"],
            external_id=os.environ["AWS_EXTERNAL_ID"],
            bucket=os.environ["AWS_BUCKET"],
        )

    @staticmethod
    def _get_s3_client(s3_resource: ServiceResource) -> BaseClient:
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
            RoleSessionName="InternalSession",
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
            RoleSessionName="ExternalSession",
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

    def upload_file(self, filename: str, object_key: str) -> bool:
        """Upload a file to an S3 bucket

        :param filename: File to upload. Should be a full path to file.
        :param object_key: S3 object key. For our purposes, this would
                           be f"{table_name}/cow_{latest_block_number}.json"
        :return: True if file was uploaded, else raises
        """

        S3Transfer(self.s3_client).upload_file(
            filename=filename,
            bucket=self.bucket,
            key=object_key,
            extra_args={"ACL": "bucket-owner-full-control"},
        )
        log.debug(f"uploaded {filename} to {self.bucket}")
        return True

    def delete_file(self, object_key: str) -> bool:
        """Delete a file from an S3 bucket

        :param object_key: S3 object key. For our purposes, this would
                           be f"{table_name}/cow_{latest_block_number}.json"
        :return: True if file was deleted, else raises
        """
        # TODO - types! error: "BaseClient" has no attribute "delete_object"
        self.s3_client.delete_object(  # type: ignore
            Bucket=self.bucket,
            Key=object_key,
        )
        log.debug(f"deleted {object_key} from {self.bucket}")
        return True

    def download_file(self, filename: str, object_key: str) -> bool:
        """Download a file from an S3 bucket

        :param filename: File to download. Should be a full path to file.
        :param object_key: S3 object key. For our purposes, this would
                           be f"{table_name}/cow_{latest_block_number}.json"
        :return: True if file was downloaded, else raises
        """
        S3Transfer(self.s3_client).download_file(
            filename=filename,
            bucket=self.bucket,
            key=object_key,
        )
        log.debug(f"downloaded {filename} from {self.bucket}")
        return True

    def existing_files(self) -> BucketStructure:
        """
        Returns an object representing the bucket file
        structure with sync block metadata
        """
        bucket = self.service_resource.Bucket(self.bucket)  # type: ignore

        bucket_objects = bucket.objects.all()
        return BucketStructure.from_bucket_collection(bucket_objects)

    def last_sync_block(self, table: SyncTable | str) -> int:
        """
        Based on the existing bucket files,
        the last sync block is uniquely determined from the file names.
        """
        table_str = str(table) if isinstance(table, SyncTable) else table
        try:
            table_files = self.existing_files().get(table_str)
            return max(file_obj.block for file_obj in table_files if file_obj.block)
        except ValueError as err:
            # Raised when table_files = []
            raise FileNotFoundError(
                f"Could not determine last sync block for {table} files. No files."
            ) from err

    def delete_all(self, table: SyncTable | str) -> None:
        """Deletes all files within the supported tables directory"""
        try:
            table_files = self.existing_files().get(table)
            for file_data in table_files:
                self.delete_file(file_data.object_key)
        except KeyError as err:
            raise ValueError(
                f"Invalid table_name {table}, please chose from {SyncTable.supported_tables()}"
            ) from err
