import os
import unittest
from os.path import exists
from pathlib import Path

import pytest
from dotenv import load_dotenv

from src.post.aws import AWSClient


class TestAWSConnection(unittest.TestCase):
    def setUp(self) -> None:
        load_dotenv()
        try:
            self.bucket = os.environ["AWS_BUCKET"]
            self.aws_client = AWSClient(
                internal_role=os.environ["AWS_INTERNAL_ROLE"],
                external_role=os.environ["AWS_EXTERNAL_ROLE"],
                external_id=os.environ["AWS_EXTERNAL_ID"],
                bucket=os.environ["AWS_BUCKET"],
            )
        except KeyError:
            pytest.skip("Insufficient ENV envs (AWS_*)")
        self.empty_file = "file.json"
        self.key = f"test/{self.empty_file}"

    def tearDown(self) -> None:
        try:
            os.remove(Path(self.empty_file))
        except FileNotFoundError:
            pass

    def test_assumed_role(self):
        s3_resource = self.aws_client._assume_role()
        self.assertIsNotNone(s3_resource.buckets)

    def create_upload_remove(self):
        Path(self.empty_file).touch()
        self.aws_client.upload_file(
            filename=self.empty_file,
            object_key=f"test/{self.empty_file}",
        )
        os.remove(Path(self.empty_file))

    def test_upload_file(self):
        Path(self.empty_file).touch()
        success = self.aws_client.upload_file(
            filename=self.empty_file,
            object_key=f"test/{self.empty_file}",
        )
        self.assertTrue(success)

    def test_download_file(self):
        # Upload file and remove it from our filesystem
        self.create_upload_remove()

        with self.assertRaises(FileNotFoundError):
            open(self.empty_file)

        success = self.aws_client.download_file(
            filename=self.empty_file,
            object_key=self.key,
        )
        self.assertTrue(success)
        # File was downloaded!
        self.assertTrue(exists(self.empty_file))

    def test_delete_file(self):
        self.create_upload_remove()
        success = self.aws_client.delete_file(object_key=self.key)
        self.assertTrue(success)


if __name__ == "__main__":
    unittest.main()
