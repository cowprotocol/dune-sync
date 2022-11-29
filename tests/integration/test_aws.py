import os
import unittest
from pathlib import Path

from dotenv import load_dotenv

from src.post.aws import AWSClient


class TestAWSConnection(unittest.TestCase):
    def setUp(self) -> None:
        load_dotenv()
        self.bucket = os.environ["AWS_BUCKET"]
        self.aws_client = AWSClient(
            internal_role=os.environ["AWS_INTERNAL_ROLE"],
            external_role=os.environ["AWS_EXTERNAL_ROLE"],
            external_id=os.environ["AWS_EXTERNAL_ID"],
        )
        self.empty_file = "file.json"
        self.key = f"test/{self.empty_file}"
        Path(self.empty_file).touch()

    def tearDown(self) -> None:
        os.remove(Path(self.empty_file))

    def test_assumed_role(self):
        s3_resource = self.aws_client._assume_role()
        self.assertIsNotNone(s3_resource.buckets)

    def test_upload_file(self):
        success = self.aws_client.upload_file(
            filename=self.empty_file,
            bucket=self.bucket,
            object_key=f"test/{self.empty_file}",
        )
        self.assertTrue(success)

    def test_download_file(self):
        success = self.aws_client.download_file(
            filename=self.empty_file, bucket=self.bucket, object_key=self.key
        )
        self.assertTrue(success)

    def test_delete_file(self):
        success = self.aws_client.delete_file(bucket=self.bucket, object_key=self.key)
        self.assertTrue(success)


if __name__ == "__main__":
    unittest.main()
