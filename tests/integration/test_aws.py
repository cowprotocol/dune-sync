import os
import unittest
from pathlib import Path


from botocore.exceptions import ClientError
from dotenv import load_dotenv

from src.post.aws import get_s3_client, upload_file, get_assumed_role


class TestAWSConnection(unittest.TestCase):
    def setUp(self) -> None:
        load_dotenv()
        # This should be an aws profile name to something like this:
        # Assumes you have the following credentials set in ~/.aws/credentials

        # [dune-sync]
        # custom_aws_user_name=
        # role=
        # aws_secret_access_key=
        # aws_access_key_id=
        self.profile = os.environ.get("AWS_ROLE", "dune-sync")

        # Must be provided!
        self.bucket = os.environ["AWS_BUCKET"]
        self.role = os.environ["AWS_ROLE"]

    def test_get_client(self):
        s3_client = get_s3_client(self.profile)
        # TODO - assert that client is legit!
        self.assertIsNotNone(s3_client)
        self.assertTrue(False)

    def test_upload_file(self):
        s3_resource = get_assumed_role(
            role_arn=os.environ["AWS_ROLE"],
        )

        empty_file = "file.json"
        Path(empty_file).touch()
        try:
            success = upload_file(
                client=s3_resource.meta.client,
                filename=empty_file,
                bucket=os.environ["AWS_BUCKET"],
                object_key=f"test/{empty_file}",
            )
        except Exception as err:
            os.remove(Path(empty_file))
            raise Exception(err)

    def test_assumed_role(self):
        load_dotenv()
        s3_resource = get_assumed_role(
            role_arn=os.environ["AWS_ROLE"],
        )
        # TODO - figure out how to assert this is a legit connection:
        # # Use the Amazon S3 resource object that is now configured with the
        # # credentials to access your S3 buckets.
        # ServiceResource has no attribute buckets.
        self.assertIsNotNone(s3_resource.buckets)

        # THIS IS NOT GOOD.
        with self.assertRaises(ClientError):
            for bucket in s3_resource.buckets.all():
                print(bucket)


if __name__ == "__main__":
    unittest.main()
