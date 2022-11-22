import os
import unittest
from pathlib import Path

from dotenv import load_dotenv

from src.post.aws import get_s3_client, upload_file


class TestIPFS(unittest.TestCase):
    def test_get_client(self):
        # This should be an aws profile name to something like this:
        # # In ~/.aws/config
        # [profile my-sso-profile]
        # sso_start_url = https://my-sso-portal.awsapps.com/start
        # sso_region = us-east-1
        # sso_account_id = 123456789011
        # sso_role_name = readOnly
        profile = os.environ.get("AWS_ROLE", "my-sso-profile")
        bucket = os.environ.get("AWS_BUCKET", "dummy-bucket")

        s3_client = get_s3_client(profile)

        empty_file = "file.json"
        Path(empty_file).touch()
        success = upload_file(
            s3_client,
            file_name=empty_file,
            bucket=bucket,
            object_key=f"test/{empty_file}",
        )

        self.assertTrue(success)


if __name__ == "__main__":
    unittest.main()
