import os
from moto import mock_aws
import boto3
from dags.utils.s3_io import put_bytes, object_exists

@mock_aws
def test_put_and_exists():
    os.environ["AWS_DEFAULT_REGION"] = "us-east-1"
    s3 = boto3.client("s3", region_name="us-east-1")
    s3.create_bucket(Bucket="test-bucket")
    put_bytes("test-bucket", "x/y/z.txt", b"hi")
    assert object_exists("test-bucket", "x/y/z.txt")
