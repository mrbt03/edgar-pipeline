# tests/test_s3_io.py

# this module tests the s3_io module using the moto library to mock the AWS S3 service.
# it includes:
# - test_s3_client_uses_env_region(): tests that the s3_client uses the environment variable AWS_DEFAULT_REGION
# - test_put_bytes_success(): tests that the put_bytes function successfully uploads a file to the S3 bucket
# - test_put_bytes_raises_runtimeerror_on_missing_bucket(): tests that the put_bytes function raises a RuntimeError if the bucket is missing
# - test_object_exists_true_and_false(): tests that the object_exists function returns True if the object exists and False if it does not

# import pytest for testing
import pytest
# import boto3 for AWS S3 operations
import boto3
# import moto for mocking AWS services
from moto import mock_aws
# import the s3_io module for testing
from dags.utils.s3_io import s3_client, put_bytes, object_exists

# test that the s3_client uses the environment variable AWS_DEFAULT_REGION
@mock_aws
def test_s3_client_uses_env_region(monkeypatch):
    monkeypatch.setenv("AWS_DEFAULT_REGION", "us-east-1")
    c = s3_client()
    assert c.meta.region_name == "us-east-1"

# test that the put_bytes function successfully uploads a file to the S3 bucket
@mock_aws
def test_put_bytes_success(monkeypatch):
    monkeypatch.setenv("AWS_DEFAULT_REGION", "us-east-1")
    s3 = boto3.client("s3", region_name="us-east-1")
    s3.create_bucket(Bucket="test-bucket")

    put_bytes("test-bucket", "folder/file.txt", b"data")
    assert object_exists("test-bucket", "folder/file.txt") is True

@mock_aws
def test_put_bytes_raises_runtimeerror_on_missing_bucket(monkeypatch):
    monkeypatch.setenv("AWS_DEFAULT_REGION", "us-east-1")
    with pytest.raises(RuntimeError):
        put_bytes("missing-bucket", "k", b"x")

@mock_aws
def test_object_exists_true_and_false(monkeypatch):
    monkeypatch.setenv("AWS_DEFAULT_REGION", "us-east-1")
    s3 = boto3.client("s3", region_name="us-east-1")
    s3.create_bucket(Bucket="test-bucket")

    # Missing key -> False
    assert object_exists("test-bucket", "nope.txt") is False

    # Present key -> True
    s3.put_object(Bucket="test-bucket", Key="yes.txt", Body=b"ok")
    assert object_exists("test-bucket", "yes.txt") is True