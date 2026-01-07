import os
import pytest
from moto import mock_aws
import boto3

import dags.edgar_pipeline as ep
from dags.edgar_pipeline import fetch_to_s3


@pytest.mark.parametrize("ds", ["20240131"])
@mock_aws
def test_fetch_to_s3_writes_to_s3(monkeypatch, ds):
    # 1) mock env + bucket
    os.environ["AWS_DEFAULT_REGION"] = "us-east-1"
    os.environ["EDGAR_S3_BUCKET"] = "test-bucket"
    s3 = boto3.client("s3", region_name="us-east-1")
    s3.create_bucket(Bucket="test-bucket")

    # 2) mock the network call where it's used (in edgar_pipeline module)
    monkeypatch.setattr(ep, "fetch_master_index", lambda _ds: b"FAKE_IDX")

    # 3) run the task function with a fake Airflow context
    fetch_to_s3(ds_nodash=ds)

    # 4) assert file is in S3
    key = f"edgar/raw/master_index/{ds}.idx"
    head = s3.head_object(Bucket="test-bucket", Key=key)
    assert head["ResponseMetadata"]["HTTPStatusCode"] == 200