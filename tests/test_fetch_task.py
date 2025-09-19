import os
import pytest
from moto import mock_aws
import boto3

# import the function under test from your DAG file
from dags.edgar_pipeline import fetch_to_s3
from dags.utils import edgar_fetch

@pytest.mark.parametrize("ds", ["20240131"])
@mock_aws
def test_fetch_to_s3_writes_to_s3(monkeypatch, ds):
    # 1) mock env + bucket
    os.environ["AWS_DEFAULT_REGION"] = "us-east-1"
    os.environ["EDGAR_S3_BUCKET"] = "test-bucket"
    s3 = boto3.client("s3", region_name="us-east-1")
    s3.create_bucket(Bucket="test-bucket")

    # 2) mock the network call so we don’t hit SEC
    from dags.utils import edgar_fetch
    monkeypatch.setattr(edgar_fetch, "fetch_master_index", lambda _ds: b"FAKE_IDX")

    # 3) run the task function with a fake Airflow context
    fetch_to_s3(ds_nodash=ds)

    # 4) assert file is in S3
    key = f"edgar/raw/master_index/{ds}.idx"
    head = s3.head_object(Bucket="test-bucket", Key=key)
    assert head["ResponseMetadata"]["HTTPStatusCode"] == 200


def test_fetch_master_index_retries(monkeypatch):
    calls = {"n": 0}

    def fake_get(url, headers=None, timeout=30):
        class R:
            def __init__(self, ok):
                self._ok = ok
                self.status_code = 200 if ok else 500
                self.content = b"OK" if ok else b""

            def raise_for_status(self):
                if not self._ok:
                    raise requests.HTTPError("500")

        calls["n"] += 1
        # Fail first two attempts, succeed on third
        return R(ok=calls["n"] >= 3)

    monkeypatch.setattr(edgar_fetch.requests, "get", fake_get)
    out = edgar_fetch.fetch_master_index("20240131")
    assert out == b"OK"
    assert calls["n"] == 3
