import os
import duckdb
import boto3
from moto import mock_aws
from dags.edgar_pipeline import load_to_duckdb


@mock_aws
def test_load_to_duckdb_inserts_rows(tmp_path, monkeypatch):
    os.environ["AWS_DEFAULT_REGION"] = "us-east-1"
    os.environ["EDGAR_S3_BUCKET"] = "test-bucket"
    duckdb_path = tmp_path / "edgar.duckdb"
    os.environ["DUCKDB_PATH"] = str(duckdb_path)

    s3 = boto3.client("s3", region_name="us-east-1")
    s3.create_bucket(Bucket="test-bucket")

    ds = "20240131"
    key = f"edgar/raw/master_index/{ds}.idx"
    # Minimal idx sample with header and two rows
    content = """CIK|Company Name|Form Type|Date Filed|Filename
0000320193|Apple Inc.|10-K|2024-01-31|edgar/data/0000320193/0000320193-24-000010.txt
0000789019|Microsoft Corp|10-Q|2024-01-31|edgar/data/0000789019/0000789019-24-000011.txt
"""
    s3.put_object(Bucket="test-bucket", Key=key, Body=content.encode("utf-8"))

    load_to_duckdb(ds_nodash=ds)

    con = duckdb.connect(str(duckdb_path))
    count = con.execute("select count(*) from raw.edgar_master").fetchone()[0]
    forms = con.execute("select form_type, count(*) from raw.edgar_master group by 1 order by 2 desc").fetchall()
    con.close()

    assert count == 2
    assert ("10-K", 1) in forms and ("10-Q", 1) in forms


