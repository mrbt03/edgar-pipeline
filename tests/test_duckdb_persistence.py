import os
import duckdb
from dags.edgar_pipeline import load_to_duckdb


def test_duckdb_path_parent_created(tmp_path, monkeypatch):
    # Simulate a nested path under /tmp rather than /data to avoid needing a real mount
    db_dir = tmp_path / "nested" / "dir"
    db_path = db_dir / "edgar.duckdb"
    monkeypatch.setenv("DUCKDB_PATH", str(db_path))

    # Set S3 to a fake bucket and monkeypatch boto3 get_object to return sample idx
    monkeypatch.setenv("EDGAR_S3_BUCKET", "test-bucket")

    class FakeBody:
        def __init__(self, data):
            self._data = data

        def read(self):
            return self._data

    def fake_get_object(Bucket=None, Key=None):
        sample = (
            b"CIK|Company Name|Form Type|Date Filed|Filename\n"
            b"1|A|10-K|2024-01-31|f1\n"
        )
        return {"Body": FakeBody(sample)}

    import dags.edgar_pipeline as ep
    class FakeS3:
        def get_object(self, Bucket=None, Key=None):
            return fake_get_object(Bucket=Bucket, Key=Key)

    # Patch boto3 client used in the module
    import boto3 as real_boto3
    monkeypatch.setattr(ep, "boto3", type("B", (), {"client": lambda *_args, **_kwargs: FakeS3()})())

    # Run load task with a fixed date
    load_to_duckdb(ds_nodash="20240131")

    # Assert parent dir and DB were created and contain data
    assert db_dir.exists()
    con = duckdb.connect(str(db_path))
    try:
        cnt = con.execute("select count(*) from raw.edgar_master").fetchone()[0]
        assert cnt == 1
    finally:
        con.close()


