import os
import duckdb
import dags.edgar_pipeline as ep
from dags.edgar_pipeline import load_to_duckdb


def test_duckdb_path_parent_created(tmp_path, monkeypatch):
    # Simulate a nested path under /tmp rather than /data to avoid needing a real mount
    db_dir = tmp_path / "nested" / "dir"
    db_path = db_dir / "edgar.duckdb"
    monkeypatch.setenv("DUCKDB_PATH", str(db_path))

    # set S3 to a fake bucket
    monkeypatch.setenv("EDGAR_S3_BUCKET", "test-bucket")

    class FakeBody:
        def __init__(self, data):
            self._data = data

        def read(self):
            """
            Return the stored byte content.
            
            Returns:
                bytes: The raw bytes previously provided to the instance.
            """
            return self._data

    class FakeS3:
        def get_object(self, Bucket=None, Key=None):
            """
            Return a fake S3 GetObject response containing a small EDGAR CSV payload.
            
            The returned mapping uses the same shape as boto3's get_object result: the "Body"
            value is an object whose read() method yields a bytes CSV with a header and one
            data row (CIK, Company Name, Form Type, Date Filed, Filename).
            
            Returns:
                dict: A dictionary with key "Body" whose value is a FakeBody wrapping the CSV bytes.
            """
            sample = (
                b"CIK|Company Name|Form Type|Date Filed|Filename\n"
                b"1|A|10-K|2024-01-31|f1\n"
            )
            return {"Body": FakeBody(sample)}

    # Patch s3_client where it's used (in edgar_pipeline module)
    monkeypatch.setattr(ep, "s3_client", lambda: FakeS3())

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




