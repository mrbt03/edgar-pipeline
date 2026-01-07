import os
import duckdb
import dags.edgar_pipeline as ep
from dags.edgar_pipeline import load_to_duckdb


def test_load_idempotent(tmp_path, monkeypatch):
    db = tmp_path / "edgar.duckdb"
    monkeypatch.setenv("DUCKDB_PATH", str(db))
    monkeypatch.setenv("EDGAR_S3_BUCKET", "test-bucket")

    class FakeBody:
        def __init__(self, data):
            self._data = data
        def read(self):
            return self._data

    class FakeS3:
        def get_object(self, Bucket=None, Key=None):
            sample = (
                b"CIK|Company Name|Form Type|Date Filed|Filename\n"
                b"1|A|10-K|2024-01-31|f1\n"
                b"2|B|10-Q|2024-01-31|f2\n"
            )
            return {"Body": FakeBody(sample)}

    # Patch s3_client where it's used (in edgar_pipeline module)
    monkeypatch.setattr(ep, "s3_client", lambda: FakeS3())

    # First load
    load_to_duckdb(ds_nodash="20240131")
    # Second load same date should not duplicate since rows are deleted before insert
    load_to_duckdb(ds_nodash="20240131")

    con = duckdb.connect(str(db), read_only=True)
    try:
        cnt = con.execute("select count(*) from raw.edgar_master").fetchone()[0]
        assert cnt == 2
    finally:
        con.close()





