import os
import duckdb
import dags.edgar_pipeline as ep
from dags.edgar_pipeline import load_to_duckdb


def test_load_idempotent(tmp_path, monkeypatch):
    """
    Verify that loading EDGAR data into DuckDB is idempotent for the same date.
    
    Creates a temporary DuckDB database, mocks S3 to return a small EDGAR CSV payload,
    calls load_to_duckdb twice with the same `ds_nodash` value, and asserts that the
    raw.edgar_master table contains exactly two rows (no duplicate insertion).
    Parameters:
        tmp_path (pathlib.Path): Pytest temporary path fixture used to create the DuckDB file.
        monkeypatch (pytest.MonkeyPatch): Pytest fixture used to set environment variables and patch the S3 client.
    """
    db = tmp_path / "edgar.duckdb"
    monkeypatch.setenv("DUCKDB_PATH", str(db))
    monkeypatch.setenv("EDGAR_S3_BUCKET", "test-bucket")

    class FakeBody:
        def __init__(self, data):
            """
            Initialize the FakeBody wrapper with response data.
            
            Parameters:
                data (bytes): Raw bytes that will be returned by the object's read() method.
            """
            self._data = data
        def read(self):
            """
            Return the stored response body bytes.
            
            Returns:
                bytes: The raw bytes previously provided to this FakeBody instance.
            """
            return self._data

    class FakeS3:
        def get_object(self, Bucket=None, Key=None):
            """
            Retrieve a fake S3 object containing a small pipe-delimited EDGAR sample.
            
            Bucket and Key parameters are accepted for interface compatibility but ignored.
            
            Returns:
                dict: A mapping with key `"Body"` whose value is a `FakeBody` wrapping a bytes payload.
                      The payload is a pipe-delimited text with a header and two rows:
                      "CIK|Company Name|Form Type|Date Filed|Filename",
                      "1|A|10-K|2024-01-31|f1",
                      "2|B|10-Q|2024-01-31|f2".
            """
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




