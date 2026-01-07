import os
import duckdb
from dags.edgar_pipeline import verify_duckdb_post_load


def test_verify_post_load_passes(tmp_path):
    db = tmp_path / "edgar.duckdb"
    os.environ["DUCKDB_PATH"] = str(db)
    con = duckdb.connect(str(db))
    con.execute("create schema if not exists raw;")
    con.execute(
        "create table raw.edgar_master (cik text, company_name text, form_type text, date_filed text, filename text, loaded_at timestamp, index_file_date text)"
    )
    con.execute(
        "insert into raw.edgar_master values ('1','A','10-K','2024-01-31','f1','2024-01-31 12:00:00','20240131'), ('2','B','10-Q','2024-01-31','f2','2024-01-31 12:00:00','20240131')"
    )
    con.close()

    # Should not raise
    verify_duckdb_post_load()


def test_verify_post_load_fails_on_empty(tmp_path):
    db = tmp_path / "edgar.duckdb"
    os.environ["DUCKDB_PATH"] = str(db)
    con = duckdb.connect(str(db))
    con.execute("create schema if not exists raw;")
    con.execute(
        "create table raw.edgar_master (cik text, company_name text, form_type text, date_filed text, filename text, loaded_at timestamp, index_file_date text)"
    )
    con.close()

    try:
        verify_duckdb_post_load()
        assert False, "Expected AssertionError"
    except AssertionError:
        pass