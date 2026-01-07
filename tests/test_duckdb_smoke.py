import os
import duckdb
from dags.edgar_pipeline import smoke_query_duckdb


def test_smoke_query_returns_counts(tmp_path, monkeypatch):
    duckdb_path = tmp_path / "edgar.duckdb"
    os.environ["DUCKDB_PATH"] = str(duckdb_path)
    con = duckdb.connect(str(duckdb_path))
    con.execute("create schema if not exists raw;")
    con.execute(
        "create table raw.edgar_master (cik text, company_name text, form_type text, date_filed text, filename text, loaded_at timestamp, index_file_date text)"
    )
    con.execute(
        "insert into raw.edgar_master values ('1','A','10-K','2024-01-31','f1', '2024-01-31 12:00:00', '20240131'), ('2','B','10-Q','2024-01-31','f2', '2024-01-31 12:00:00', '20240131'), ('3','C','10-K','2024-01-31','f3', '2024-01-31 12:00:00', '20240131')"
    )
    con.close()

    rows = smoke_query_duckdb()
    # Expect 10-K count 2 and 10-Q count 1 present
    forms = dict(rows)
    assert forms.get("10-K") == 2
    assert forms.get("10-Q") == 1


