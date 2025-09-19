import os
import duckdb
from dags.edgar_pipeline import run_ge_checkpoint


def test_ge_validation_against_duckdb(tmp_path):
    db = tmp_path / "edgar.duckdb"
    os.environ["DUCKDB_PATH"] = str(db)
    con = duckdb.connect(str(db))
    con.execute("create schema if not exists raw;")
    con.execute(
        "create table raw.edgar_master (cik text, company_name text, form_type text, date_filed text, filename text)"
    )
    con.execute(
        "insert into raw.edgar_master values ('1','A','10-K','2024-01-31','f1'), ('2','B','10-Q','2024-01-31','f2')"
    )
    con.close()

    # Should not raise; soft-fail behavior prints but does not throw
    run_ge_checkpoint()
