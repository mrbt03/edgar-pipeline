import os, subprocess, shutil
from pathlib import Path


def test_dbt_run_staging(tmp_path):
    # Copy the dbt project to a temp dir to avoid modifying workspace
    project_src = Path('/usr/local/airflow/dags/dbt/edgar')
    project_dst = tmp_path / 'edgar'
    shutil.copytree(project_src, project_dst)

    # Point DUCKDB_PATH to a temp DB and create source table
    duckdb_path = tmp_path / 'edgar.duckdb'
    os.environ['DUCKDB_PATH'] = str(duckdb_path)

    import duckdb
    con = duckdb.connect(str(duckdb_path))
    con.execute('create schema if not exists raw;')
    con.execute('create table raw.edgar_master (cik text, company_name text, form_type text, date_filed text, filename text);')
    con.execute("insert into raw.edgar_master values ('1','A','10-K','2024-01-31','f1')")
    con.close()

    # Run dbt deps + run using local profiles.yml
    env = os.environ.copy()
    cmd = ['bash', '-lc', 'cd "'+str(project_dst)+'" && dbt deps && dbt run --profiles-dir .']
    result = subprocess.run(cmd, capture_output=True, text=True, env=env)
    assert result.returncode == 0, result.stdout + '\n' + result.stderr

    # Verify staging object by selecting from it
    con = duckdb.connect(str(duckdb_path))
    # dbt-duckdb typically namespaces schema with the database (main)
    count = con.execute("select count(*) from main_staging.stg_edgar_master").fetchone()[0]
    con.close()
    assert count == 1
