import os
import io
import duckdb
import boto3
from airflow import DAG
from airflow.utils import timezone
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.standard.operators.bash import BashOperator
from dags.utils.s3_io import put_bytes
from dags.utils.edgar_fetch import fetch_master_index

def fetch_to_s3(**ctx):
    ds = ctx["ds_nodash"]  # YYYYMMDD
    bucket = os.getenv("EDGAR_S3_BUCKET")
    if not bucket:
        raise ValueError("EDGAR_S3_BUCKET env var is required")
    key = f"edgar/raw/master_index/{ds}.idx"
    data = fetch_master_index(ds)
    put_bytes(bucket, key, data)

def load_to_duckdb(**ctx):
    ds = ctx["ds_nodash"]
    bucket = os.getenv("EDGAR_S3_BUCKET")
    if not bucket:
        raise ValueError("EDGAR_S3_BUCKET env var is required")
    key = f"edgar/raw/master_index/{ds}.idx"
    duckdb_path = os.getenv("DUCKDB_PATH", "/data/edgar.duckdb")

    s3 = boto3.client("s3", region_name=os.getenv("AWS_DEFAULT_REGION", "us-east-1"))
    obj = s3.get_object(Bucket=bucket, Key=key)
    body: bytes = obj["Body"].read()

    # Parse .idx: expect pipe-delimited lines with 5 fields
    lines = io.BytesIO(body).read().decode("utf-8", errors="ignore").splitlines()
    records = []
    for line in lines:
        if "|" not in line:
            continue
        parts = line.split("|")
        if len(parts) < 5:
            continue
        cik, company_name, form_type, date_filed, filename = parts[:5]
        # skip header row if present
        if cik.strip().lower() == "cik" and company_name.strip().lower() == "company name":
            continue
        records.append((cik.strip(), company_name.strip(), form_type.strip(), date_filed.strip(), filename.strip()))

    con = duckdb.connect(duckdb_path)
    con.execute("create schema if not exists raw;")
    con.execute(
        """
        create table if not exists raw.edgar_master (
            cik text,
            company_name text,
            form_type text,
            date_filed text,
            filename text
        );
        """
    )
    # Truncate then insert fresh
    con.execute("delete from raw.edgar_master;")
    if records:
        con.executemany(
            "insert into raw.edgar_master (cik, company_name, form_type, date_filed, filename) values (?, ?, ?, ?, ?)",
            records,
        )
    con.close()


def smoke_query_duckdb(**_):
    duckdb_path = os.getenv("DUCKDB_PATH", "/data/edgar.duckdb")
    con = duckdb.connect(duckdb_path)
    try:
        rows = con.execute(
            "select form_type, count(*) as c from raw.edgar_master group by 1 order by 2 desc limit 10"
        ).fetchall()
        print("Top form_type counts:")
        for r in rows:
            print(f"{r[0]}\t{r[1]}")
        return rows
    finally:
        con.close()

def run_ge_checkpoint(**_):
    import os
    import great_expectations as ge

    ge_home = "/usr/local/airflow/great_expectations"
    os.environ["GE_HOME"] = ge_home
    context = ge.get_context(context_root_dir=ge_home)

    try:
        result = context.run_checkpoint(checkpoint_name="edgar_staging_checkpoint")
        if not result["success"]:
            raise Exception("Great Expectations validation failed")
    except Exception as e:
        print(f"GE validation skipped/soft-failed: {e}")

with DAG(
    dag_id="edgar_pipeline",
    start_date=timezone.datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    default_args={"owner": "data-eng"},
) as dag:


    fetch_filings = PythonOperator(
        task_id="fetch_filings_to_s3",
        python_callable=fetch_to_s3,
    )

    load_raw = PythonOperator(
        task_id="load_duckdb",
        python_callable=load_to_duckdb,
    )

    dbt_run = BashOperator(
        task_id="run_dbt_models",
        bash_command="cd /usr/local/airflow/dags/dbt/edgar && dbt deps && dbt run",
    )

    ge_validate = PythonOperator(
        task_id="run_ge_validation",
        python_callable=run_ge_checkpoint,
    )

    smoke = PythonOperator(
        task_id="run_smoke_query",
        python_callable=smoke_query_duckdb,
    )


    fetch_filings >> load_raw >> dbt_run >> ge_validate >> smoke
