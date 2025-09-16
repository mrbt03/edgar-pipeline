import os
from airflow import DAG
from airflow.utils import timezone
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.standard.operators.bash import BashOperator
from dags.utils.s3_io import put_bytes
from dags.utils.edgar_fetch import fetch_master_index
def fetch_to_s3(**ctx):
    ds = ctx["ds_nodash"]  # YYYYMMDD
    bucket = os.getenv("RAW_BUCKET")
    if not bucket:
        raise ValueError("RAW_BUCKET env var is required")
    key = f"edgar/raw/master_index/{ds}.idx"
    data = fetch_master_index(ds)
    put_bytes(bucket, key, data)

def load_to_redshift(**ctx):
    pass

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
        task_id="load_raw_to_redshift",
        python_callable=load_to_redshift,
    )

    dbt_run = BashOperator(
        task_id="run_dbt_models",
        bash_command="cd /usr/local/airflow/dags/dbt/edgar && dbt deps && dbt run",
    )

    ge_validate = BashOperator(
        task_id="run_ge_validation",
        bash_command="great_expectations checkpoint run edgar_staging_checkpoint",
    )

    fetch_filings >> load_raw >> dbt_run >> ge_validate
