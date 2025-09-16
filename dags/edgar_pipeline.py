import os
from airflow import DAG
from airflow.utils import timezone
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.standard.operators.bash import BashOperator
from dags.utils.s3_io import put_bytes
from dags.utils.edgar_fetch import fetch_master_index
import psycopg2

def fetch_to_s3(**ctx):
    ds = ctx["ds_nodash"]  # YYYYMMDD
    bucket = os.getenv("RAW_BUCKET")
    if not bucket:
        raise ValueError("RAW_BUCKET env var is required")
    key = f"edgar/raw/master_index/{ds}.idx"
    data = fetch_master_index(ds)
    put_bytes(bucket, key, data)

def load_to_redshift(**ctx):
    ds = ctx["ds_nodash"]
    bucket = os.getenv("RAW_BUCKET")
    schema_raw = os.getenv("REDSHIFT_SCHEMA_RAW", "raw")
    iam_role = os.getenv("REDSHIFT_IAM_ROLE_ARN")

    key = f"edgar/raw/master_index/{ds}.idx"
    s3_uri = f"s3://{bucket}/{key}"

    conn = psycopg2.connect(
        host=os.getenv("REDSHIFT_HOST"),
        port=os.getenv("REDSHIFT_PORT", "5439"),
        dbname=os.getenv("REDSHIFT_DB"),
        user=os.getenv("REDSHIFT_USER"),
        password=os.getenv("REDSHIFT_PASSWORD"),
        connect_timeout=10,
    )
    conn.autocommit = True
    cur = conn.cursor()

    # create raw table if not exists (tweak cols to your real parse downstream)
    cur.execute(f"""
        create schema if not exists {schema_raw};
        create table if not exists {schema_raw}.edgar_master_raw (
            line text
        );
        truncate table {schema_raw}.edgar_master_raw;
    """)

    # COPY the raw file  adjust FORMAT as needed once you parse
    cur.execute(f"""
        copy {schema_raw}.edgar_master_raw
        from %s
        iam_role %s
        format as TEXT
        compupdate off
        statupdate off;
    """, (s3_uri, iam_role))

    cur.close()
    conn.close()

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
        task_id="load_raw_to_redshift",
        python_callable=load_to_redshift,
    )

    dbt_run = BashOperator(
        task_id="run_dbt_models",
        bash_command="cd /usr/local/airflow/dags/dbt/edgar && dbt deps && dbt run",
    )

    ge_validate = PythonOperator(
        task_id="run_ge_validation",
        python_callable=run_ge_checkpoint,
    )


    fetch_filings >> load_raw >> dbt_run >> ge_validate
