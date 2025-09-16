from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator

def fetch_to_s3(**ctx):
    pass

def load_to_redshift(**ctx):
    pass

with DAG(
    dag_id="edgar_pipeline",
    start_date=days_ago(1),
    schedule_interval=None,
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
