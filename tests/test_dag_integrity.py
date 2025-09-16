from airflow.models import DagBag

def test_dag_loaded():
    dag_bag = DagBag()
    assert "edgar_pipeline" in dag_bag.dags
    dag = dag_bag.dags["edgar_pipeline"]
    assert [t.task_id for t in dag.tasks] == [
        "fetch_filings_to_s3",
        "load_raw_to_redshift",
        "run_dbt_models",
        "run_ge_validation",
    ]
