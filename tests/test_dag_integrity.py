from airflow.models import DagBag

# test that the DAG is loaded and has the correct tasks
def test_dag_loaded():
    dag_bag = DagBag()
    assert "edgar_pipeline" in dag_bag.dags
    dag = dag_bag.dags["edgar_pipeline"]
    assert sorted([t.task_id for t in dag.tasks]) == sorted([
        "fetch_filings_to_s3",
        "load_duckdb",
        "verify_post_load",
        "run_dbt_models",
        "run_dbt_tests",
        "run_smoke_query",
    ])
