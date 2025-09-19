from airflow.models import DagBag

def test_dag_loaded():
    dag_bag = DagBag()
    assert "edgar_pipeline" in dag_bag.dags
    dag = dag_bag.dags["edgar_pipeline"]
    assert set(t.task_id for t in dag.tasks) == {
        "fetch_filings_to_s3",
        "load_duckdb",
        "verify_post_load",
        "run_dbt_models",
        "run_ge_validation",
        "run_smoke_query",
    }
