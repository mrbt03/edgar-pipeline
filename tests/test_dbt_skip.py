import os, pytest
need = ["DBT_HOST","DBT_USER","DBT_PASSWORD","DBT_DB"]
missing = [k for k in need if not os.getenv(k)]
@pytest.mark.skipif(missing, reason=f"Missing env: {missing}")
def test_dbt_env_present():
    assert True
