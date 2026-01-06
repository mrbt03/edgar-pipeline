# dags/edgar_pipeline.py
"""
EDGAR PIPELINE OVERVIEW

This Airflow DAG downloads the SEC EDGAR master index for a given date,
stores it in S3, loads it into DuckDB, validates the data, and finally runs
a smoke query to confirm the pipeline completed successfully.

TASK FLOW:
1. fetch_filings_to_s3
   - Downloads the SEC master index for the Airflow run date.
   - Uploads the raw .idx file to the configured S3 bucket.

2. load_duckdb
   - Reads the .idx file from S3.
   - Parses each line into structured fields.
   - Loads the records into the DuckDB table raw.edgar_master.
   - Ensures idempotency by deleting old rows for that date before inserting.

3. verify_post_load
   - Performs basic data quality checks on raw.edgar_master.
   - Ensures the table is not empty and key fields (cik, form_type) are non-null.

4. run_dbt_models
   - Executes dbt models for downstream transformations using the DuckDB profile.

5. run_dbt_tests
   - Runs dbt tests to validate data quality (not_null on cik, form_type, date_filed, filename).
   - Fails the DAG if any test fails.

6. run_smoke_query
   - Executes a simple “smoke test” SQL query.
   - Prints the top form types by count to verify the pipeline output.
   - Acts as a final confirmation that the DuckDB data is readable and correct.

NOTES:
- S3 upload and download are handled through small utility functions.
- DuckDB is used as the local analytics engine for storing the parsed EDGAR data.
- Environment variables control S3 bucket name, AWS region, and DuckDB file path.
- The DAG is unscheduled (schedule=None) and runs only when triggered manually.

This pipeline demonstrates a complete mini ETL:
SEC → S3 → DuckDB → Data Quality Checks → dbt → dbt tests → Smoke Test
"""
# import python os module, io module, duckdb module, boto3 module, DAG module, timezone module, PythonOperator module, BashOperator module, put_bytes module, and fetch_master_index module
# import the DAG class from the airflow module
# import the timezone class from the airflow module
# import the PythonOperator class from the airflow module
# import the BashOperator class from the airflow module
# import the put_bytes function from the s3_io module
# import the fetch_master_index function from the edgar_fetch module

import os
import io
import duckdb
from datetime import datetime
from airflow import DAG
from airflow.utils import timezone, datetime
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.standard.operators.bash import BashOperator
from dags.utils.s3_io import put_bytes, s3_client
from dags.utils.edgar_fetch import fetch_master_index

# fetch the master index from the SEC EDGAR website and load it into an S3 bucket packing in all the named parameters with ** into dictionary ctx
def fetch_to_s3(**ctx):
    # get the date from the context in YYYYMMDD format
    ds = ctx["ds_nodash"]
    # get the S3 bucket name from the environment variable or raise an error if not set
    bucket = os.getenv("EDGAR_S3_BUCKET")
    if not bucket:
        raise ValueError("EDGAR_S3_BUCKET env var is required")

    # create the key for the master index file
    key = f"edgar/raw/master_index/{ds}.idx"
    # fetch the master index from the SEC EDGAR website using the fetch_master_index helper function
    data = fetch_master_index(ds)
    # put the master index into the S3 bucket with the put_bytes helper function
    put_bytes(bucket, key, data)

# load the master index from the S3 bucket into a DuckDB database packing in all the named parameters with ** into dictionary ctx
def load_to_duckdb(**ctx):
    # get the date from the context in YYYYMMDD format
    ds_nodash = ctx["ds_nodash"]
    # get the S3 bucket name from the environment variable or raise an error if not set
    bucket = os.getenv("EDGAR_S3_BUCKET")
    if not bucket:
        raise ValueError("EDGAR_S3_BUCKET env var is required")
    # create the key for the master index file
    key = f"edgar/raw/master_index/{ds_nodash}.idx"
    # get the duckdb path from the environmental variable or use the default path
    duckdb_path = os.getenv("DUCKDB_PATH", "/data/edgar.duckdb")

    # get s3 client with the s3_client helper function
    s3 = s3_client()
    # get the object from the S3 bucket with the get_object method using bucket and created key
    obj = s3.get_object(Bucket=bucket, Key=key)
    # read the body of the object into a bytes object
    body: bytes = obj["Body"].read()

    # Get the lines from the body of the object, decdode the bytes to string, ignore errors
    # from potentially invalid characters in messy data, and split the lines into a list
    # of lines every new line character
    lines = io.BytesIO(body).read().decode("utf-8", errors="ignore").splitlines()

    # intialize empty records list of data to be inserted into the DuckDB database
    records = []

    # iterate over the lines in the list
    for line in lines:
        # skip lines that do not contain a pipe character (valid data looks like
        # "0000320193|Apple Inc.|10-K|2024-01-31|edgar/data/0000320193/0000320193-24-000010.txt")
        if "|" not in line:
            continue
        # split parts of the line into a list of parts every pipe character
        parts = line.split("|")
        # skip lines that do not have at least 5 parts
        # (cik, company_name, form_type, date_filed, filename)
        if len(parts) < 5:
            continue
        # unpack the first 5 parts of the list into cik, company_name, form_type, date_filed, filename
        cik, company_name, form_type, date_filed, filename = parts[:5]
        # skip header row if present
        # would look like: CIK|Company Name|Form Type|Date Filed|Filename
        if cik.strip().lower() == "cik" and company_name.strip().lower() == "company name":
            continue
        # append the data to the records list as a tuple
        # loaded_at should be current timestamp when data is loaded, not the execution date
        loaded_at = datetime.now(timezone.utc)
        records.append((cik.strip(), company_name.strip(), form_type.strip(), date_filed.strip(), filename.strip(), loaded_at, ds_nodash))

    # get parent directory of the duckdb path or use the current directory if not set
    parent_dir = os.path.dirname(duckdb_path) or "."
    # create the parent directory if it does not exist
    os.makedirs(parent_dir, exist_ok=True)

    # connect to the DuckDB database using the duckdb path
    con = duckdb.connect(duckdb_path)

    # sql command to create the raw schema if it does not yet exist
    con.execute("create schema if not exists raw;")

    # sql command to create the edgar_master table if it does not yet exist
    # the table will have 5 columns: cik, company_name, form_type, date_filed, filename and
    #  index_file_date tracks which SEC daily index file the row came from (for idempotent deletes)

    con.execute(
        """
        create table if not exists raw.edgar_master (
            cik text,
            company_name text,
            form_type text,
            date_filed text,
            filename text,
            loaded_at timestamp,
            index_file_date text
        );
        """
    )

    # delete all the rows from the edgar_master table where 
    # the index_filed_date matches the execution date to ensure idempotency
    # and avoid duplicate rows for the same data date
    con.execute("delete from raw.edgar_master where index_file_date = ?;", [ds_nodash])
    
    # if there are records to insert, insert them into the edgar_master table
    if records:
        con.executemany(
            "insert into raw.edgar_master (cik, company_name, form_type, date_filed, filename, loaded_at, index_file_date) values (?, ?, ?, ?, ?, ?, ?)",
            records,
        )
    # close the connection to the DuckDB database
    con.close()

# run a smoke test on the DuckDB database to verify schema, types, and data integrity
# follows smoke test practices: verify structure, catch type/column errors, assert on failures
def smoke_query_duckdb(**_):
    # get the duckdb path from the environmental variable or use the default path
    duckdb_path = os.getenv("DUCKDB_PATH", "/data/edgar.duckdb")
    # connect to the DuckDB database using the duckdb path
    con = duckdb.connect(duckdb_path)

    # define expected schema for raw.edgar_master table
    # maps column name to expected DuckDB type
    expected_columns = {
        "cik": "VARCHAR",
        "company_name": "VARCHAR",
        "form_type": "VARCHAR",
        "date_filed": "VARCHAR",
        "filename": "VARCHAR",
        "loaded_at": "TIMESTAMP",
        "index_file_date": "VARCHAR",
    }

    try:
        # step 1: verify table exists and schema matches expected columns
        # this catches "Stupid Mistakes" like missing columns or wrong types
        schema_info = con.execute(
            "select column_name, data_type from information_schema.columns where table_schema = 'raw' and table_name = 'edgar_master'"
        ).fetchall()

        if not schema_info:
            raise AssertionError("Smoke test failed: raw.edgar_master table does not exist")

        # build actual schema dict from query results
        actual_schema = {col[0]: col[1] for col in schema_info}

        # verify all expected columns exist with correct types
        for col_name, expected_type in expected_columns.items():
            if col_name not in actual_schema:
                raise AssertionError(f"Smoke test failed: missing column '{col_name}'")
            if actual_schema[col_name] != expected_type:
                raise AssertionError(
                    f"Smoke test failed: column '{col_name}' has type '{actual_schema[col_name]}', expected '{expected_type}'"
                )

        print("Schema validation passed: all expected columns exist with correct types")

        # step 2: verify downstream transformations can read the data
        # exercises the data by selecting all columns, ensuring they are accessible
        sample = con.execute(
            "select cik, company_name, form_type, date_filed, filename, loaded_at, index_file_date from raw.edgar_master limit 1"
        ).fetchone()

        if sample is None:
            raise AssertionError("Smoke test failed: raw.edgar_master is empty")

        print(f"Sample row retrieved: cik={sample[0]}, form_type={sample[2]}")

        # step 3: verify aggregations work (catches type coercion issues)
        rows = con.execute(
            # select the form_type and count the number of rows for each form_type and order by the count descending and limit the results to 10
            "select form_type, count(*) as c from raw.edgar_master group by form_type order by count(*) desc limit 10"
        ).fetchall()

        if not rows:
            raise AssertionError("Smoke test failed: aggregation returned no results")

        print("Top 10 form types by count:")
        for r in rows:
            print(f"  {r[0]}\t{r[1]}")

        print("Smoke test passed: schema valid, data accessible, aggregations work")
        return rows
    # finally, close the connection to the DuckDB database
    finally:
        con.close()


def verify_duckdb_post_load(**ctx):
    """
    Pre-dbt sanity checks that dbt source tests cannot do:
    1. Connection health - fail fast if DuckDB inaccessible
    2. Row count > 0 - since dbt has no built-in "source not empty" test
    3. Row count sanity - SEC daily index typically has 1000+ filings
    4. Freshness - loaded_at should be from current pipeline run
    
    Note: not_null checks are handled by dbt source tests (sources.yml)
    """
    duckdb_path = os.getenv("DUCKDB_PATH", "/data/edgar.duckdb")
    ds_nodash = ctx.get("ds_nodash")
    
    # 1. Connection health check
    try:
        con = duckdb.connect(duckdb_path)
    except Exception as e:
        raise AssertionError(f"Connection health check failed: {e}") from e
    
    try:
        # 2. Row count > 0 (dbt can't do this for sources)
        cnt = con.execute("select count(*) from raw.edgar_master").fetchone()[0]
        if cnt <= 0:
            raise AssertionError("Row count check failed: raw.edgar_master is empty")
        
        # 3. Row count sanity - SEC publishes 1000+ filings on typical trading days, try with 100
        # Warn if suspiciously low (weekends/holidays may have fewer)
        if cnt < 100:
            print(f"WARNING: Only {cnt} rows loaded - verify this is expected (weekend/holiday?)")
        
        # 4. Freshness check - verify data for this run date exists
        if ds_nodash:
            date_cnt = con.execute(
                "select count(*) from raw.edgar_master where index_file_date = ?",
                [ds_nodash]
            ).fetchone()[0]
            if date_cnt <= 0:
                raise AssertionError(f"Freshness check failed: no rows for index_file_date={ds_nodash}")
            print(f"Freshness check passed: {date_cnt} rows for {ds_nodash}")
        
        # 5. Duplicate detection (informational - dbt staging handles dedup)
        dup_cnt = con.execute(
            "select count(*) - count(distinct filename) from raw.edgar_master"
        ).fetchone()[0]
        if dup_cnt > 0:
            print(f"INFO: {dup_cnt} duplicate filenames detected (will be deduped by dbt staging)")
        
        print(f"Post-load verification passed: {cnt} total rows")
    finally:
        con.close()

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
        bash_command="cd /usr/local/airflow/dags/dbt/edgar && dbt deps && dbt run --profiles-dir .",
    )

    dbt_test = BashOperator(
        task_id="run_dbt_tests",
        bash_command="cd /usr/local/airflow/dags/dbt/edgar && dbt test --profiles-dir .",
    )

    smoke = PythonOperator(
        task_id="run_smoke_query",
        python_callable=smoke_query_duckdb,
    )

    postload_verify = PythonOperator(
        task_id="verify_post_load",
        python_callable=verify_duckdb_post_load,
    )

    fetch_filings >> load_raw >> postload_verify >> dbt_run >> dbt_test >> smoke