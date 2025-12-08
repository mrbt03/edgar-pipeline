# EDGAR Pipeline

An end-to-end ELT pipeline for SEC EDGAR master index files.
Local-first design: land raw index files in S3, load into DuckDB on disk, run dbt staging, validate with Great Expectations, and run a smoke query.

## Tech Stack
- Airflow (Astro CLI) – orchestration and scheduling  
- AWS S3 – raw data landing zone  
- DuckDB – local columnar store persisted at /data/edgar.duckdb  
- dbt-duckdb – SQL-based transformations  
- Great Expectations – basic data quality checks  
- Docker – containerized local development  

## Quickstart
1. Clone the repository  
   git clone https://github.com/mrbt03/edgar-pipeline.git  
   cd edgar-pipeline  

2. Configure environment variables  
   - Copy `.env.example` to `.env`  
   - Set:  
     - `EDGAR_S3_BUCKET` – your S3 bucket name  
     - `DUCKDB_PATH` – defaults to `/data/edgar.duckdb`  
     - `AWS_DEFAULT_REGION` – e.g., `us-east-1`

3. Start Airflow locally  
   astro dev start  
   Airflow UI: http://localhost:8080

4. Run the DAG  
   - DAG id: `edgar_pipeline`  
   - Tasks: `fetch_filings_to_s3` → `load_duckdb` → `run_dbt_models` → `run_ge_validation` → `run_smoke_query`

5. Smoke query (from container)  
   astro dev bash -s --scheduler  
   python dags/scripts/duckdb_smoke.py

## Outputs
- DuckDB database at `/data/edgar.duckdb` with `raw.edgar_master`  
- dbt staging view `main_staging.stg_edgar_master`  
- GE validation logs in task output  
- Smoke query prints top `form_type` counts  

## Repository Structure
- dags/ – Airflow DAGs and scripts  
- dags/dbt/edgar – dbt project (DuckDB profile in `profiles.yml`)  
- great_expectations/ – optional GE configs (inline checks used in DAG)  
- tests/ – pytest suite for DAG, utils, dbt, GE  
- .env.example – environment template  

## Notes
- Only the following env vars are used: `EDGAR_S3_BUCKET`, `DUCKDB_PATH`, `AWS_DEFAULT_REGION`.  
- The DuckDB file is persisted via `/data` volume (configure a Docker volume when deploying).

## Next Steps
- Persist `/data` using a Docker volume in your runtime  
- Add Makefile shortcuts (dev start, test, smoke)  
- Expand dbt models and GE checks  
