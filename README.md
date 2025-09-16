# EDGAR Pipeline

An end-to-end data pipeline for parsing and analyzing SEC EDGAR filings.  
This project showcases scalable data engineering practices by ingesting raw filings, transforming them with dbt, validating with Great Expectations, and storing results in AWS Redshift.

## Tech Stack
- Airflow (Astro CLI) – orchestration and scheduling  
- AWS S3 – raw data landing zone  
- AWS Redshift Serverless – cloud data warehouse  
- dbt – SQL-based transformations and data marts  
- Great Expectations – automated data quality validation  
- Docker – containerized local development (optional)  
- Streamlit / Metabase – optional BI visualization  
- GitHub – version control and collaboration  

## Quickstart
1. Clone the repository  
   git clone https://github.com/mrbt03/edgar-pipeline.git  
   cd edgar-pipeline  

2. Configure environment variables  
   - Copy `.env.example` to `.env`  
   - Add AWS credentials and Redshift details  

3. Start Airflow locally  
   astro dev start  

4. Trigger the pipeline  
   - DAG name: `edgar_pipeline`  
   - Tasks: fetch → load → transform → validate  

## Outputs
- Redshift schema with cleaned EDGAR filings  
- dbt-generated marts for analytics  
- Great Expectations validation reports  
- Optional BI dashboard with Streamlit or Metabase  

## Repository Structure
dags/                # Airflow DAGs  
dags/dbt/            # dbt project  
ge/                  # Great Expectations configs  
docs/                # Documentation  
.env.example         # Environment template  
.gitignore  
README.md  

## Next Steps
- Expand dbt models with richer transformations  
- Add additional Great Expectations test suites  
- Deploy a BI dashboard for insights  
- Automate deployment with CI/CD  
