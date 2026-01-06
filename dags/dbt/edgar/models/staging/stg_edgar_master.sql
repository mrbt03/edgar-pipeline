-- Staging model: cleans and deduplicates raw SEC EDGAR data
-- Raw layer preserves source data as-is for auditing
-- Staging layer applies business logic (deduplication, type casting)

with source as (
  select * from {{ source('raw', 'edgar_master') }}
),

-- deduplicate by unique filing identifier (filename is unique per filing)
-- keep the most recent load if duplicates exist
deduplicated as (
  select
    cik, company_name, form_type, date_filed, filename, loaded_at, index_file_date,
    row_number() over (partition by filename order by loaded_at desc) as row_num
  from source
)

select
  cast(cik as text) as cik, company_name, form_type, date_filed, filename, loaded_at, index_file_date
from deduplicated
where row_num = 1