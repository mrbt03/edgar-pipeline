with source as (
  select * from {{ source('raw', 'edgar_master') }}
)

select
  cast(cik as text) as cik,
  company_name,
  form_type,
  date_filed,
  filename
from source

