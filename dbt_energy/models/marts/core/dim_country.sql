with source as (
    select * from {{ ref('country_iso_map') }}
)

select
    code as country_code,
    country_name,
    eu_region
from source