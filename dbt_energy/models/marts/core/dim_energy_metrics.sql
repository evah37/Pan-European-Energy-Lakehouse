with source as (
    select * from {{ ref('energy_metrics') }}
)

select
    metric_code,
    metric_name,
    category,
    is_renewable,
    unit
from source