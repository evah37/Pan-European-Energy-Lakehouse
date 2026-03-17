with stats as (
    select * from {{ ref('stg_spark__price_stats') }}
),

countries as (
    select * from {{ ref('dim_country') }}
)

select
    -- composite key
    s.country_code,
    s.month_of_year,
    s.day_of_week,
    s.hour_of_day,
    
    -- dimension attributes
    c.country_name,
    c.eu_region,
    
    -- statistical baselines
    s.avg_price_baseline,
    s.stddev_price_baseline,
    s.sample_size,
    
    -- anomaly thresholds (mean + 3 * stddev)
    (s.avg_price_baseline + 3 * s.stddev_price_baseline) as upper_bound_price,
    (s.avg_price_baseline - 3 * s.stddev_price_baseline) as lower_bound_price

from stats s
left join countries c on s.country_code = c.country_code