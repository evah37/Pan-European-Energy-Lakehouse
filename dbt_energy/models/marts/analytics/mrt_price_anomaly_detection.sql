with hourly_data as (
    select 
        energy_event_id,
        event_time,
        country_code,
        price_eur_mwh,
        
        -- extract connection keys
        month(event_time) as month_of_year,
        dayofweek(event_time) as day_of_week, -- DuckDB: 0=Sunday, 6=Saturday
        hour(event_time) as hour_of_day
    from {{ ref('fct_energy_hourly') }}
    where price_eur_mwh is not null
),

baselines as (
    select 
        country_code,
        month_of_year,
        day_of_week, -- ensure source data align with hourly_data
        hour_of_day,
        avg_price_baseline,
        stddev_price_baseline
    from {{ ref('fct_price_baselines') }}
)

select
    h.energy_event_id,
    h.event_time,
    h.country_code,
    
    -- actual price
    h.price_eur_mwh as actual_price,
    
    -- expected baseline
    b.avg_price_baseline as expected_price,
    b.stddev_price_baseline,
    
    -- Z-Score (standardized anomaly score)
    -- Formula: (actual value - mean) / standard deviation
    -- Use NULLIF to prevent division by zero
    (h.price_eur_mwh - b.avg_price_baseline) / NULLIF(b.stddev_price_baseline, 0) as price_z_score,
    
    -- anomaly status (High/Low)
    case 
        when (h.price_eur_mwh - b.avg_price_baseline) > (3 * b.stddev_price_baseline) then 'High Anomaly'
        when (b.avg_price_baseline - h.price_eur_mwh) > (3 * b.stddev_price_baseline) then 'Low Anomaly'
        else 'Normal'
    end as anomaly_status

from hourly_data h
inner join baselines b 
    on h.country_code = b.country_code
    and h.month_of_year = b.month_of_year
    and h.day_of_week = b.day_of_week
    and h.hour_of_day = b.hour_of_day