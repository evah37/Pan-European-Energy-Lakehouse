with hourly_facts as (
    select 
        country_name,
        year,
        month,
        renewable_penetration_rate,
        price_eur_mwh,
        volatility_24h,
        residual_load_mw,
        is_extreme_ramp
    from {{ ref('fct_energy_hourly') }}
),

buckets as (
    select
        country_name,
        year,
        month,
        
        -- bucket by renewable penetration rate
        case 
            when renewable_penetration_rate < 0.2 then '0-20%'
            when renewable_penetration_rate >= 0.2 and renewable_penetration_rate < 0.4 then '20-40%'
            when renewable_penetration_rate >= 0.4 and renewable_penetration_rate < 0.6 then '40-60%'
            when renewable_penetration_rate >= 0.6 and renewable_penetration_rate < 0.8 then '60-80%'
            when renewable_penetration_rate >= 0.8 then '80%+'
            else 'Unknown'
        end as penetration_bucket,
        
        -- sort order for penetration bucket
        case 
            when renewable_penetration_rate < 0.2 then 1
            when renewable_penetration_rate >= 0.2 and renewable_penetration_rate < 0.4 then 2
            when renewable_penetration_rate >= 0.4 and renewable_penetration_rate < 0.6 then 3
            when renewable_penetration_rate >= 0.6 and renewable_penetration_rate < 0.8 then 4
            when renewable_penetration_rate >= 0.8 then 5
            else 0
        end as bucket_sort_order,
        
        price_eur_mwh,
        volatility_24h,
        residual_load_mw,
        is_extreme_ramp
        
    from hourly_facts
)

select
    country_name,
    year,
    month,
    penetration_bucket,
    bucket_sort_order,
    
    -- count hours in bucket
    count(*) as hours_in_bucket,
    
    -- avg volatility
    avg(volatility_24h) as avg_price_volatility,
    
    -- avg residual load
    avg(residual_load_mw) as avg_residual_load,
    
    -- total extreme ramps
    sum(case when is_extreme_ramp then 1 else 0 end) as total_extreme_ramps,
    
    -- avg price
    avg(price_eur_mwh) as avg_price

from buckets
group by 1, 2, 3, 4, 5  -- group by Country, Year, Month, Bucket, Order
order by country_name, year, month, bucket_sort_order