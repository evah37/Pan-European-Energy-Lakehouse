with source as (
    select * from {{ source('energy_lake', 'silver_price_profile') }}
),

renamed as (
    select
        -- dimension
        country as country_code,
        cast(month as int) as month_of_year,
        cast(weekday as int) as day_of_week,
        cast(hour as int) as hour_of_day,
        
        -- statistical metrics
        cast(avg_price_baseline as double) as avg_price_baseline,
        cast(stddev_price_baseline as double) as stddev_price_baseline,
        cast(sample_size as int) as sample_size
        
    from source
)

select * from renamed