with source as (
    select * from {{ source('energy_lake', 'silver_features') }}
),

renamed as (
    select
        -- generate surrogate key
        md5(concat(country, cast(timestamp as varchar))) as energy_event_id,
        
        -- rename core fields
        timestamp as event_time,
        country as country_code,
        
        -- metrics
        cast(price as double) as price_eur_mwh,
        cast(load as double) as load_mw,
        cast(solar as double) as solar_mw,
        cast(wind as double) as wind_mw,
        cast(residual_load as double) as residual_load_mw,
        
        -- advanced features
        cast(volatility_24h as double) as volatility_24h,
        cast(ramp_rate_pct as double) as ramp_rate_pct,
        
        -- flags
        cast(is_extreme_ramp as boolean) as is_extreme_ramp,
        cast(is_dunkelflaute as boolean) as is_dunkelflaute,
        
        -- time metadata (used for partitioning)
        cast(year as int) as year,
        cast(month as int) as month,
        cast(day as int) as day
        
    from source
)

select * from renamed