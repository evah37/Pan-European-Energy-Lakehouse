with energy as (
    select * from {{ ref('stg_spark__energy') }}
),

countries as (
    select * from {{ ref('dim_country') }}
),

dates as (
    select * from {{ ref('dim_date') }}
)

select
    e.energy_event_id,
    
    -- foreign key
    e.event_time,
    e.country_code,
    
    -- dimension redundancy for analysis
    c.country_name,
    c.eu_region,
    d.year,
    d.month,
    d.is_weekend,
    d.is_peak_hour,
    
    -- core metrics
    e.price_eur_mwh,
    e.load_mw,
    e.solar_mw,
    e.wind_mw,
    (e.solar_mw + e.wind_mw) as total_renewable_mw,
    e.residual_load_mw,
    
    -- renewable penetration rate
    case 
        when e.load_mw > 0 then (e.solar_mw + e.wind_mw) / e.load_mw 
        else 0 
    end as renewable_penetration_rate,
    
    -- risk features
    e.volatility_24h,
    e.ramp_rate_pct,
    e.is_extreme_ramp,
    e.is_dunkelflaute

from energy e
left join countries c on e.country_code = c.country_code
left join dates d on e.event_time = d.time_key