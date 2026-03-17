with hourly_data as (
    select 
        country_name,
        year,
        month,
        -- get hour from timestamp
        hour(event_time) as hour_of_day,
        load_mw,
        solar_mw,
        wind_mw,
        residual_load_mw,
        price_eur_mwh
    from {{ ref('fct_energy_hourly') }}
)

select
    country_name,
    year,
    month,
    hour_of_day,
    
    -- avg total load
    avg(load_mw) as avg_total_load,
    
    -- avg solar generation
    avg(solar_mw) as avg_solar_generation,
    
    -- avg wind generation
    avg(wind_mw) as avg_wind_generation,
    
    -- avg residual load
    avg(residual_load_mw) as avg_net_load,
    
    -- avg price
    avg(price_eur_mwh) as avg_price

from hourly_data
group by 1, 2, 3, 4
order by country_name, year, month, hour_of_day