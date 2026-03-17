with hourly_data as (
    select * from {{ ref('fct_energy_hourly') }}
),

-- compute monthly average price baseline
monthly_baseline as (
    select
        country_code,
        year,
        month,
        avg(price_eur_mwh) as monthly_avg_price
    from hourly_data
    where price_eur_mwh is not null
    group by 1, 2, 3
),

-- select Dunkelflaute events
-- formula: Sum( (Actual_Price - Monthly_Avg_Price) * Actual_Load )
risk_calculation as (
    select
        h.country_code,
        h.country_name,
        h.year,
        h.month,
        
        -- count hours in Dunkelflaute
        count(*) as hours_in_dunkelflaute,
        
        -- sum load at risk
        sum(case when h.price_eur_mwh is not null then h.load_mw else 0 end) as priced_load_mwh_at_risk,
        
        -- compute total Dunkelflaute cost
        -- strictly filter NULL price, prevent NULL result
        sum(
            (h.price_eur_mwh - m.monthly_avg_price) * h.load_mw
        ) as total_dunkelflaute_cost_eur,
        
        avg(h.price_eur_mwh) as avg_price_during_event,
        max(m.monthly_avg_price) as baseline_monthly_price

    from hourly_data h
    -- join baseline table
    left join monthly_baseline m 
        on h.country_code = m.country_code 
        and h.year = m.year 
        and h.month = m.month
    -- only select Dunkelflaute events
    where h.is_dunkelflaute = true
    and h.price_eur_mwh is not null
    
    group by 1, 2, 3, 4
)

select
    country_name,
    year,
    month,
    hours_in_dunkelflaute,
    priced_load_mwh_at_risk,
    baseline_monthly_price,
    avg_price_during_event,
    
    -- Dunkelflaute cost
    total_dunkelflaute_cost_eur,
    
    -- risk premium per MWh
    total_dunkelflaute_cost_eur / NULLIF(priced_load_mwh_at_risk, 0) as risk_premium_per_mwh

from risk_calculation
order by country_name, year, month