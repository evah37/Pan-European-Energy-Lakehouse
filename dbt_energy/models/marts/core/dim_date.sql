
-- generate precise time series

with date_spine as (
    select 
        range as date_hour 
    from range(
        TIMESTAMP '2014-12-31 23:00:00', 
        TIMESTAMP '2020-10-01 00:00:00', 
        INTERVAL 1 HOUR
    )
)

select
    -- main key timestamp 
    date_hour as time_key,
    
    -- basic time features
    year(date_hour) as year,
    month(date_hour) as month,
    day(date_hour) as day,
    hour(date_hour) as hour_of_day,
    
    -- cyclical features
    dayofweek(date_hour) as day_of_week_index,
    case 
        when dayofweek(date_hour) = 0 then 'Sunday'
        when dayofweek(date_hour) = 1 then 'Monday'
        when dayofweek(date_hour) = 2 then 'Tuesday'
        when dayofweek(date_hour) = 3 then 'Wednesday'
        when dayofweek(date_hour) = 4 then 'Thursday'
        when dayofweek(date_hour) = 5 then 'Friday'
        when dayofweek(date_hour) = 6 then 'Saturday'
    end as day_of_week_name,

    -- business logic flags
    -- weekend flag
    case when dayofweek(date_hour) in (0, 6) then true else false end as is_weekend,
    
    -- peak hour (08:00 - 20:00)
    case when hour(date_hour) between 8 and 20 then true else false end as is_peak_hour,
    
    -- season
    case 
        when month(date_hour) in (12, 1, 2) then 'Winter'
        when month(date_hour) in (3, 4, 5) then 'Spring'
        when month(date_hour) in (6, 7, 8) then 'Summer'
        when month(date_hour) in (9, 10, 11) then 'Autumn'
    end as season

from date_spine