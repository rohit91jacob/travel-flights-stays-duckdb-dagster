with quotes as (
    select
        *,
        case
            when duration_minutes is null then 'unknown'
            when duration_minutes < 180 then '< 3h'
            when duration_minutes < 360 then '3-6h'
            when duration_minutes < 720 then '6-12h'
            when duration_minutes < 1440 then '12-24h'
            else '24h+'
        end as duration_band,
        case
            when duration_minutes is null then 9
            when duration_minutes < 180 then 1
            when duration_minutes < 360 then 2
            when duration_minutes < 720 then 3
            when duration_minutes < 1440 then 4
            else 5
        end as duration_band_order
    from {{ ref('fct_flight_fare_quotes') }}
)

select
    cabin_class,
    stops,
    stops_label,
    duration_band,
    count(*) as fare_quotes,
    cast(duration_band_order as integer) as duration_band_order,
    cast(median(fare_inr) as decimal(12, 2)) as median_fare_inr,
    cast(avg(fare_inr) as decimal(12, 2)) as avg_fare_inr,
    cast(median(duration_minutes) as decimal(8, 1)) as median_duration_minutes
from quotes
group by cabin_class, stops, stops_label, duration_band, duration_band_order
