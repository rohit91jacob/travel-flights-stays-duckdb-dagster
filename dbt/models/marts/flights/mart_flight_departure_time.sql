with quotes as (
    select * from {{ ref('fct_flight_fare_quotes') }}
),

by_band as (
    select
        cabin_class,
        departure_time_band,
        count(*) as fare_quotes,
        median(fare_inr) as median_fare,
        avg(fare_inr) as avg_fare
    from quotes
    group by cabin_class, departure_time_band
),

by_class as (
    select
        cabin_class,
        median(fare_inr) as class_median_fare
    from quotes
    group by cabin_class
)

select
    by_band.cabin_class,
    by_band.departure_time_band,
    by_band.fare_quotes,
    cast(by_band.median_fare as decimal(12, 2)) as median_fare_inr,
    cast(by_band.avg_fare as decimal(12, 2)) as avg_fare_inr,
    cast(by_band.median_fare / by_class.class_median_fare as decimal(8, 3)) as median_fare_index_vs_class,
    cast(
        case by_band.departure_time_band
            when 'late_night' then 1
            when 'early_morning' then 2
            when 'morning' then 3
            when 'afternoon' then 4
            when 'evening' then 5
            when 'night' then 6
        end as integer
    ) as band_order
from by_band
inner join by_class on by_band.cabin_class = by_class.cabin_class
