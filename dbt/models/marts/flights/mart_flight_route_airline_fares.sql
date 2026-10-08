with quotes as (
    select * from {{ ref('fct_flight_fare_quotes') }}
),

aggregated as (
    select
        route_code,
        origin_city,
        destination_city,
        carrier_code,
        airline_name,
        cabin_class,
        count(*) as fare_quotes,
        count(distinct departure_date) as departure_dates,
        count(distinct flight_code) as flight_codes,
        median(fare_inr) as median_fare,
        avg(fare_inr) as avg_fare,
        quantile_cont(fare_inr, 0.25) as p25_fare,
        quantile_cont(fare_inr, 0.75) as p75_fare,
        min(fare_inr) as min_fare,
        max(fare_inr) as max_fare,
        median(duration_minutes) as median_duration,
        avg(case when stops = 0 then 1.0 else 0.0 end) as nonstop_share
    from quotes
    group by route_code, origin_city, destination_city, carrier_code, airline_name, cabin_class
)

select
    route_code,
    origin_city,
    destination_city,
    carrier_code,
    airline_name,
    cabin_class,
    fare_quotes,
    departure_dates,
    flight_codes,
    cast(median_fare as decimal(12, 2)) as median_fare_inr,
    cast(avg_fare as decimal(12, 2)) as avg_fare_inr,
    cast(p25_fare as decimal(12, 2)) as p25_fare_inr,
    cast(p75_fare as decimal(12, 2)) as p75_fare_inr,
    cast(min_fare as integer) as min_fare_inr,
    cast(max_fare as integer) as max_fare_inr,
    cast(median_duration as decimal(8, 1)) as median_duration_minutes,
    cast(nonstop_share as decimal(6, 4)) as nonstop_share,
    cast(
        rank() over (partition by route_code, cabin_class order by median_fare) as integer
    ) as median_fare_rank_on_route
from aggregated
