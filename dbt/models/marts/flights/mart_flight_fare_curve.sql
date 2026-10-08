{#-
    Fare curve by days to departure. All quotes were observed on one day, so this is a
    cross-section across departure dates (1-49 days out), not one flight's price over time:
    it mixes lead-time effects with day-of-week and season effects of the departure date.
-#}
with quotes as (
    select * from {{ ref('fct_flight_fare_quotes') }}
),

per_route as (
    select
        route_code,
        cabin_class,
        days_to_departure,
        departure_date,
        count(*) as fare_quotes,
        median(fare_inr) as median_fare,
        avg(fare_inr) as avg_fare,
        quantile_cont(fare_inr, 0.25) as p25_fare,
        quantile_cont(fare_inr, 0.75) as p75_fare,
        min(fare_inr) as min_fare
    from quotes
    group by route_code, cabin_class, days_to_departure, departure_date
),

all_routes as (
    select
        'ALL' as route_code,
        cabin_class,
        days_to_departure,
        departure_date,
        count(*) as fare_quotes,
        median(fare_inr) as median_fare,
        avg(fare_inr) as avg_fare,
        quantile_cont(fare_inr, 0.25) as p25_fare,
        quantile_cont(fare_inr, 0.75) as p75_fare,
        min(fare_inr) as min_fare
    from quotes
    group by cabin_class, days_to_departure, departure_date
),

combined as (
    select * from per_route
    union all
    select * from all_routes
)

select
    route_code,
    cabin_class,
    days_to_departure,
    departure_date,
    fare_quotes,
    cast(median_fare as decimal(12, 2)) as median_fare_inr,
    cast(avg_fare as decimal(12, 2)) as avg_fare_inr,
    cast(p25_fare as decimal(12, 2)) as p25_fare_inr,
    cast(p75_fare as decimal(12, 2)) as p75_fare_inr,
    cast(min_fare as integer) as min_fare_inr,
    cast(
        median_fare / first_value(median_fare) over (
            partition by route_code, cabin_class order by days_to_departure desc
        ) as decimal(8, 3)
    ) as median_fare_index_vs_furthest
from combined
