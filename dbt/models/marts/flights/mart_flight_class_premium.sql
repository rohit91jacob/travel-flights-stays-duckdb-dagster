{#-
    Business-class premium per route and airline, two ways:
    * medians of all quotes per class, and
    * like-for-like: the same itinerary (flight, departure date and times, stops) priced in
      both classes, using the cheapest quote per class.
-#}
with quotes as (
    select * from {{ ref('fct_flight_fare_quotes') }}
),

class_medians as (
    select
        route_code,
        carrier_code,
        airline_name,
        cabin_class,
        count(*) as fare_quotes,
        median(fare_inr) as median_fare
    from quotes
    group by route_code, carrier_code, airline_name, cabin_class
),

itineraries as (
    select
        route_code,
        carrier_code,
        departure_date,
        flight_code,
        departure_time,
        arrival_time,
        duration_minutes,
        stops,
        cabin_class,
        min(fare_inr) as cheapest_fare
    from quotes
    group by
        route_code,
        carrier_code,
        departure_date,
        flight_code,
        departure_time,
        arrival_time,
        duration_minutes,
        stops,
        cabin_class
),

matched as (
    select
        economy.route_code,
        economy.carrier_code,
        business.cheapest_fare * 1.0 / economy.cheapest_fare as premium_ratio
    from itineraries as economy
    inner join itineraries as business
        on
            economy.route_code = business.route_code
            and economy.carrier_code = business.carrier_code
            and economy.departure_date = business.departure_date
            and economy.flight_code = business.flight_code
            and economy.departure_time = business.departure_time
            and economy.arrival_time = business.arrival_time
            and economy.duration_minutes = business.duration_minutes
            and economy.stops = business.stops
    where economy.cabin_class = 'Economy' and business.cabin_class = 'Business'
),

matched_summary as (
    select
        route_code,
        carrier_code,
        count(*) as matched_itineraries,
        median(premium_ratio) as median_matched_ratio
    from matched
    group by route_code, carrier_code
)

select
    economy.route_code,
    economy.carrier_code,
    economy.airline_name,
    economy.fare_quotes as economy_quotes,
    business.fare_quotes as business_quotes,
    cast(economy.median_fare as decimal(12, 2)) as economy_median_fare_inr,
    cast(business.median_fare as decimal(12, 2)) as business_median_fare_inr,
    cast(business.median_fare - economy.median_fare as decimal(12, 2)) as business_premium_inr,
    cast(business.median_fare / economy.median_fare as decimal(8, 3)) as business_premium_ratio,
    coalesce(matched_summary.matched_itineraries, 0) as matched_itineraries,
    cast(matched_summary.median_matched_ratio as decimal(8, 3)) as median_like_for_like_premium_ratio
from class_medians as economy
inner join class_medians as business
    on
        economy.route_code = business.route_code
        and economy.carrier_code = business.carrier_code
        and business.cabin_class = 'Business'
left join matched_summary
    on
        economy.route_code = matched_summary.route_code
        and economy.carrier_code = matched_summary.carrier_code
where economy.cabin_class = 'Economy'
