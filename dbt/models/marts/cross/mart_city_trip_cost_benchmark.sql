{#-
    Indicative trip cost per destination city: median one-way economy fare into the city
    plus N nights at the median 3-4 star listing. The two inputs share a real geography
    (the same six metros) but were captured about 18 months apart (fares observed
    2022-02-10, listing prices 2023-08), so this is a benchmark for comparing cities with
    each other, not a price quote. Both capture dates are carried as columns.
-#}
{%- set nights = 3 -%}

with inbound as (
    select
        destination_city as city,
        count(*) as inbound_quotes,
        median(fare_inr) as median_fare,
        min(observed_on) as fares_observed_on,
        min(departure_date) as first_departure_date,
        max(departure_date) as last_departure_date
    from {{ ref('fct_flight_fare_quotes') }}
    where cabin_class = 'Economy'
    group by destination_city
),

stays as (
    select
        city,
        count(*) as listings,
        median(total_nightly_price_inr) as median_nightly,
        min(captured_on) as listing_prices_captured_on
    from {{ ref('dim_hotel_listing') }}
    where star_rating in (3, 4)
    group by city
)

select
    cities.city_name as city,
    cities.city_code,
    inbound.inbound_quotes,
    stays.listings as mid_range_listings,
    inbound.fares_observed_on,
    inbound.first_departure_date,
    inbound.last_departure_date,
    stays.listing_prices_captured_on,
    cast({{ nights }} as integer) as nights,
    cast(inbound.median_fare as decimal(12, 2)) as median_inbound_economy_fare_inr,
    cast(stays.median_nightly as decimal(12, 2)) as median_mid_range_nightly_price_inr,
    cast(inbound.median_fare + {{ nights }} * stays.median_nightly as decimal(12, 2)) as indicative_trip_cost_inr,
    cast(
        date_diff('day', inbound.fares_observed_on, stays.listing_prices_captured_on) as integer
    ) as days_between_captures
from {{ ref('dim_city') }} as cities
left join inbound on cities.city_name = inbound.city
left join stays on cities.city_name = stays.city
