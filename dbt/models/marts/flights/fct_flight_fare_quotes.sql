{{
    config(
        materialized='incremental',
        incremental_strategy='delete+insert',
        unique_key='departure_date',
        on_schema_change='fail'
    )
}}

{#-
    Incremental by departure date: only dates with rows (re)loaded since the last build are
    rebuilt, and each is replaced wholesale, so reruns and backfills stay idempotent.
-#}
with quotes as (
    select * from {{ ref('stg_flights__fare_quotes') }}
    {% if is_incremental() %}
        where departure_date in (
                select distinct reloaded.departure_date
                from {{ ref('stg_flights__fare_quotes') }} as reloaded
                where reloaded._loaded_at > (
                        select coalesce(max(built._loaded_at), timestamp '1900-01-01')
                        from {{ this }} as built
                    )
            )
    {% endif %}
),

routes as (
    select * from {{ ref('dim_route') }}
)

select
    quotes.fare_quote_id,
    cast(strftime(quotes.departure_date, '%Y%m%d') as integer) as departure_date_key,
    quotes.departure_date,
    quotes.observed_on,
    cast(quotes.days_to_departure as integer) as days_to_departure,
    routes.route_code,
    quotes.origin_city,
    quotes.destination_city,
    quotes.carrier_code,
    quotes.airline_name,
    quotes.flight_code,
    quotes.cabin_class,
    cast(quotes.stops as integer) as stops,
    quotes.stops_label,
    quotes.via_point,
    quotes.departure_time,
    quotes.arrival_time,
    quotes.departure_time_band,
    quotes.duration_minutes,
    quotes.fare_inr,
    'INR' as currency_code,
    quotes.is_duration_malformed,
    quotes.is_arrival_time_consistent,
    quotes.dataset_version,
    quotes._loaded_at
from quotes
inner join routes
    on
        quotes.origin_city = routes.origin_city
        and quotes.destination_city = routes.destination_city
