with routes as (
    select distinct
        origin_city,
        destination_city
    from {{ ref('stg_flights__fare_quotes') }}
),

cities as (
    select * from {{ ref('dim_city') }}
)

select
    routes.origin_city,
    origin.city_code as origin_city_code,
    routes.destination_city,
    destination.city_code as destination_city_code,
    origin.city_code || '-' || destination.city_code as route_code,
    least(origin.city_code, destination.city_code)
    || '-'
    || greatest(origin.city_code, destination.city_code) as city_pair_code
from routes
inner join cities as origin on routes.origin_city = origin.city_name
inner join cities as destination on routes.destination_city = destination.city_name
