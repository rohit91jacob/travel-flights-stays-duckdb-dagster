select
    carrier_code,
    airline_name,
    count(*) as fare_quotes,
    count(distinct flight_code) as flight_codes,
    bool_or(cabin_class = 'Business') as sells_business_class
from {{ ref('stg_flights__fare_quotes') }}
group by carrier_code, airline_name
