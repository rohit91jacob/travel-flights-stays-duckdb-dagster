select
    city_name,
    city_code,
    primary_airport_iata,
    state_name,
    country_code,
    currency_code
from {{ ref('ref_cities') }}
