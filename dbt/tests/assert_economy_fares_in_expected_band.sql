{{ config(severity='warn') }}

-- Drift detector: the median one-way domestic economy fare should stay in a plausible band.
with fares as (
    select median(fare_inr) as median_fare
    from {{ ref('fct_flight_fare_quotes') }}
    where cabin_class = 'Economy'
)

select median_fare
from fares
where median_fare not between 2000 and 20000
