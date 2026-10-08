{{ config(severity='warn') }}

-- Drift detector: across both hotels roughly 37% of bookings with a known outcome cancel.
-- A large move usually means an upstream change, not a change in guest behaviour.
with known as (
    select avg(case when is_canceled then 1.0 else 0.0 end) as cancellation_rate
    from {{ ref('fct_hotel_bookings') }}
    where is_outcome_known
)

select cancellation_rate
from known
where cancellation_rate not between 0.25 and 0.50
