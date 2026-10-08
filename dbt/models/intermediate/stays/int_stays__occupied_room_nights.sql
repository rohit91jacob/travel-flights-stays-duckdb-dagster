{#-
    One row per occupied room-night of a completed stay (status Check-Out, at least one
    night). Each booking is treated as one room, as in the source paper.
-#}
with stays as (
    select
        booking_id,
        hotel_code,
        arrival_date,
        checkout_date,
        adr_eur
    from {{ ref('int_stays__bookings') }}
    where current_status = 'Check-Out' and nights > 0
),

nights as (
    select
        booking_id,
        hotel_code,
        adr_eur as room_revenue_eur,
        unnest(
            range(arrival_date::timestamp, checkout_date::timestamp, interval 1 day)
        ) as stay_night_ts
    from stays
)

select
    booking_id,
    hotel_code,
    room_revenue_eur,
    stay_night_ts::date as stay_night
from nights
